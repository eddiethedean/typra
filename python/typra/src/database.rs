//! PyO3 `Database` class: file- and memory-backed [`crate::inner_db::InnerDb`] behind a mutex.

use std::sync::Mutex;

use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};
use std::sync::MutexGuard;
use typra_core::catalog::CollectionInfo;
use typra_core::Database as CoreDatabase;

use crate::errors::db_error_to_py;
use crate::fields_json;
use crate::inner_db::InnerDb;
use crate::row_values;

fn lock_inner(inner: &Mutex<InnerDb>) -> PyResult<MutexGuard<'_, InnerDb>> {
    inner
        .lock()
        .map_err(|e| PyRuntimeError::new_err(format!("database lock poisoned: {e}")))
}

/// Python `Database`: Typra engine behind an internal mutex (safe across threads that release the GIL).
#[pyclass(name = "Database")]
pub struct Database {
    pub(crate) inner: Mutex<InnerDb>,
}

fn collection_info(inner: &Mutex<InnerDb>, name: &str) -> PyResult<CollectionInfo> {
    let g = lock_inner(inner)?;
    let cid = g.collection_id_named(name).map_err(db_error_to_py)?;
    g.catalog()
        .get(cid)
        .cloned()
        .ok_or_else(|| PyValueError::new_err("collection missing after resolve"))
}

#[pymethods]
impl Database {
    /// Open or create an on-disk database at the given path.
    ///
    /// Args:
    ///     path (str): Filesystem path to the Typra file (created if it does not exist).
    ///
    /// Returns:
    ///     Database: Handle backed by a file store.
    ///
    /// Raises:
    ///     OSError: File open/create or I/O failures from the engine.
    ///     ValueError: Invalid or unsupported on-disk format.
    ///     RuntimeError: Engine reports an unimplemented code path.
    #[staticmethod]
    fn open(path: &str) -> PyResult<Self> {
        let db = CoreDatabase::open(path).map_err(db_error_to_py)?;
        Ok(Self {
            inner: Mutex::new(InnerDb::File(db)),
        })
    }

    /// Return the path string for this database.
    ///
    /// For in-memory databases this is ``":memory:"`` (see ``open_in_memory``).
    fn path(&self) -> PyResult<String> {
        let g = lock_inner(&self.inner)?;
        Ok(g.path_display())
    }

    /// Register a new collection at schema version 1.
    ///
    /// Args:
    ///     name (str): Collection name (trimmed; must be unique).
    ///     fields_json (str): JSON array of field objects, e.g.
    ///         ``[{"path": ["title"], "type": "string"}, ...]``. See the package README for the v1 shape.
    ///     primary_field (str): Top-level field name used as the primary key.
    ///
    /// Returns:
    ///     tuple[int, int]: ``(collection_id, schema_version)`` (both ``1`` for a new collection).
    ///
    /// Raises:
    ///     ValueError: Invalid JSON or schema rules (including unknown types for unsupported shapes).
    ///     OSError / RuntimeError: Mapped from engine errors where applicable.
    fn register_collection(
        &self,
        name: &str,
        fields_json: &str,
        primary_field: &str,
    ) -> PyResult<(u32, u32)> {
        let fields = fields_json::fields_from_json(fields_json).map_err(PyValueError::new_err)?;
        let mut g = lock_inner(&self.inner)?;
        let (id, v) = g
            .register_collection(name, fields, primary_field)
            .map_err(db_error_to_py)?;
        Ok((id.0, v.0))
    }

    /// Return all collection names in sorted order.
    fn collection_names(&self) -> PyResult<Vec<String>> {
        let g = lock_inner(&self.inner)?;
        Ok(g.collection_names())
    }

    /// Insert or replace one row (all top-level fields required per schema).
    ///
    /// Args:
    ///     collection_name (str): Registered collection name.
    ///     row (dict): Maps field name strings to Python values; must include the primary key.
    ///
    /// Raises:
    ///     ValueError: Unknown field, wrong Python type for schema, or missing required field.
    ///     OSError / RuntimeError: Engine errors from the Rust layer.
    fn insert(
        &self,
        py: Python<'_>,
        collection_name: &str,
        row: &Bound<'_, PyDict>,
    ) -> PyResult<()> {
        let col = collection_info(&self.inner, collection_name)?;
        let mapped = row_values::row_from_dict(py, row, &col)?;
        let mut g = lock_inner(&self.inner)?;
        let cid = g
            .collection_id_named(collection_name)
            .map_err(db_error_to_py)?;
        g.insert(cid, mapped).map_err(db_error_to_py)
    }

    /// Fetch the latest row for a primary key, or ``None`` if absent.
    ///
    /// Args:
    ///     collection_name (str): Registered collection name.
    ///     pk: Primary-key value compatible with the schema type (e.g. ``str`` for ``string``).
    ///
    /// Returns:
    ///     dict | None: Row as a ``dict`` of field names to Python values, or ``None``.
    ///
    /// Raises:
    ///     ValueError: Unknown collection, missing primary in schema, type mismatch, or unsupported type.
    ///     OSError / RuntimeError: Engine errors from the Rust layer.
    fn get(
        &self,
        py: Python<'_>,
        collection_name: &str,
        pk: &Bound<'_, PyAny>,
    ) -> PyResult<Option<Py<PyDict>>> {
        let col = collection_info(&self.inner, collection_name)?;
        let pk_name = col
            .primary_field
            .as_deref()
            .ok_or_else(|| PyValueError::new_err("collection has no primary key"))?;
        let pk_ty = col
            .fields
            .iter()
            .find(|f| f.path.0.len() == 1 && f.path.0[0] == pk_name)
            .map(|f| &f.ty)
            .ok_or_else(|| PyValueError::new_err("primary field not in schema"))?;
        let pk_val = row_values::scalar_from_py(py, pk, pk_ty)?;
        let row = {
            let g = lock_inner(&self.inner)?;
            let cid = g
                .collection_id_named(collection_name)
                .map_err(db_error_to_py)?;
            g.get(cid, &pk_val).map_err(db_error_to_py)?
        };
        match row {
            None => Ok(None),
            Some(r) => Ok(Some(row_values::row_to_dict(py, &r)?.unbind())),
        }
    }

    /// Create a new empty in-memory database (``VecStore``; path ``":memory:"``).
    ///
    /// Returns:
    ///     Database: In-memory handle; use ``snapshot_bytes`` / ``open_snapshot_bytes`` to serialize.
    ///
    /// Raises:
    ///     OSError / RuntimeError: If the engine fails to initialize.
    #[staticmethod]
    fn open_in_memory() -> PyResult<Self> {
        let db = CoreDatabase::open_in_memory().map_err(db_error_to_py)?;
        Ok(Self {
            inner: Mutex::new(InnerDb::Mem(db)),
        })
    }

    /// Restore an in-memory database from bytes produced by ``snapshot_bytes``.
    ///
    /// Args:
    ///     data (bytes): Full database image (same layout as a file).
    ///
    /// Returns:
    ///     Database: In-memory handle ready for reads and writes.
    #[staticmethod]
    fn open_snapshot_bytes(data: &[u8]) -> PyResult<Self> {
        let db = CoreDatabase::from_snapshot_bytes(data.to_vec()).map_err(db_error_to_py)?;
        Ok(Self {
            inner: Mutex::new(InnerDb::Mem(db)),
        })
    }

    /// Serialize an in-memory database to bytes (not supported for on-disk databases).
    ///
    /// Returns:
    ///     bytes: Copy of the full store image.
    ///
    /// Raises:
    ///     ValueError: If this database is file-backed (only in-memory images can be snapshotted here).
    ///     OSError / RuntimeError: Engine errors when reading the buffer.
    fn snapshot_bytes(&self, py: Python<'_>) -> PyResult<Py<PyBytes>> {
        let g = lock_inner(&self.inner)?;
        let v = g.snapshot_bytes()?;
        Ok(PyBytes::new_bound(py, &v).unbind())
    }
}
