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
use crate::query as query_api;
use crate::row_values;

fn schema_change_to_str(change: &typra_core::schema::SchemaChange) -> (&'static str, Option<&str>) {
    match change {
        typra_core::schema::SchemaChange::Safe => ("safe", None),
        typra_core::schema::SchemaChange::NeedsMigration { reason } => {
            ("needs_migration", Some(reason.as_str()))
        }
        typra_core::schema::SchemaChange::Breaking { reason } => {
            ("breaking", Some(reason.as_str()))
        }
    }
}

pub(crate) fn lock_inner(inner: &Mutex<InnerDb>) -> PyResult<MutexGuard<'_, InnerDb>> {
    inner
        .lock()
        .map_err(|e| PyRuntimeError::new_err(format!("database lock poisoned: {e}")))
}

/// Python `Database`: Typra engine behind an internal mutex (safe across threads that release the GIL).
#[pyclass(name = "Database")]
pub struct Database {
    pub(crate) inner: Mutex<InnerDb>,
}

pub(crate) fn collection_info(inner: &Mutex<InnerDb>, name: &str) -> PyResult<CollectionInfo> {
    let g = lock_inner(inner)?;
    let cid = g.collection_id_named(name).map_err(db_error_to_py)?;
    g.catalog()
        .get(cid)
        .cloned()
        .ok_or_else(|| PyValueError::new_err("collection missing after resolve"))
}

/// Context manager returned by ``Database.transaction()`` (``with`` / ``__enter__`` / ``__exit__``).
#[pyclass(name = "Transaction")]
pub struct PyTransaction {
    db: Py<Database>,
}

#[pymethods]
impl PyTransaction {
    fn __enter__(&self, py: Python<'_>) -> PyResult<()> {
        {
            let db = self.db.bind(py).borrow();
            let mut g = lock_inner(&db.inner)?;
            g.begin_transaction().map_err(db_error_to_py)?;
        }
        Ok(())
    }

    #[pyo3(signature = (exc_type=None, _exc_value=None, _traceback=None))]
    fn __exit__(
        &self,
        py: Python<'_>,
        exc_type: Option<&Bound<'_, PyAny>>,
        _exc_value: Option<&Bound<'_, PyAny>>,
        _traceback: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<bool> {
        {
            let db = self.db.bind(py).borrow();
            let mut g = lock_inner(&db.inner)?;
            if exc_type.is_none() {
                g.commit_transaction().map_err(db_error_to_py)?;
            } else {
                g.rollback_transaction();
            }
        }
        Ok(false)
    }
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
    #[pyo3(signature = (path, *, read_only=false))]
    fn open(path: &str, read_only: bool) -> PyResult<Self> {
        let db = if read_only {
            CoreDatabase::open_read_only(path)
        } else {
            CoreDatabase::open(path)
        }
        .map_err(db_error_to_py)?;
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
    ///     indexes_json (str | None): Optional JSON array of index objects
    ///         ``[{"name": "...", "path": ["field"], "kind": "unique"|"index"|"non_unique"}, ...]``.
    ///         Each ``path`` must match a field in ``fields_json``; only scalar (or optional scalar)
    ///         fields may be indexed.
    ///
    /// Returns:
    ///     tuple[int, int]: ``(collection_id, schema_version)`` (both ``1`` for a new collection).
    ///
    /// Raises:
    ///     ValueError: Invalid JSON or schema rules (including unknown types for unsupported shapes).
    ///     OSError / RuntimeError: Mapped from engine errors where applicable.
    #[pyo3(signature = (name, fields_json, primary_field, indexes_json=None))]
    fn register_collection(
        &self,
        name: &str,
        fields_json: &str,
        primary_field: &str,
        indexes_json: Option<&str>,
    ) -> PyResult<(u32, u32)> {
        let fields = fields_json::fields_from_json(fields_json).map_err(PyValueError::new_err)?;
        let indexes = match indexes_json {
            None => Vec::new(),
            Some(s) if s.trim().is_empty() => Vec::new(),
            Some(s) => fields_json::indexes_from_json(s, &fields).map_err(PyValueError::new_err)?,
        };
        let mut g = lock_inner(&self.inner)?;
        let (id, v) = g
            .register_collection_with_indexes(name, fields, indexes, primary_field)
            .map_err(db_error_to_py)?;
        Ok((id.0, v.0))
    }

    /// Return all collection names in sorted order.
    fn collection_names(&self) -> PyResult<Vec<String>> {
        let g = lock_inner(&self.inner)?;
        Ok(g.collection_names())
    }

    /// Return a collection handle for building queries.
    fn collection(
        slf: PyRef<'_, Self>,
        py: Python<'_>,
        name: &str,
    ) -> PyResult<query_api::Collection> {
        // Validate early that the collection exists.
        let _ = collection_info(&slf.inner, name)?;
        let any: Py<PyAny> = slf.into_py(py);
        let db: Py<Database> = any.bind(py).downcast::<Database>()?.clone().unbind();
        Ok(query_api::Collection {
            db,
            name: name.to_string(),
        })
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

    /// Delete the latest row for a primary key (no-op if absent).
    fn delete(&self, py: Python<'_>, collection_name: &str, pk: &Bound<'_, PyAny>) -> PyResult<()> {
        let col = collection_info(&self.inner, collection_name)?;
        let pk_name = col
            .primary_field
            .as_deref()
            .ok_or_else(|| PyValueError::new_err("collection has no primary key"))?;
        let pk_ty = col
            .fields
            .iter()
            .find(|f| f.path.0.len() == 1 && f.path.0[0].as_ref() == pk_name)
            .map(|f| &f.ty)
            .ok_or_else(|| PyValueError::new_err("primary field not in schema"))?;
        let pk_val = row_values::scalar_from_py(py, pk, pk_ty)?;
        let mut g = lock_inner(&self.inner)?;
        let cid = g
            .collection_id_named(collection_name)
            .map_err(db_error_to_py)?;
        g.delete(cid, &pk_val).map_err(db_error_to_py)
    }

    /// Register a new schema version for an existing collection.
    ///
    /// Returns the new schema version number.
    #[pyo3(signature = (collection_name, fields_json, indexes_json=None, force=false))]
    fn register_schema_version(
        &self,
        collection_name: &str,
        fields_json: &str,
        indexes_json: Option<&str>,
        force: bool,
    ) -> PyResult<u32> {
        let fields = fields_json::fields_from_json(fields_json).map_err(PyValueError::new_err)?;
        let indexes = match indexes_json {
            None => Vec::new(),
            Some(s) if s.trim().is_empty() => Vec::new(),
            Some(s) => fields_json::indexes_from_json(s, &fields).map_err(PyValueError::new_err)?,
        };
        let mut g = lock_inner(&self.inner)?;
        let cid = g
            .collection_id_named(collection_name)
            .map_err(db_error_to_py)?;
        let v = if force {
            g.register_schema_version_with_indexes_force(cid, fields, indexes)
        } else {
            g.register_schema_version_with_indexes(cid, fields, indexes)
        }
        .map_err(db_error_to_py)?;
        Ok(v.0)
    }

    /// Plan a schema version bump and return a JSON-like dict describing required steps.
    #[pyo3(signature = (collection_name, fields_json, indexes_json=None))]
    fn plan_schema_version(
        &self,
        py: Python<'_>,
        collection_name: &str,
        fields_json: &str,
        indexes_json: Option<&str>,
    ) -> PyResult<Py<PyDict>> {
        let fields = fields_json::fields_from_json(fields_json).map_err(PyValueError::new_err)?;
        let indexes = match indexes_json {
            None => Vec::new(),
            Some(s) if s.trim().is_empty() => Vec::new(),
            Some(s) => fields_json::indexes_from_json(s, &fields).map_err(PyValueError::new_err)?,
        };
        let g = lock_inner(&self.inner)?;
        let cid = g
            .collection_id_named(collection_name)
            .map_err(db_error_to_py)?;
        let plan = g
            .plan_schema_version_with_indexes(cid, fields, indexes)
            .map_err(db_error_to_py)?;
        let d = PyDict::new_bound(py);
        let (kind, reason) = schema_change_to_str(&plan.change);
        d.set_item("change", kind)?;
        if let Some(r) = reason {
            d.set_item("reason", r)?;
        }
        let steps: Vec<String> = plan
            .steps
            .into_iter()
            .map(|s| match s {
                typra_core::MigrationStep::BackfillTopLevelField { field } => {
                    format!("backfill_top_level_field:{field}")
                }
                typra_core::MigrationStep::RebuildIndexes => "rebuild_indexes".to_string(),
            })
            .collect();
        d.set_item("steps", steps)?;
        Ok(d.unbind())
    }

    /// Backfill a missing top-level field with a fixed value for all rows.
    fn backfill_top_level_field(
        &self,
        py: Python<'_>,
        collection_name: &str,
        field: &str,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        let col = collection_info(&self.inner, collection_name)?;
        let def = col
            .fields
            .iter()
            .find(|f| f.path.0.len() == 1 && f.path.0[0].as_ref() == field)
            .ok_or_else(|| PyValueError::new_err(format!("unknown field {field:?}")))?;
        let rv = row_values::value_from_py(py, value, &def.ty)?;
        let mut g = lock_inner(&self.inner)?;
        let cid = g
            .collection_id_named(collection_name)
            .map_err(db_error_to_py)?;
        g.backfill_top_level_field_with_value(cid, field, rv)
            .map_err(db_error_to_py)
    }

    /// Rebuild index entries for a collection based on the latest rows.
    fn rebuild_indexes(&self, collection_name: &str) -> PyResult<()> {
        let mut g = lock_inner(&self.inner)?;
        let cid = g
            .collection_id_named(collection_name)
            .map_err(db_error_to_py)?;
        g.rebuild_indexes_for_collection(cid)
            .map_err(db_error_to_py)
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
            .find(|f| f.path.0.len() == 1 && f.path.0[0].as_ref() == pk_name)
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

    /// Open an in-memory database from a snapshot file on disk.
    #[staticmethod]
    fn open_snapshot(path: &str) -> PyResult<Self> {
        let db = CoreDatabase::open_snapshot_path(path).map_err(db_error_to_py)?;
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

    /// Export a consistent snapshot of this database to `dest_path`.
    ///
    /// - File-backed DBs: checkpoint then copy the `.typra` file.
    /// - In-memory DBs: write snapshot bytes to `dest_path`.
    fn export_snapshot(&self, dest_path: &str) -> PyResult<()> {
        let mut g = lock_inner(&self.inner)?;
        g.export_snapshot_to_path(dest_path)
    }

    /// Restore a snapshot file to `dest_path` by atomically replacing the destination.
    ///
    /// This is an operational helper intended for backup/restore workflows.
    #[staticmethod]
    fn restore_snapshot(snapshot_path: &str, dest_path: &str) -> PyResult<()> {
        InnerDb::restore_snapshot_to_path(snapshot_path, dest_path)
    }

    /// Rewrite the database file into a compacted image at `dest_path`.
    fn compact_to(&self, dest_path: &str) -> PyResult<()> {
        let g = lock_inner(&self.inner)?;
        g.compact_to(dest_path)
    }

    /// Compact this database in place (rewrites the file).
    fn compact(&self) -> PyResult<()> {
        let mut g = lock_inner(&self.inner)?;
        g.compact_in_place()
    }

    /// Return a context manager for a multi-write transaction (commits on success, rolls back on exception).
    #[pyo3(name = "transaction")]
    fn py_transaction(slf: PyRef<'_, Self>, py: Python<'_>) -> PyResult<Py<PyTransaction>> {
        let any: Py<PyAny> = slf.into_py(py);
        let db: Py<Database> = any.bind(py).downcast::<Database>()?.clone().unbind();
        Py::new(py, PyTransaction { db })
    }
}
