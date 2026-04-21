#![allow(clippy::useless_conversion)]

mod fields_json;
mod row_values;

use std::collections::BTreeMap;
use std::sync::Mutex;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};
use typra_core::record::ScalarValue;
use typra_core::schema::{CollectionId, FieldDef, SchemaVersion};
use typra_core::storage::{FileStore, VecStore};
use typra_core::Database as CoreDatabase;

enum InnerDb {
    File(CoreDatabase<FileStore>),
    Mem(CoreDatabase<VecStore>),
}

impl InnerDb {
    fn register_collection(
        &mut self,
        name: &str,
        fields: Vec<FieldDef>,
        primary_field: &str,
    ) -> Result<(CollectionId, SchemaVersion), typra_core::DbError> {
        match self {
            InnerDb::File(d) => d.register_collection(name, fields, primary_field),
            InnerDb::Mem(d) => d.register_collection(name, fields, primary_field),
        }
    }

    fn collection_names(&self) -> Vec<String> {
        match self {
            InnerDb::File(d) => d.collection_names(),
            InnerDb::Mem(d) => d.collection_names(),
        }
    }

    fn collection_id_named(&self, name: &str) -> Result<CollectionId, typra_core::DbError> {
        match self {
            InnerDb::File(d) => d.collection_id_named(name),
            InnerDb::Mem(d) => d.collection_id_named(name),
        }
    }

    fn catalog(&self) -> &typra_core::Catalog {
        match self {
            InnerDb::File(d) => d.catalog(),
            InnerDb::Mem(d) => d.catalog(),
        }
    }

    fn insert(
        &mut self,
        id: CollectionId,
        row: BTreeMap<String, ScalarValue>,
    ) -> Result<(), typra_core::DbError> {
        match self {
            InnerDb::File(d) => d.insert(id, row),
            InnerDb::Mem(d) => d.insert(id, row),
        }
    }

    fn get(
        &self,
        id: CollectionId,
        pk: &ScalarValue,
    ) -> Result<Option<BTreeMap<String, ScalarValue>>, typra_core::DbError> {
        match self {
            InnerDb::File(d) => d.get(id, pk),
            InnerDb::Mem(d) => d.get(id, pk),
        }
    }

    fn path_display(&self) -> String {
        match self {
            InnerDb::File(d) => d.path().display().to_string(),
            InnerDb::Mem(d) => d.path().display().to_string(),
        }
    }

    fn snapshot_bytes(&self) -> Result<Vec<u8>, PyErr> {
        match self {
            InnerDb::File(_) => Err(PyValueError::new_err(
                "snapshot_bytes is only supported for in-memory databases",
            )),
            InnerDb::Mem(d) => Ok(d.snapshot_bytes()),
        }
    }
}

/// Shared Python handle for on-disk and in-memory databases.
#[pyclass(name = "Database")]
pub struct Database {
    inner: Mutex<InnerDb>,
}

#[pymethods]
impl Database {
    /// Open or create a database file at ``path``.
    #[staticmethod]
    fn open(path: &str) -> PyResult<Self> {
        let db = CoreDatabase::open(path).map_err(fields_json::db_error_to_py)?;
        Ok(Self {
            inner: Mutex::new(InnerDb::File(db)),
        })
    }

    fn path(&self) -> String {
        let g = self.inner.lock().unwrap();
        g.path_display()
    }

    /// Register a collection with schema version 1.
    ///
    /// ``fields_json`` is a JSON array of objects like
    /// ``{"path": ["title"], "type": "string"}``. See README for the v1 shape.
    /// ``primary_field`` is the top-level field name used as the primary key.
    fn register_collection(
        &self,
        name: &str,
        fields_json: &str,
        primary_field: &str,
    ) -> PyResult<(u32, u32)> {
        let fields = fields_json::fields_from_json(fields_json).map_err(PyValueError::new_err)?;
        let mut g = self.inner.lock().unwrap();
        let (id, v) = g
            .register_collection(name, fields, primary_field)
            .map_err(fields_json::db_error_to_py)?;
        Ok((id.0, v.0))
    }

    fn collection_names(&self) -> Vec<String> {
        self.inner.lock().unwrap().collection_names()
    }

    /// Insert a full row (all fields). ``row`` keys are field names; the primary key must be included.
    fn insert(
        &self,
        py: Python<'_>,
        collection_name: &str,
        row: &Bound<'_, PyDict>,
    ) -> PyResult<()> {
        let col = {
            let g = self.inner.lock().unwrap();
            let cid = g
                .collection_id_named(collection_name)
                .map_err(fields_json::db_error_to_py)?;
            g.catalog()
                .get(cid)
                .cloned()
                .ok_or_else(|| PyValueError::new_err("collection missing after resolve"))?
        };
        let mapped = row_values::row_from_dict(py, row, &col)?;
        let mut g = self.inner.lock().unwrap();
        let cid = g
            .collection_id_named(collection_name)
            .map_err(fields_json::db_error_to_py)?;
        g.insert(cid, mapped).map_err(fields_json::db_error_to_py)
    }

    /// Return the latest row for ``pk``, or ``None``.
    fn get(
        &self,
        py: Python<'_>,
        collection_name: &str,
        pk: &Bound<'_, PyAny>,
    ) -> PyResult<Option<Py<PyDict>>> {
        let col = {
            let g = self.inner.lock().unwrap();
            let cid = g
                .collection_id_named(collection_name)
                .map_err(fields_json::db_error_to_py)?;
            g.catalog()
                .get(cid)
                .cloned()
                .ok_or_else(|| PyValueError::new_err("collection missing after resolve"))?
        };
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
            let g = self.inner.lock().unwrap();
            let cid = g
                .collection_id_named(collection_name)
                .map_err(fields_json::db_error_to_py)?;
            g.get(cid, &pk_val).map_err(fields_json::db_error_to_py)?
        };
        match row {
            None => Ok(None),
            Some(r) => Ok(Some(row_values::row_to_dict(py, &r)?.unbind())),
        }
    }

    /// In-memory database (see Rust ``Database::open_in_memory``).
    #[staticmethod]
    fn open_in_memory() -> PyResult<Self> {
        let db = CoreDatabase::open_in_memory().map_err(fields_json::db_error_to_py)?;
        Ok(Self {
            inner: Mutex::new(InnerDb::Mem(db)),
        })
    }

    /// Load a snapshot produced by [`snapshot_bytes`](Self::snapshot_bytes).
    #[staticmethod]
    fn open_snapshot_bytes(data: &[u8]) -> PyResult<Self> {
        let db = CoreDatabase::from_snapshot_bytes(data.to_vec())
            .map_err(fields_json::db_error_to_py)?;
        Ok(Self {
            inner: Mutex::new(InnerDb::Mem(db)),
        })
    }

    /// Serialize the in-memory database to bytes (in-memory databases only).
    fn snapshot_bytes(&self, py: Python<'_>) -> PyResult<Py<PyBytes>> {
        let g = self.inner.lock().unwrap();
        let v = g.snapshot_bytes()?;
        Ok(PyBytes::new_bound(py, &v).unbind())
    }
}

#[pymodule]
fn typra(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add(
        "__doc__",
        "Python bindings for Typra: typed embedded database (Database.open, register_collection, collection_names).",
    )?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_class::<Database>()?;
    Ok(())
}
