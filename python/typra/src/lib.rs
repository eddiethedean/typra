#![allow(clippy::useless_conversion)]

mod fields_json;

use std::sync::Mutex;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use typra_core::Database as CoreDatabase;

#[pyclass(name = "Database")]
pub struct Database {
    inner: Mutex<CoreDatabase>,
}

#[pymethods]
impl Database {
    /// Open or create a database file at ``path``.
    #[staticmethod]
    fn open(path: &str) -> PyResult<Self> {
        let db = CoreDatabase::open(path).map_err(fields_json::db_error_to_py)?;
        Ok(Self {
            inner: Mutex::new(db),
        })
    }

    fn path(&self) -> String {
        self.inner.lock().unwrap().path().display().to_string()
    }

    /// Register a collection with schema version 1.
    ///
    /// ``fields_json`` is a JSON array of objects like
    /// ``{"path": ["title"], "type": "string"}``. See README for the v1 shape.
    fn register_collection(&self, name: &str, fields_json: &str) -> PyResult<(u32, u32)> {
        let fields = fields_json::fields_from_json(fields_json).map_err(PyValueError::new_err)?;
        let mut g = self.inner.lock().unwrap();
        let (id, v) = g
            .register_collection(name, fields)
            .map_err(fields_json::db_error_to_py)?;
        Ok((id.0, v.0))
    }

    /// Sorted list of registered collection names.
    fn collection_names(&self) -> Vec<String> {
        self.inner.lock().unwrap().collection_names()
    }
}

#[pymodule]
fn typra(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__doc__", "Python bindings for Typra (preview).")?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_class::<Database>()?;
    Ok(())
}
