//! PyO3 `Database` class: file- and memory-backed [`crate::inner_db::InnerDb`] with concurrent reads.

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};

use crate::db_handle::{
    collection_info, finish_transaction, lock_inner_read, lock_inner_write, DbHandle,
};
use crate::errors::db_error_to_py;
use crate::fields_json;
use crate::inner_db::InnerDb;
use crate::query as query_api;
use crate::row_values;

pub(crate) fn schema_change_to_str(
    change: &modelvault_core::schema::SchemaChange,
) -> (&'static str, Option<&str>) {
    match change {
        modelvault_core::schema::SchemaChange::Safe => ("safe", None),
        modelvault_core::schema::SchemaChange::NeedsMigration { reason, .. } => {
            ("needs_migration", Some(reason.as_str()))
        }
        modelvault_core::schema::SchemaChange::Breaking { reason } => {
            ("breaking", Some(reason.as_str()))
        }
    }
}

/// Python `Database`: ModelVault engine with concurrent reads and exclusive writes.
#[pyclass(name = "Database")]
pub struct Database {
    pub(crate) inner: DbHandle,
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
            let mut g = lock_inner_write(&db.inner)?;
            g.begin_transaction().map_err(db_error_to_py)?;
            db.inner.txn_enter();
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
        let db = self.db.bind(py).borrow();
        let txn_active = db.inner.txn_depth_active();
        let finish_result = match lock_inner_write(&db.inner) {
            Ok(mut g) => finish_transaction(&mut g, exc_type.is_some()).map_err(db_error_to_py),
            Err(e) => Err(e),
        };
        db.inner.txn_exit_if_active(txn_active);
        finish_result?;
        Ok(false)
    }
}

#[pymethods]
impl Database {
    #[staticmethod]
    #[pyo3(signature = (path, *, read_only=false, recovery=None))]
    fn open(path: &str, read_only: bool, recovery: Option<&str>) -> PyResult<Self> {
        Ok(Self {
            inner: DbHandle::new(InnerDb::open_path(path, read_only, recovery)?),
        })
    }

    fn path(&self) -> PyResult<String> {
        let g = lock_inner_read(&self.inner)?;
        Ok(g.path_display())
    }

    #[getter]
    fn recovery_info(&self, py: Python<'_>) -> PyResult<Py<PyAny>> {
        let g = lock_inner_read(&self.inner)?;
        let info = g.recovery_info();
        let dict = PyDict::new(py);
        dict.set_item("truncated_bytes", info.truncated_bytes)?;
        dict.set_item("truncate_reason", info.truncate_reason)?;
        Ok(dict.into())
    }

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
        let mut g = lock_inner_write(&self.inner)?;
        let (id, v) = g
            .register_collection_with_indexes(name, fields, indexes, primary_field)
            .map_err(db_error_to_py)?;
        Ok((id.0, v.0))
    }

    fn collection_names(&self) -> PyResult<Vec<String>> {
        let g = lock_inner_read(&self.inner)?;
        Ok(g.collection_names())
    }

    fn collection(
        slf: PyRef<'_, Self>,
        py: Python<'_>,
        name: &str,
    ) -> PyResult<query_api::Collection> {
        let _ = collection_info(&slf.inner, name)?;
        let db: Py<Database> = slf.into_pyobject(py)?.unbind();
        Ok(query_api::Collection {
            db,
            name: name.to_string(),
        })
    }

    fn insert(
        &self,
        py: Python<'_>,
        collection_name: &str,
        row: &Bound<'_, PyDict>,
    ) -> PyResult<()> {
        let col = collection_info(&self.inner, collection_name)?;
        let mapped = row_values::row_from_dict(py, row, &col)?;
        let mut g = lock_inner_write(&self.inner)?;
        let cid = g
            .collection_id_named(collection_name)
            .map_err(db_error_to_py)?;
        g.insert(cid, mapped).map_err(db_error_to_py)
    }

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
        let mut g = lock_inner_write(&self.inner)?;
        let cid = g
            .collection_id_named(collection_name)
            .map_err(db_error_to_py)?;
        g.delete(cid, &pk_val).map_err(db_error_to_py)
    }

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
        let mut g = lock_inner_write(&self.inner)?;
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
        let g = lock_inner_read(&self.inner)?;
        let cid = g
            .collection_id_named(collection_name)
            .map_err(db_error_to_py)?;
        let plan = g
            .plan_schema_version_with_indexes(cid, fields, indexes)
            .map_err(db_error_to_py)?;
        let d = PyDict::new(py);
        let (kind, reason) = schema_change_to_str(&plan.change);
        d.set_item("change", kind)?;
        if let Some(r) = reason {
            d.set_item("reason", r)?;
        }
        let steps: Vec<String> = plan
            .steps
            .into_iter()
            .map(|s| match s {
                modelvault_core::MigrationStep::BackfillTopLevelField { field } => {
                    format!("backfill_top_level_field:{field}")
                }
                modelvault_core::MigrationStep::BackfillFieldAtPath { path } => {
                    let segs: Vec<&str> = path.0.iter().map(|s| s.as_ref()).collect();
                    format!("backfill_field_at_path:{}", segs.join("."))
                }
                modelvault_core::MigrationStep::RebuildIndexes => "rebuild_indexes".to_string(),
            })
            .collect();
        d.set_item("steps", steps)?;
        Ok(d.unbind())
    }

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
        let mut g = lock_inner_write(&self.inner)?;
        let cid = g
            .collection_id_named(collection_name)
            .map_err(db_error_to_py)?;
        g.backfill_top_level_field_with_value(cid, field, rv)
            .map_err(db_error_to_py)
    }

    fn backfill_field_at_path(
        &self,
        py: Python<'_>,
        collection_name: &str,
        path: Vec<String>,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<()> {
        use std::borrow::Cow;
        let col = collection_info(&self.inner, collection_name)?;
        let fp = modelvault_core::schema::FieldPath(
            path.iter()
                .map(|s| Cow::Owned(s.clone()))
                .collect::<Vec<_>>(),
        );
        let def = col
            .fields
            .iter()
            .find(|f| f.path == fp)
            .ok_or_else(|| PyValueError::new_err(format!("unknown field path {path:?}")))?;
        let rv = row_values::value_from_py(py, value, &def.ty)?;
        let mut g = lock_inner_write(&self.inner)?;
        let cid = g
            .collection_id_named(collection_name)
            .map_err(db_error_to_py)?;
        g.backfill_field_at_path_with_value(cid, &fp, rv)
            .map_err(db_error_to_py)
    }

    fn rebuild_indexes(&self, collection_name: &str) -> PyResult<()> {
        let mut g = lock_inner_write(&self.inner)?;
        let cid = g
            .collection_id_named(collection_name)
            .map_err(db_error_to_py)?;
        g.rebuild_indexes_for_collection(cid)
            .map_err(db_error_to_py)
    }

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
            let g = lock_inner_read(&self.inner)?;
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

    #[staticmethod]
    fn open_in_memory() -> PyResult<Self> {
        Ok(Self {
            inner: DbHandle::new(InnerDb::open_in_memory()?),
        })
    }

    #[staticmethod]
    fn open_snapshot_bytes(data: &[u8]) -> PyResult<Self> {
        Ok(Self {
            inner: DbHandle::new(InnerDb::from_snapshot_bytes(data.to_vec())?),
        })
    }

    #[staticmethod]
    fn open_snapshot(path: &str) -> PyResult<Self> {
        Ok(Self {
            inner: DbHandle::new(InnerDb::open_snapshot_path(path)?),
        })
    }

    fn snapshot_bytes(&self, py: Python<'_>) -> PyResult<Py<PyBytes>> {
        let g = lock_inner_read(&self.inner)?;
        let v = g.snapshot_bytes()?;
        Ok(PyBytes::new(py, &v).unbind())
    }

    fn export_snapshot(&self, dest_path: &str) -> PyResult<()> {
        let mut g = lock_inner_write(&self.inner)?;
        g.export_snapshot_to_path(dest_path)
    }

    #[staticmethod]
    fn restore_snapshot(snapshot_path: &str, dest_path: &str) -> PyResult<()> {
        InnerDb::restore_snapshot_to_path(snapshot_path, dest_path)
    }

    fn compact_to(&self, dest_path: &str) -> PyResult<()> {
        let g = lock_inner_write(&self.inner)?;
        g.compact_to(dest_path)
    }

    fn compact(&self) -> PyResult<()> {
        let mut g = lock_inner_write(&self.inner)?;
        g.compact_in_place()
    }

    fn checkpoint(&self) -> PyResult<()> {
        let mut g = lock_inner_write(&self.inner)?;
        g.checkpoint().map_err(db_error_to_py)
    }

    #[pyo3(name = "transaction")]
    fn py_transaction(slf: PyRef<'_, Self>, py: Python<'_>) -> PyResult<Py<PyTransaction>> {
        let db: Py<Database> = slf.into_pyobject(py)?.unbind();
        Py::new(py, PyTransaction { db })
    }
}
