//! Asyncio-first `AsyncDatabase` / `AsyncTransaction` over the sync engine.

use std::borrow::Cow;
use std::sync::Arc;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyDict};

use crate::async_query::AsyncCollection;
use crate::async_util::{future_into_blocking, future_into_gil_then_blocking, SharedInner};
use crate::database::schema_change_to_str;
use crate::db_handle::{
    collection_info, finish_transaction, lock_inner_read, lock_inner_write, DbHandle,
};
use crate::errors::db_error_to_py;
use crate::fields_json;
use crate::inner_db::InnerDb;
use crate::row_values;

/// Asyncio `Database` handle: same engine as [`crate::database::Database`], operations run on a thread pool.
#[pyclass(name = "AsyncDatabase", from_py_object)]
#[derive(Clone)]
pub struct AsyncDatabase {
    pub(crate) inner: SharedInner,
}

/// Async context manager for ``AsyncDatabase.transaction()``.
#[pyclass(name = "AsyncTransaction")]
pub struct AsyncTransaction {
    db: Py<AsyncDatabase>,
}

#[pymethods]
impl AsyncTransaction {
    fn __aenter__<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = {
            let db = self.db.bind(py).borrow();
            Arc::clone(&db.inner)
        };
        future_into_blocking(py, move || {
            let mut g = lock_inner_write(inner.as_ref())?;
            g.begin_transaction().map_err(db_error_to_py)?;
            inner.txn_enter();
            Ok(())
        })
    }

    #[pyo3(signature = (exc_type=None, _exc_value=None, _traceback=None))]
    fn __aexit__<'py>(
        &self,
        py: Python<'py>,
        exc_type: Option<&Bound<'_, PyAny>>,
        _exc_value: Option<&Bound<'_, PyAny>>,
        _traceback: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = {
            let db = self.db.bind(py).borrow();
            Arc::clone(&db.inner)
        };
        let had_exc = exc_type.is_some();
        future_into_blocking(py, move || {
            let txn_active = inner.txn_depth_active();
            let finish_result = match lock_inner_write(inner.as_ref()) {
                Ok(mut g) => finish_transaction(&mut g, had_exc).map_err(db_error_to_py),
                Err(e) => Err(e),
            };
            inner.txn_exit_if_active(txn_active);
            finish_result?;
            Ok(false)
        })
    }
}

#[pymethods]
impl AsyncDatabase {
    #[staticmethod]
    #[pyo3(signature = (path, *, read_only=false))]
    fn open<'py>(py: Python<'py>, path: String, read_only: bool) -> PyResult<Bound<'py, PyAny>> {
        future_into_blocking(py, move || {
            Ok(Self {
                inner: Arc::new(DbHandle::new(InnerDb::open_path(&path, read_only)?)),
            })
        })
    }

    #[staticmethod]
    fn open_in_memory<'py>(py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        future_into_blocking(py, move || {
            Ok(Self {
                inner: Arc::new(DbHandle::new(InnerDb::open_in_memory()?)),
            })
        })
    }

    #[staticmethod]
    fn open_snapshot_bytes<'py>(py: Python<'py>, data: Vec<u8>) -> PyResult<Bound<'py, PyAny>> {
        future_into_blocking(py, move || {
            Ok(Self {
                inner: Arc::new(DbHandle::new(InnerDb::from_snapshot_bytes(data)?)),
            })
        })
    }

    #[staticmethod]
    fn open_snapshot<'py>(py: Python<'py>, path: String) -> PyResult<Bound<'py, PyAny>> {
        future_into_blocking(py, move || {
            Ok(Self {
                inner: Arc::new(DbHandle::new(InnerDb::open_snapshot_path(&path)?)),
            })
        })
    }

    #[staticmethod]
    fn restore_snapshot<'py>(
        py: Python<'py>,
        snapshot_path: String,
        dest_path: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        future_into_blocking(py, move || {
            InnerDb::restore_snapshot_to_path(&snapshot_path, &dest_path)?;
            Ok(())
        })
    }

    fn path<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_blocking(py, move || {
            let g = lock_inner_read(inner.as_ref())?;
            Ok(g.path_display())
        })
    }

    #[pyo3(signature = (name, fields_json, primary_field, indexes_json=None))]
    fn register_collection<'py>(
        &self,
        py: Python<'py>,
        name: String,
        fields_json: String,
        primary_field: String,
        indexes_json: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_gil_then_blocking(py, move |py| {
            let fields =
                fields_json::fields_from_json(&fields_json).map_err(PyValueError::new_err)?;
            let indexes = match indexes_json.as_deref() {
                None => Vec::new(),
                Some(s) if s.trim().is_empty() => Vec::new(),
                Some(s) => {
                    fields_json::indexes_from_json(s, &fields).map_err(PyValueError::new_err)?
                }
            };
            let _ = py;
            Ok(move || {
                let mut g = lock_inner_write(inner.as_ref())?;
                let (id, v) = g
                    .register_collection_with_indexes(&name, fields, indexes, &primary_field)
                    .map_err(db_error_to_py)?;
                Ok((id.0, v.0))
            })
        })
    }

    fn collection_names<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_blocking(py, move || {
            let g = lock_inner_read(inner.as_ref())?;
            Ok(g.collection_names())
        })
    }

    fn collection(&self, py: Python<'_>, name: String) -> PyResult<AsyncCollection> {
        self.collection_handle(py, name)
    }

    fn insert<'py>(
        &self,
        py: Python<'py>,
        collection_name: String,
        row: Py<PyDict>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_gil_then_blocking(py, move |py| {
            let col = collection_info(inner.as_ref(), &collection_name)?;
            let row = row.bind(py);
            let mapped = row_values::row_from_dict(py, row, &col)?;
            Ok(move || {
                let mut g = lock_inner_write(inner.as_ref())?;
                let cid = g
                    .collection_id_named(&collection_name)
                    .map_err(db_error_to_py)?;
                g.insert(cid, mapped).map_err(db_error_to_py)?;
                Ok(())
            })
        })
    }

    fn delete<'py>(
        &self,
        py: Python<'py>,
        collection_name: String,
        pk: Py<PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_gil_then_blocking(py, move |py| {
            let col = collection_info(inner.as_ref(), &collection_name)?;
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
            let pk_val = row_values::scalar_from_py(py, pk.bind(py), pk_ty)?;
            Ok(move || {
                let mut g = lock_inner_write(inner.as_ref())?;
                let cid = g
                    .collection_id_named(&collection_name)
                    .map_err(db_error_to_py)?;
                g.delete(cid, &pk_val).map_err(db_error_to_py)?;
                Ok(())
            })
        })
    }

    #[pyo3(signature = (collection_name, fields_json, indexes_json=None, force=false))]
    fn register_schema_version<'py>(
        &self,
        py: Python<'py>,
        collection_name: String,
        fields_json: String,
        indexes_json: Option<String>,
        force: bool,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_gil_then_blocking(py, move |py| {
            let fields =
                fields_json::fields_from_json(&fields_json).map_err(PyValueError::new_err)?;
            let indexes = match indexes_json.as_deref() {
                None => Vec::new(),
                Some(s) if s.trim().is_empty() => Vec::new(),
                Some(s) => {
                    fields_json::indexes_from_json(s, &fields).map_err(PyValueError::new_err)?
                }
            };
            let _ = py;
            Ok(move || {
                let mut g = lock_inner_write(inner.as_ref())?;
                let cid = g
                    .collection_id_named(&collection_name)
                    .map_err(db_error_to_py)?;
                let v = if force {
                    g.register_schema_version_with_indexes_force(cid, fields, indexes)
                } else {
                    g.register_schema_version_with_indexes(cid, fields, indexes)
                }
                .map_err(db_error_to_py)?;
                Ok(v.0)
            })
        })
    }

    #[pyo3(signature = (collection_name, fields_json, indexes_json=None))]
    fn plan_schema_version<'py>(
        &self,
        py: Python<'py>,
        collection_name: String,
        fields_json: String,
        indexes_json: Option<String>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_gil_then_blocking(py, move |py| {
            let fields =
                fields_json::fields_from_json(&fields_json).map_err(PyValueError::new_err)?;
            let indexes = match indexes_json.as_deref() {
                None => Vec::new(),
                Some(s) if s.trim().is_empty() => Vec::new(),
                Some(s) => {
                    fields_json::indexes_from_json(s, &fields).map_err(PyValueError::new_err)?
                }
            };
            let _ = py;
            Ok(move || {
                let g = lock_inner_read(inner.as_ref())?;
                let cid = g
                    .collection_id_named(&collection_name)
                    .map_err(db_error_to_py)?;
                let plan = g
                    .plan_schema_version_with_indexes(cid, fields, indexes)
                    .map_err(db_error_to_py)?;
                Python::attach(|py| {
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
                            modelvault_core::MigrationStep::RebuildIndexes => {
                                "rebuild_indexes".to_string()
                            }
                        })
                        .collect();
                    d.set_item("steps", steps)?;
                    Ok(d.unbind())
                })
            })
        })
    }

    fn backfill_top_level_field<'py>(
        &self,
        py: Python<'py>,
        collection_name: String,
        field: String,
        value: Py<PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_gil_then_blocking(py, move |py| {
            let col = collection_info(inner.as_ref(), &collection_name)?;
            let def = col
                .fields
                .iter()
                .find(|f| f.path.0.len() == 1 && f.path.0[0].as_ref() == field.as_str())
                .ok_or_else(|| PyValueError::new_err(format!("unknown field {field:?}")))?;
            let rv = row_values::value_from_py(py, value.bind(py), &def.ty)?;
            Ok(move || {
                let mut g = lock_inner_write(inner.as_ref())?;
                let cid = g
                    .collection_id_named(&collection_name)
                    .map_err(db_error_to_py)?;
                g.backfill_top_level_field_with_value(cid, &field, rv)
                    .map_err(db_error_to_py)?;
                Ok(())
            })
        })
    }

    fn backfill_field_at_path<'py>(
        &self,
        py: Python<'py>,
        collection_name: String,
        path: Vec<String>,
        value: Py<PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_gil_then_blocking(py, move |py| {
            let col = collection_info(inner.as_ref(), &collection_name)?;
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
            let rv = row_values::value_from_py(py, value.bind(py), &def.ty)?;
            Ok(move || {
                let mut g = lock_inner_write(inner.as_ref())?;
                let cid = g
                    .collection_id_named(&collection_name)
                    .map_err(db_error_to_py)?;
                g.backfill_field_at_path_with_value(cid, &fp, rv)
                    .map_err(db_error_to_py)?;
                Ok(())
            })
        })
    }

    fn rebuild_indexes<'py>(
        &self,
        py: Python<'py>,
        collection_name: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_blocking(py, move || {
            let mut g = lock_inner_write(inner.as_ref())?;
            let cid = g
                .collection_id_named(&collection_name)
                .map_err(db_error_to_py)?;
            g.rebuild_indexes_for_collection(cid)
                .map_err(db_error_to_py)?;
            Ok(())
        })
    }

    fn get<'py>(
        &self,
        py: Python<'py>,
        collection_name: String,
        pk: Py<PyAny>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_gil_then_blocking(py, move |py| {
            let col = collection_info(inner.as_ref(), &collection_name)?;
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
            let pk_val = row_values::scalar_from_py(py, pk.bind(py), pk_ty)?;
            Ok(move || {
                let g = lock_inner_read(inner.as_ref())?;
                let cid = g
                    .collection_id_named(&collection_name)
                    .map_err(db_error_to_py)?;
                let row = g.get(cid, &pk_val).map_err(db_error_to_py)?;
                Python::attach(|py| match row {
                    None => Ok(None::<Py<PyDict>>),
                    Some(r) => Ok(Some(row_values::row_to_dict(py, &r)?.unbind())),
                })
            })
        })
    }

    fn snapshot_bytes<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_blocking(py, move || {
            let g = lock_inner_read(inner.as_ref())?;
            let v = g.snapshot_bytes()?;
            Python::attach(|py| Ok(PyBytes::new(py, &v).unbind()))
        })
    }

    fn export_snapshot<'py>(
        &self,
        py: Python<'py>,
        dest_path: String,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_blocking(py, move || {
            let mut g = lock_inner_write(inner.as_ref())?;
            g.export_snapshot_to_path(&dest_path)?;
            Ok(())
        })
    }

    fn compact_to<'py>(&self, py: Python<'py>, dest_path: String) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_blocking(py, move || {
            let g = lock_inner_write(inner.as_ref())?;
            g.compact_to(&dest_path)?;
            Ok(())
        })
    }

    fn compact<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = Arc::clone(&self.inner);
        future_into_blocking(py, move || {
            let mut g = lock_inner_write(inner.as_ref())?;
            g.compact_in_place()?;
            Ok(())
        })
    }

    #[pyo3(name = "transaction")]
    fn py_transaction(slf: PyRef<'_, Self>, py: Python<'_>) -> PyResult<AsyncTransaction> {
        let db: Py<AsyncDatabase> = slf.into_pyobject(py)?.unbind();
        Ok(AsyncTransaction { db })
    }
}

impl AsyncDatabase {
    pub(crate) fn collection_handle(
        &self,
        py: Python<'_>,
        name: String,
    ) -> PyResult<AsyncCollection> {
        let _ = collection_info(self.inner.as_ref(), &name)?;
        let db = Py::new(py, self.clone())?;
        Ok(AsyncCollection { db, name })
    }
}
