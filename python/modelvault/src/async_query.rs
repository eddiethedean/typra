//! Async query builder types for [`crate::async_database::AsyncDatabase`].

use std::sync::Arc;

use modelvault_core::db::row_subset_by_field_defs;
use modelvault_core::query::{OrderBy, OrderDirection, Predicate, Query};
use modelvault_core::schema::FieldPath;
use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;

use crate::async_database::AsyncDatabase;
use crate::async_util::{future_into_blocking, future_into_gil_then_blocking};
use crate::db_handle::{collection_info, lock_inner_read};
use crate::errors::db_error_to_py;
use crate::query::{
    field_defs_allowlist, merge_and, parse_path_or_field_ref, scalar_for_path, to_field_path,
};
use crate::row_values;

#[pyclass(name = "AsyncCollection")]
pub struct AsyncCollection {
    pub(crate) db: Py<AsyncDatabase>,
    pub(crate) name: String,
}

#[pyclass(name = "AsyncQuery")]
pub struct AsyncQueryBuilder {
    db: Py<AsyncDatabase>,
    collection_name: String,
    predicate: Option<Predicate>,
    limit: Option<usize>,
    order_by: Option<OrderBy>,
}

impl AsyncQueryBuilder {
    fn all_impl<'py>(
        &self,
        py: Python<'py>,
        fields: Option<Py<PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let inner = {
            let db = self.db.bind(py).borrow();
            Arc::clone(&db.inner)
        };
        let collection_name = self.collection_name.clone();
        let predicate = self.predicate.clone();
        let limit = self.limit;
        let order_by = self.order_by.clone();
        future_into_gil_then_blocking(py, move |py| {
            let col = collection_info(inner.as_ref(), &collection_name)?;
            let allow = match fields.as_ref() {
                None => None,
                Some(f) => Some(field_defs_allowlist(py, &col, f.bind(py))?),
            };
            Ok(move || {
                let g = lock_inner_read(inner.as_ref())?;
                let cid = g
                    .collection_id_named(&collection_name)
                    .map_err(db_error_to_py)?;
                let q = Query {
                    collection: cid,
                    predicate,
                    limit,
                    order_by,
                };
                let rows = g.query(&q).map_err(db_error_to_py)?;
                Python::attach(|py| {
                    let mut out = Vec::with_capacity(rows.len());
                    for r in rows {
                        let dict = match &allow {
                            None => row_values::row_to_dict(py, &r)?,
                            Some(defs) => {
                                let sub = row_subset_by_field_defs(&r, defs);
                                row_values::row_to_dict(py, &sub)?
                            }
                        };
                        out.push(dict.unbind());
                    }
                    Ok(out)
                })
            })
        })
    }

    fn with_cmp(
        &self,
        py: Python<'_>,
        path: &Bound<'_, PyAny>,
        value: &Bound<'_, PyAny>,
        make: fn(FieldPath, modelvault_core::ScalarValue) -> Predicate,
    ) -> PyResult<AsyncQueryBuilder> {
        let parts = parse_path_or_field_ref(path)?;
        let inner = {
            let db = self.db.bind(py).borrow();
            Arc::clone(&db.inner)
        };
        let col = collection_info(inner.as_ref(), &self.collection_name)?;
        let scalar = scalar_for_path(py, &col, &parts, value)?;
        let path_fp = to_field_path(&parts)?;
        Ok(AsyncQueryBuilder {
            db: self.db.clone_ref(py),
            collection_name: self.collection_name.clone(),
            predicate: Some(merge_and(self.predicate.clone(), make(path_fp, scalar))),
            limit: self.limit,
            order_by: self.order_by.clone(),
        })
    }
}

impl AsyncCollection {
    pub(crate) fn where_query(
        &self,
        py: Python<'_>,
        path: &Bound<'_, PyAny>,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<AsyncQueryBuilder> {
        self.start_query_with_cmp(py, path, value, |path, value| Predicate::Eq { path, value })
    }

    pub(crate) fn all_rows<'py>(
        &self,
        py: Python<'py>,
        fields: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        let fields = fields.map(|f| f.unbind());
        let qb = AsyncQueryBuilder {
            db: self.db.clone_ref(py),
            collection_name: self.name.clone(),
            predicate: None,
            limit: None,
            order_by: None,
        };
        qb.all_impl(py, fields)
    }

    fn start_query_with_cmp(
        &self,
        py: Python<'_>,
        path: &Bound<'_, PyAny>,
        value: &Bound<'_, PyAny>,
        make: fn(FieldPath, modelvault_core::ScalarValue) -> Predicate,
    ) -> PyResult<AsyncQueryBuilder> {
        let parts = parse_path_or_field_ref(path)?;
        let inner = {
            let db = self.db.bind(py).borrow();
            Arc::clone(&db.inner)
        };
        let col = collection_info(inner.as_ref(), &self.name)?;
        let scalar = scalar_for_path(py, &col, &parts, value)?;
        let path_fp = to_field_path(&parts)?;
        Ok(AsyncQueryBuilder {
            db: self.db.clone_ref(py),
            collection_name: self.name.clone(),
            predicate: Some(make(path_fp, scalar)),
            limit: None,
            order_by: None,
        })
    }
}

#[pymethods]
impl AsyncCollection {
    #[pyo3(name = "where")]
    fn where_(
        &self,
        py: Python<'_>,
        path: &Bound<'_, PyAny>,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<AsyncQueryBuilder> {
        self.start_query_with_cmp(py, path, value, |path, value| Predicate::Eq { path, value })
    }

    #[pyo3(signature = (fields=None))]
    fn all<'py>(
        &self,
        py: Python<'py>,
        fields: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.all_rows(py, fields)
    }

    fn gte_where(
        &self,
        py: Python<'_>,
        path: &Bound<'_, PyAny>,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<AsyncQueryBuilder> {
        self.start_query_with_cmp(py, path, value, |path, value| Predicate::Gte {
            path,
            value,
        })
    }
}

#[pymethods]
impl AsyncQueryBuilder {
    fn limit(&self, py: Python<'_>, n: usize) -> PyResult<AsyncQueryBuilder> {
        Ok(AsyncQueryBuilder {
            db: self.db.clone_ref(py),
            collection_name: self.collection_name.clone(),
            predicate: self.predicate.clone(),
            limit: Some(n),
            order_by: self.order_by.clone(),
        })
    }

    fn and_where(
        &self,
        py: Python<'_>,
        path: &Bound<'_, PyAny>,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<AsyncQueryBuilder> {
        self.with_cmp(py, path, value, |path, value| Predicate::Eq { path, value })
    }

    fn lt_where(
        &self,
        py: Python<'_>,
        path: &Bound<'_, PyAny>,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<AsyncQueryBuilder> {
        self.with_cmp(py, path, value, |path, value| Predicate::Lt { path, value })
    }

    fn lte_where(
        &self,
        py: Python<'_>,
        path: &Bound<'_, PyAny>,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<AsyncQueryBuilder> {
        self.with_cmp(py, path, value, |path, value| Predicate::Lte {
            path,
            value,
        })
    }

    fn gt_where(
        &self,
        py: Python<'_>,
        path: &Bound<'_, PyAny>,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<AsyncQueryBuilder> {
        self.with_cmp(py, path, value, |path, value| Predicate::Gt { path, value })
    }

    fn gte_where(
        &self,
        py: Python<'_>,
        path: &Bound<'_, PyAny>,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<AsyncQueryBuilder> {
        self.with_cmp(py, path, value, |path, value| Predicate::Gte {
            path,
            value,
        })
    }

    fn or_where(&self, py: Python<'_>, other: &AsyncQueryBuilder) -> PyResult<AsyncQueryBuilder> {
        if self.collection_name != other.collection_name {
            return Err(PyValueError::new_err(
                "or_where requires the same collection",
            ));
        }
        let left = self.predicate.clone();
        let right = other.predicate.clone();
        let combined = match (left, right) {
            (None, None) => None,
            (Some(p), None) | (None, Some(p)) => Some(p),
            (Some(a), Some(b)) => Some(Predicate::Or(vec![a, b])),
        };
        Ok(AsyncQueryBuilder {
            db: self.db.clone_ref(py),
            collection_name: self.collection_name.clone(),
            predicate: combined,
            limit: self.limit.or(other.limit),
            order_by: self.order_by.clone().or(other.order_by.clone()),
        })
    }

    #[pyo3(signature = (path, *, desc=false))]
    fn order_by(
        &self,
        py: Python<'_>,
        path: &Bound<'_, PyAny>,
        desc: bool,
    ) -> PyResult<AsyncQueryBuilder> {
        let parts = parse_path_or_field_ref(path)?;
        Ok(AsyncQueryBuilder {
            db: self.db.clone_ref(py),
            collection_name: self.collection_name.clone(),
            predicate: self.predicate.clone(),
            limit: self.limit,
            order_by: Some(OrderBy {
                path: to_field_path(&parts)?,
                direction: if desc {
                    OrderDirection::Desc
                } else {
                    OrderDirection::Asc
                },
            }),
        })
    }

    fn explain<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        let inner = {
            let db = self.db.bind(py).borrow();
            Arc::clone(&db.inner)
        };
        let collection_name = self.collection_name.clone();
        let predicate = self.predicate.clone();
        let limit = self.limit;
        let order_by = self.order_by.clone();
        future_into_blocking(py, move || {
            let g = lock_inner_read(inner.as_ref())?;
            let cid = g
                .collection_id_named(&collection_name)
                .map_err(db_error_to_py)?;
            let q = Query {
                collection: cid,
                predicate,
                limit,
                order_by,
            };
            g.explain_query(&q).map_err(db_error_to_py)
        })
    }

    #[pyo3(signature = (fields=None))]
    fn all<'py>(
        &self,
        py: Python<'py>,
        fields: Option<Bound<'py, PyAny>>,
    ) -> PyResult<Bound<'py, PyAny>> {
        self.all_impl(py, fields.map(|f| f.unbind()))
    }
}
