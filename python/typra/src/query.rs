use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};
use std::borrow::Cow;

use typra_core::db::row_subset_by_field_defs;
use typra_core::query::{Predicate, Query};
use typra_core::schema::{FieldDef, FieldPath};

use crate::database::Database;
use crate::errors::db_error_to_py;
use crate::row_values;

fn parse_path(obj: &Bound<'_, PyAny>) -> PyResult<Vec<String>> {
    if let Ok(s) = obj.extract::<String>() {
        let raw = s.trim();
        if raw.is_empty() {
            return Err(PyValueError::new_err("path must be non-empty"));
        }
        return Ok(raw.split('.').map(|x: &str| x.to_string()).collect());
    }
    if let Ok(parts) = obj.extract::<Vec<String>>() {
        if parts.is_empty() {
            return Err(PyValueError::new_err("path must have at least one segment"));
        }
        if parts.iter().any(|p| p.is_empty()) {
            return Err(PyValueError::new_err("path segments must be non-empty"));
        }
        return Ok(parts);
    }
    Err(PyValueError::new_err(
        "path must be a dotted string (\"a.b\") or a tuple of strings (\"a\",\"b\")",
    ))
}

fn to_field_path(parts: &[String]) -> PyResult<FieldPath> {
    if parts.is_empty() || parts.iter().any(|p| p.is_empty()) {
        return Err(PyValueError::new_err("invalid path"));
    }
    Ok(FieldPath(
        parts.iter().map(|s| Cow::Owned(s.clone())).collect(),
    ))
}

fn build_predicate(
    predicates: &[(Vec<String>, typra_core::ScalarValue)],
) -> PyResult<Option<Predicate>> {
    if predicates.is_empty() {
        return Ok(None);
    }
    let mut items = Vec::with_capacity(predicates.len());
    for (parts, value) in predicates {
        let path = to_field_path(parts)?;
        items.push(Predicate::Eq {
            path,
            value: value.clone(),
        });
    }
    Ok(if items.len() == 1 {
        Some(items.remove(0))
    } else {
        Some(Predicate::And(items))
    })
}

fn field_defs_allowlist(
    _py: Python<'_>,
    col: &typra_core::catalog::CollectionInfo,
    fields: &Bound<'_, PyAny>,
) -> PyResult<Vec<FieldDef>> {
    let mut out = Vec::new();
    if let Ok(list) = fields.downcast::<PyList>() {
        for item in list.iter() {
            out.push(one_path_to_field_def(col, &item)?);
        }
    } else if let Ok(tup) = fields.downcast::<PyTuple>() {
        for item in tup.iter() {
            out.push(one_path_to_field_def(col, &item)?);
        }
    } else {
        out.push(one_path_to_field_def(col, fields)?);
    }
    if out.is_empty() {
        return Err(PyValueError::new_err(
            "fields must list at least one schema path",
        ));
    }
    Ok(out)
}

fn one_path_to_field_def(
    col: &typra_core::catalog::CollectionInfo,
    obj: &Bound<'_, PyAny>,
) -> PyResult<FieldDef> {
    let parts = parse_path(obj)?;
    let fp = to_field_path(&parts)?;
    col.fields
        .iter()
        .find(|f| f.path == fp)
        .cloned()
        .ok_or_else(|| {
            PyValueError::new_err(format!(
                "unknown field path for this collection: {:?}",
                parts
            ))
        })
}

#[pyclass]
pub struct Collection {
    pub(crate) db: Py<Database>,
    pub(crate) name: String,
}

#[pymethods]
impl Collection {
    #[pyo3(name = "where")]
    fn where_(
        &self,
        py: Python<'_>,
        path: &Bound<'_, PyAny>,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<QueryBuilder> {
        let parts = parse_path(path)?;
        let db_ref = self.db.borrow(py);
        let col = super::database::collection_info(&db_ref.inner, &self.name)?;
        let field_path = to_field_path(&parts)?;
        let leaf_ty = col
            .fields
            .iter()
            .find(|f| f.path == field_path)
            .map(|f| &f.ty)
            .ok_or_else(|| PyValueError::new_err("unknown field path"))?;
        let scalar = row_values::scalar_from_py(py, value, leaf_ty)?;
        let db = self.db.clone_ref(py);
        Ok(QueryBuilder {
            db,
            collection_name: self.name.clone(),
            predicates: vec![(parts, scalar)],
            limit: None,
        })
    }

    #[pyo3(signature = (fields=None))]
    fn all(&self, py: Python<'_>, fields: Option<Bound<'_, PyAny>>) -> PyResult<Vec<Py<PyDict>>> {
        QueryBuilder {
            db: self.db.clone_ref(py),
            collection_name: self.name.clone(),
            predicates: Vec::new(),
            limit: None,
        }
        .all_impl(py, fields)
    }
}

#[pyclass(name = "Query")]
pub struct QueryBuilder {
    db: Py<Database>,
    collection_name: String,
    predicates: Vec<(Vec<String>, typra_core::ScalarValue)>,
    limit: Option<usize>,
}

impl QueryBuilder {
    fn all_impl(
        &self,
        py: Python<'_>,
        fields: Option<Bound<'_, PyAny>>,
    ) -> PyResult<Vec<Py<PyDict>>> {
        let col = {
            let db_ref = self.db.borrow(py);
            super::database::collection_info(&db_ref.inner, &self.collection_name)?
        };
        let allow = match fields {
            None => None,
            Some(f) => Some(field_defs_allowlist(py, &col, &f)?),
        };
        let rows = {
            let db_ref = self.db.borrow(py);
            let g = super::database::lock_inner(&db_ref.inner)?;
            let cid = g
                .collection_id_named(&self.collection_name)
                .map_err(db_error_to_py)?;
            let pred = build_predicate(&self.predicates)?;
            let q = Query {
                collection: cid,
                predicate: pred,
                limit: self.limit,
                order_by: None,
            };
            g.query(&q).map_err(db_error_to_py)?
        };
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
    }
}

#[pymethods]
impl QueryBuilder {
    fn limit(&self, py: Python<'_>, n: usize) -> PyResult<Self> {
        Ok(Self {
            db: self.db.clone_ref(py),
            collection_name: self.collection_name.clone(),
            predicates: self.predicates.clone(),
            limit: Some(n),
        })
    }

    /// Add another equality predicate; combined with previous filters using logical AND.
    fn and_where(
        &self,
        py: Python<'_>,
        path: &Bound<'_, PyAny>,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<Self> {
        let mut preds = self.predicates.clone();
        let parts = parse_path(path)?;
        let db_ref = self.db.borrow(py);
        let col = super::database::collection_info(&db_ref.inner, &self.collection_name)?;
        let field_path = to_field_path(&parts)?;
        let leaf_ty = col
            .fields
            .iter()
            .find(|f| f.path == field_path)
            .map(|f| &f.ty)
            .ok_or_else(|| PyValueError::new_err("unknown field path"))?;
        let scalar = row_values::scalar_from_py(py, value, leaf_ty)?;
        preds.push((parts, scalar));
        Ok(Self {
            db: self.db.clone_ref(py),
            collection_name: self.collection_name.clone(),
            predicates: preds,
            limit: self.limit,
        })
    }

    fn explain(&self, py: Python<'_>) -> PyResult<String> {
        let db_ref = self.db.borrow(py);
        let g = super::database::lock_inner(&db_ref.inner)?;
        let cid = g
            .collection_id_named(&self.collection_name)
            .map_err(db_error_to_py)?;
        let pred = build_predicate(&self.predicates)?;
        let q = Query {
            collection: cid,
            predicate: pred,
            limit: self.limit,
            order_by: None,
        };
        g.explain_query(&q).map_err(db_error_to_py)
    }

    #[pyo3(signature = (fields=None))]
    fn all(&self, py: Python<'_>, fields: Option<Bound<'_, PyAny>>) -> PyResult<Vec<Py<PyDict>>> {
        self.all_impl(py, fields)
    }
}
