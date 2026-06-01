use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyDict, PyList, PyTuple};
use std::borrow::Cow;

use modelvault_core::db::row_subset_by_field_defs;
use modelvault_core::query::{OrderBy, OrderDirection, Predicate, Query};
use modelvault_core::schema::{FieldDef, FieldPath, Type};

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
        "path must be a dotted string (\"a.b\") or list[str]",
    ))
}

fn parse_path_or_field_ref(obj: &Bound<'_, PyAny>) -> PyResult<Vec<String>> {
    if let Ok(path) = obj.getattr("path") {
        if let Ok(parts) = path.extract::<Vec<String>>() {
            if !parts.is_empty() && !parts.iter().any(|p| p.is_empty()) {
                return Ok(parts);
            }
        }
    }
    parse_path(obj)
}

fn to_field_path(parts: &[String]) -> PyResult<FieldPath> {
    if parts.is_empty() || parts.iter().any(|p| p.is_empty()) {
        return Err(PyValueError::new_err("invalid path"));
    }
    Ok(FieldPath(
        parts.iter().map(|s| Cow::Owned(s.clone())).collect(),
    ))
}

fn resolve_leaf_type<'a>(
    col: &'a modelvault_core::catalog::CollectionInfo,
    fp: &FieldPath,
) -> Option<&'a Type> {
    if let Some(def) = col.fields.iter().find(|f| f.path == *fp) {
        return Some(&def.ty);
    }
    if fp.0.is_empty() {
        return None;
    }
    let root = col
        .fields
        .iter()
        .find(|f| f.path.0.len() == 1 && f.path.0[0] == fp.0[0])?;
    if fp.0.len() == 1 {
        return Some(&root.ty);
    }
    type_at_nested_segments(&root.ty, &fp.0[1..])
}

fn type_at_nested_segments<'a>(ty: &'a Type, segs: &[Cow<'static, str>]) -> Option<&'a Type> {
    if segs.is_empty() {
        return Some(ty);
    }
    match ty {
        Type::Optional(inner) => type_at_nested_segments(inner, segs),
        Type::Object(fields) => {
            let f = fields
                .iter()
                .find(|f| f.path.0.len() == 1 && f.path.0[0] == segs[0])?;
            type_at_nested_segments(&f.ty, &segs[1..])
        }
        _ => None,
    }
}

fn merge_and(existing: Option<Predicate>, new: Predicate) -> Predicate {
    match existing {
        None => new,
        Some(Predicate::And(mut items)) => {
            items.push(new);
            Predicate::And(items)
        }
        Some(p) => Predicate::And(vec![p, new]),
    }
}

fn scalar_for_path(
    py: Python<'_>,
    col: &modelvault_core::catalog::CollectionInfo,
    parts: &[String],
    value: &Bound<'_, PyAny>,
) -> PyResult<modelvault_core::ScalarValue> {
    let field_path = to_field_path(parts)?;
    let leaf_ty = resolve_leaf_type(col, &field_path)
        .ok_or_else(|| PyValueError::new_err("unknown field path"))?;
    row_values::scalar_from_py(py, value, leaf_ty)
}

fn field_defs_allowlist(
    _py: Python<'_>,
    col: &modelvault_core::catalog::CollectionInfo,
    fields: &Bound<'_, PyAny>,
) -> PyResult<Vec<FieldDef>> {
    let mut out = Vec::new();
    if let Ok(list) = fields.cast::<PyList>() {
        for item in list.iter() {
            out.push(one_path_to_field_def(col, &item)?);
        }
    } else if let Ok(tup) = fields.cast::<PyTuple>() {
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
    col: &modelvault_core::catalog::CollectionInfo,
    obj: &Bound<'_, PyAny>,
) -> PyResult<FieldDef> {
    let parts = parse_path_or_field_ref(obj)?;
    let fp = to_field_path(&parts)?;
    let Some(ty) = resolve_leaf_type(col, &fp) else {
        return Err(PyValueError::new_err(format!(
            "unknown field path for this collection: {:?}",
            parts
        )));
    };
    Ok(FieldDef {
        path: fp,
        ty: ty.clone(),
        constraints: vec![],
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
        self.start_query_with_cmp(py, path, value, |path, value| Predicate::Eq { path, value })
    }

    #[pyo3(signature = (fields=None))]
    fn all(&self, py: Python<'_>, fields: Option<Bound<'_, PyAny>>) -> PyResult<Vec<Py<PyDict>>> {
        QueryBuilder {
            db: self.db.clone_ref(py),
            collection_name: self.name.clone(),
            predicate: None,
            limit: None,
            order_by: None,
        }
        .all_impl(py, fields)
    }

    fn gte_where(
        &self,
        py: Python<'_>,
        path: &Bound<'_, PyAny>,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<QueryBuilder> {
        self.start_query_with_cmp(py, path, value, |path, value| Predicate::Gte {
            path,
            value,
        })
    }
}

impl Collection {
    fn start_query_with_cmp(
        &self,
        py: Python<'_>,
        path: &Bound<'_, PyAny>,
        value: &Bound<'_, PyAny>,
        make: fn(FieldPath, modelvault_core::ScalarValue) -> Predicate,
    ) -> PyResult<QueryBuilder> {
        let parts = parse_path_or_field_ref(path)?;
        let db_ref = self.db.borrow(py);
        let col = super::database::collection_info(&db_ref.inner, &self.name)?;
        let scalar = scalar_for_path(py, &col, &parts, value)?;
        let path_fp = to_field_path(&parts)?;
        Ok(QueryBuilder {
            db: self.db.clone_ref(py),
            collection_name: self.name.clone(),
            predicate: Some(make(path_fp, scalar)),
            limit: None,
            order_by: None,
        })
    }
}

#[pyclass(name = "Query")]
pub struct QueryBuilder {
    db: Py<Database>,
    collection_name: String,
    predicate: Option<Predicate>,
    limit: Option<usize>,
    order_by: Option<OrderBy>,
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
            let q = Query {
                collection: cid,
                predicate: self.predicate.clone(),
                limit: self.limit,
                order_by: self.order_by.clone(),
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

    fn with_cmp(
        &self,
        py: Python<'_>,
        path: &Bound<'_, PyAny>,
        value: &Bound<'_, PyAny>,
        make: fn(FieldPath, modelvault_core::ScalarValue) -> Predicate,
    ) -> PyResult<Self> {
        let parts = parse_path_or_field_ref(path)?;
        let db_ref = self.db.borrow(py);
        let col = super::database::collection_info(&db_ref.inner, &self.collection_name)?;
        let scalar = scalar_for_path(py, &col, &parts, value)?;
        let path_fp = to_field_path(&parts)?;
        Ok(Self {
            db: self.db.clone_ref(py),
            collection_name: self.collection_name.clone(),
            predicate: Some(merge_and(self.predicate.clone(), make(path_fp, scalar))),
            limit: self.limit,
            order_by: self.order_by.clone(),
        })
    }
}

#[pymethods]
impl QueryBuilder {
    fn limit(&self, py: Python<'_>, n: usize) -> PyResult<Self> {
        Ok(Self {
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
    ) -> PyResult<Self> {
        self.with_cmp(py, path, value, |path, value| Predicate::Eq { path, value })
    }

    fn lt_where(
        &self,
        py: Python<'_>,
        path: &Bound<'_, PyAny>,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<Self> {
        self.with_cmp(py, path, value, |path, value| Predicate::Lt { path, value })
    }

    fn lte_where(
        &self,
        py: Python<'_>,
        path: &Bound<'_, PyAny>,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<Self> {
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
    ) -> PyResult<Self> {
        self.with_cmp(py, path, value, |path, value| Predicate::Gt { path, value })
    }

    fn gte_where(
        &self,
        py: Python<'_>,
        path: &Bound<'_, PyAny>,
        value: &Bound<'_, PyAny>,
    ) -> PyResult<Self> {
        self.with_cmp(py, path, value, |path, value| Predicate::Gte {
            path,
            value,
        })
    }

    fn or_where(&self, py: Python<'_>, other: &QueryBuilder) -> PyResult<Self> {
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
        Ok(Self {
            db: self.db.clone_ref(py),
            collection_name: self.collection_name.clone(),
            predicate: combined,
            limit: self.limit.or(other.limit),
            order_by: self.order_by.clone().or(other.order_by.clone()),
        })
    }

    #[pyo3(signature = (path, *, desc=false))]
    fn order_by(&self, py: Python<'_>, path: &Bound<'_, PyAny>, desc: bool) -> PyResult<Self> {
        let parts = parse_path_or_field_ref(path)?;
        Ok(Self {
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

    fn explain(&self, py: Python<'_>) -> PyResult<String> {
        let db_ref = self.db.borrow(py);
        let g = super::database::lock_inner(&db_ref.inner)?;
        let cid = g
            .collection_id_named(&self.collection_name)
            .map_err(db_error_to_py)?;
        let q = Query {
            collection: cid,
            predicate: self.predicate.clone(),
            limit: self.limit,
            order_by: self.order_by.clone(),
        };
        g.explain_query(&q).map_err(db_error_to_py)
    }

    #[pyo3(signature = (fields=None))]
    fn all(&self, py: Python<'_>, fields: Option<Bound<'_, PyAny>>) -> PyResult<Vec<Py<PyDict>>> {
        self.all_impl(py, fields)
    }
}
