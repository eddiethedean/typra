use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyDict, PyList, PyTuple};

use typra_core::catalog::CollectionInfo;
use typra_core::db::row_subset_by_field_defs;
use typra_core::query::{Predicate, Query};
use typra_core::schema::{FieldDef, FieldPath, Type};

use crate::database::Database;
use crate::errors::db_error_to_py;
use crate::row_values;

#[pyfunction]
pub fn connect(py: Python<'_>, path: String) -> PyResult<Connection> {
    let db = typra_core::Database::open(&path).map_err(db_error_to_py)?;
    let py_db = Py::new(
        py,
        Database {
            inner: std::sync::Mutex::new(crate::inner_db::InnerDb::File(db)),
        },
    )?;
    Ok(Connection {
        db: py_db,
        closed: false,
    })
}

#[pyclass]
pub struct Connection {
    db: Py<Database>,
    closed: bool,
}

#[pymethods]
impl Connection {
    fn cursor(&self, py: Python<'_>) -> PyResult<Cursor> {
        if self.closed {
            return Err(PyRuntimeError::new_err("connection is closed"));
        }
        Ok(Cursor {
            conn: self.db.clone_ref(py),
            rows: Vec::new(),
            pos: 0,
            description: None,
            closed: false,
        })
    }

    fn close(&mut self) {
        self.closed = true;
    }

    fn commit(&self) -> PyResult<()> {
        // Read-only DB-API v1: no-op (exists for compatibility).
        Ok(())
    }

    fn rollback(&self) -> PyResult<()> {
        // Read-only DB-API v1: no-op (exists for compatibility).
        Ok(())
    }
}

#[pyclass]
pub struct Cursor {
    conn: Py<Database>,
    rows: Vec<Py<PyTuple>>,
    pos: usize,
    description: Option<Py<PyAny>>,
    closed: bool,
}

fn sql_path_to_parts(p: &FieldPath) -> Vec<String> {
    p.0.iter().map(|s| s.to_string()).collect()
}

fn find_leaf_type<'a>(col: &'a CollectionInfo, path: &FieldPath) -> Option<&'a Type> {
    col.fields.iter().find(|f| &f.path == path).map(|f| &f.ty)
}

fn scalar_param(
    py: Python<'_>,
    col: &CollectionInfo,
    path: &FieldPath,
    params: &Bound<'_, PyAny>,
    idx: usize,
) -> PyResult<typra_core::ScalarValue> {
    let ty = find_leaf_type(col, path).ok_or_else(|| {
        PyValueError::new_err(format!("unknown field path {:?}", sql_path_to_parts(path)))
    })?;
    let item = params.get_item(idx)?;
    row_values::scalar_from_py(py, &item, ty)
}

fn build_predicate(
    py: Python<'_>,
    col: &CollectionInfo,
    sql_pred: &typra_core::sql::SqlPredicate,
    params: &Bound<'_, PyAny>,
) -> PyResult<Predicate> {
    use typra_core::sql::SqlPredicate as SP;
    use typra_core::sql::SqlValue as SV;
    match sql_pred {
        SP::Eq { path, value }
        | SP::Lt { path, value }
        | SP::Lte { path, value }
        | SP::Gt { path, value }
        | SP::Gte { path, value } => {
            let SV::Param(i) = value;
            let scalar = scalar_param(py, col, path, params, *i)?;
            Ok(match sql_pred {
                SP::Eq { path, .. } => Predicate::Eq {
                    path: path.clone(),
                    value: scalar,
                },
                SP::Lt { path, .. } => Predicate::Lt {
                    path: path.clone(),
                    value: scalar,
                },
                SP::Lte { path, .. } => Predicate::Lte {
                    path: path.clone(),
                    value: scalar,
                },
                SP::Gt { path, .. } => Predicate::Gt {
                    path: path.clone(),
                    value: scalar,
                },
                SP::Gte { path, .. } => Predicate::Gte {
                    path: path.clone(),
                    value: scalar,
                },
                _ => unreachable!(),
            })
        }
        SP::And(items) => Ok(Predicate::And(
            items
                .iter()
                .map(|x| build_predicate(py, col, x, params))
                .collect::<PyResult<Vec<_>>>()?,
        )),
        SP::Or(items) => Ok(Predicate::Or(
            items
                .iter()
                .map(|x| build_predicate(py, col, x, params))
                .collect::<PyResult<Vec<_>>>()?,
        )),
    }
}

fn projection_field_defs(col: &CollectionInfo, paths: &[FieldPath]) -> PyResult<Vec<FieldDef>> {
    let mut defs = Vec::with_capacity(paths.len());
    for p in paths {
        let def = col.fields.iter().find(|f| f.path == *p).ok_or_else(|| {
            PyValueError::new_err(format!("unknown field path {:?}", sql_path_to_parts(p)))
        })?;
        defs.push(def.clone());
    }
    Ok(defs)
}

fn schema_top_level_paths(col: &CollectionInfo) -> Vec<FieldPath> {
    col.fields
        .iter()
        .filter(|f| f.path.0.len() == 1)
        .map(|f| f.path.clone())
        .collect()
}

fn py_get_at_path(
    py: Python<'_>,
    mut obj: Bound<'_, PyAny>,
    parts: &[String],
) -> PyResult<PyObject> {
    for (i, seg) in parts.iter().enumerate() {
        if obj.is_none() {
            return Ok(py.None());
        }
        let d = obj
            .downcast::<PyDict>()
            .map_err(|_| PyValueError::new_err(format!("expected dict at path segment {i}")))?;
        match d.get_item(seg)? {
            Some(v) => obj = v,
            None => return Ok(py.None()),
        }
    }
    Ok(obj.into_py(py))
}

fn make_description(py: Python<'_>, names: &[String]) -> PyResult<Py<PyAny>> {
    let mut cols = Vec::with_capacity(names.len());
    for n in names {
        // PEP 249: 7-item sequence; only name is required by most consumers.
        let item = PyTuple::new_bound(
            py,
            [
                n.into_py(py),
                py.None(),
                py.None(),
                py.None(),
                py.None(),
                py.None(),
                py.None(),
            ],
        );
        cols.push(item.unbind());
    }
    Ok(PyTuple::new_bound(py, cols).into_any().unbind())
}

#[pymethods]
impl Cursor {
    #[getter]
    fn description(&self, py: Python<'_>) -> PyResult<PyObject> {
        Ok(self
            .description
            .as_ref()
            .map(|x| x.clone_ref(py).into_py(py))
            .unwrap_or_else(|| py.None()))
    }

    fn close(&mut self) {
        self.closed = true;
        self.rows.clear();
        self.pos = 0;
        self.description = None;
    }

    #[pyo3(signature = (sql, params=None))]
    fn execute(
        &mut self,
        py: Python<'_>,
        sql: String,
        params: Option<Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        if self.closed {
            return Err(PyRuntimeError::new_err("cursor is closed"));
        }
        let parsed = typra_core::sql::parse_select(&sql).map_err(db_error_to_py)?;

        let params_obj = match params {
            None => PyTuple::empty_bound(py).into_any(),
            Some(p) => {
                if p.is_none() {
                    PyTuple::empty_bound(py).into_any()
                } else if let Ok(t) = p.downcast::<PyTuple>() {
                    t.clone().into_any()
                } else if let Ok(l) = p.downcast::<PyList>() {
                    l.clone().into_any()
                } else {
                    return Err(PyValueError::new_err("params must be a tuple or list"));
                }
            }
        };
        let params_len = params_obj.len()?;
        if params_len != parsed.param_count {
            return Err(PyValueError::new_err(format!(
                "expected {} SQL parameters, got {}",
                parsed.param_count, params_len
            )));
        }

        let (col, rows) = {
            let db_ref = self.conn.borrow(py);
            let col = super::database::collection_info(&db_ref.inner, &parsed.collection)?;
            let g = super::database::lock_inner(&db_ref.inner)?;
            let cid = g
                .collection_id_named(&parsed.collection)
                .map_err(db_error_to_py)?;
            let pred = match &parsed.predicate {
                None => None,
                Some(p) => Some(build_predicate(py, &col, p, &params_obj)?),
            };
            let q = Query {
                collection: cid,
                predicate: pred,
                limit: parsed.limit,
                order_by: parsed.order_by.clone(),
            };
            let rows = g.query(&q).map_err(db_error_to_py)?;
            (col, rows)
        };

        // Projection: define column order + description.
        let (paths, names, allow_defs) = match &parsed.columns {
            typra_core::sql::SqlColumns::Star => {
                let p = schema_top_level_paths(&col);
                let n = p
                    .iter()
                    .map(|fp| fp.0[0].as_ref().to_string())
                    .collect::<Vec<_>>();
                (p, n, None)
            }
            typra_core::sql::SqlColumns::Paths(paths) => {
                let n = paths
                    .iter()
                    .map(|fp| {
                        fp.0.iter()
                            .map(|s| s.as_ref())
                            .collect::<Vec<_>>()
                            .join(".")
                    })
                    .collect::<Vec<_>>();
                let defs = projection_field_defs(&col, paths)?;
                (paths.clone(), n, Some(defs))
            }
        };

        self.description = Some(make_description(py, &names)?);
        self.rows.clear();
        self.pos = 0;

        for r in rows {
            let projected = match &allow_defs {
                None => r,
                Some(defs) => row_subset_by_field_defs(&r, defs),
            };
            // Convert dict -> tuple in selected column order.
            let d = row_values::row_to_dict(py, &projected)?;
            let mut items = Vec::with_capacity(paths.len());
            for p in &paths {
                let parts = sql_path_to_parts(p);
                let v = py_get_at_path(py, d.clone().into_any(), &parts)?;
                items.push(v);
            }
            self.rows.push(PyTuple::new_bound(py, items).unbind());
        }

        Ok(())
    }

    fn fetchone(&mut self, py: Python<'_>) -> PyResult<PyObject> {
        if self.pos >= self.rows.len() {
            return Ok(py.None());
        }
        let r = self.rows[self.pos].clone_ref(py);
        self.pos += 1;
        Ok(r.into_py(py))
    }

    #[pyo3(signature = (size=1))]
    fn fetchmany(&mut self, py: Python<'_>, size: usize) -> PyResult<PyObject> {
        let end = std::cmp::min(self.pos + size, self.rows.len());
        let out = PyList::empty_bound(py);
        for i in self.pos..end {
            out.append(self.rows[i].clone_ref(py))?;
        }
        self.pos = end;
        Ok(out.into_py(py))
    }

    fn fetchall(&mut self, py: Python<'_>) -> PyResult<PyObject> {
        let out = PyList::empty_bound(py);
        while self.pos < self.rows.len() {
            out.append(self.rows[self.pos].clone_ref(py))?;
            self.pos += 1;
        }
        Ok(out.into_py(py))
    }
}
