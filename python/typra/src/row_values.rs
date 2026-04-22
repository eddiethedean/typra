//! Convert Python objects to [`typra_core::record::RowValue`] using each field's [`typra_core::schema::Type`].

use std::collections::BTreeMap;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyBytes, PyDict, PyDictMethods, PyList, PyListMethods};
use pyo3::IntoPy;
use typra_core::catalog::CollectionInfo;
use typra_core::record::{RowValue, ScalarValue};
use typra_core::schema::Type;

/// Build a full row map from a Python `dict` using top-level field names from `col`.
pub fn row_from_dict(
    py: Python<'_>,
    dict: &Bound<'_, PyDict>,
    col: &CollectionInfo,
) -> PyResult<BTreeMap<String, RowValue>> {
    let mut out = BTreeMap::new();
    for (k, v) in dict.iter() {
        let name: String = k.extract()?;
        let def = col
            .fields
            .iter()
            .find(|f| f.path.0.len() == 1 && f.path.0[0].as_ref() == name.as_str())
            .ok_or_else(|| PyValueError::new_err(format!("unknown field {name:?}")))?;
        let rv = value_from_py(py, &v, &def.ty)?;
        out.insert(name, rv);
    }
    Ok(out)
}

/// Coerce a Python value to [`RowValue`] according to `ty`.
pub fn value_from_py(py: Python<'_>, obj: &Bound<'_, PyAny>, ty: &Type) -> PyResult<RowValue> {
    match ty {
        Type::Optional(inner) => {
            if obj.is_none() {
                return Ok(RowValue::None);
            }
            value_from_py(py, obj, inner)
        }
        Type::List(inner) => {
            let list = obj
                .downcast::<PyList>()
                .map_err(|_| PyValueError::new_err("expected list for list type"))?;
            let mut items = Vec::with_capacity(list.len());
            for item in list.iter() {
                items.push(value_from_py(py, &item, inner)?);
            }
            Ok(RowValue::List(items))
        }
        Type::Object(fields) => {
            let d = obj
                .downcast::<PyDict>()
                .map_err(|_| PyValueError::new_err("expected dict for object type"))?;
            let mut map = BTreeMap::new();
            for sub in fields {
                let key = sub.path.0[0].to_string();
                let absent_ok = matches!(sub.ty, Type::Optional(_));
                match d.get_item(&key)? {
                    None if absent_ok => {
                        map.insert(key, RowValue::None);
                    }
                    None => {
                        return Err(PyValueError::new_err(format!(
                            "missing object field {key:?}"
                        )));
                    }
                    Some(x) => {
                        if x.is_none() && matches!(sub.ty, Type::Optional(_)) {
                            map.insert(key, RowValue::None);
                        } else if x.is_none() {
                            return Err(PyValueError::new_err(format!(
                                "unexpected None for required field {key:?}"
                            )));
                        } else {
                            map.insert(key, value_from_py(py, &x, &sub.ty)?);
                        }
                    }
                }
            }
            for item in d.iter() {
                let (k, _v) = item;
                let ks: String = k.extract()?;
                if !fields.iter().any(|f| f.path.0[0].as_ref() == ks.as_str()) {
                    return Err(PyValueError::new_err(format!(
                        "unknown field in object: {ks:?}"
                    )));
                }
            }
            Ok(RowValue::Object(map))
        }
        Type::Enum(_) => {
            let s: String = obj
                .extract()
                .map_err(|_| PyValueError::new_err("expected string for enum"))?;
            Ok(RowValue::String(s))
        }
        _ => {
            let s = scalar_from_py(py, obj, ty)?;
            Ok(RowValue::from_scalar(s))
        }
    }
}

/// Coerce `obj` to a [`ScalarValue`] for primitive `ty` (including UUID and timestamp rules).
pub fn scalar_from_py(py: Python<'_>, obj: &Bound<'_, PyAny>, ty: &Type) -> PyResult<ScalarValue> {
    match ty {
        Type::Bool => obj.extract::<bool>().map(ScalarValue::Bool),
        Type::Int64 => obj.extract::<i64>().map(ScalarValue::Int64),
        Type::Uint64 => obj.extract::<u64>().map(ScalarValue::Uint64),
        Type::Float64 => obj.extract::<f64>().map(ScalarValue::Float64),
        Type::String => obj.extract::<String>().map(ScalarValue::String),
        Type::Bytes => obj.extract::<Vec<u8>>().map(ScalarValue::Bytes),
        Type::Uuid => {
            let uuid_cls = py.import_bound("uuid")?.getattr("UUID")?;
            let bytes: Vec<u8> = if let Ok(b) = obj.extract::<[u8; 16]>() {
                b.to_vec()
            } else if let Ok(b) = obj.downcast::<PyBytes>() {
                b.as_bytes().to_vec()
            } else if obj.is_instance(&uuid_cls)? {
                obj.getattr("bytes")?.extract::<Vec<u8>>()?
            } else {
                let u = uuid_cls.call1((obj,))?;
                u.getattr("bytes")?.extract::<Vec<u8>>()?
            };
            if bytes.len() != 16 {
                return Err(PyValueError::new_err("uuid must be 16 bytes"));
            }
            let mut arr = [0u8; 16];
            arr.copy_from_slice(&bytes);
            Ok(ScalarValue::Uuid(arr))
        }
        Type::Timestamp => obj.extract::<i64>().map(ScalarValue::Timestamp),
        Type::Optional(_) | Type::List(_) | Type::Object(_) | Type::Enum(_) => Err(
            PyValueError::new_err("internal: scalar_from_py called on composite type"),
        ),
    }
}

/// Serialize a row map to a new Python `dict` (UUIDs become `uuid.UUID` instances).
pub fn row_to_dict<'py>(
    py: Python<'py>,
    row: &BTreeMap<String, RowValue>,
) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new_bound(py);
    for (k, v) in row {
        d.set_item(k, row_value_to_py(py, v)?)?;
    }
    Ok(d)
}

fn row_value_to_py(py: Python<'_>, v: &RowValue) -> PyResult<Py<PyAny>> {
    match v {
        RowValue::Bool(b) => Ok(b.into_py(py)),
        RowValue::Int64(n) => Ok(n.into_py(py)),
        RowValue::Uint64(n) => Ok(n.into_py(py)),
        RowValue::Float64(n) => Ok(n.into_py(py)),
        RowValue::String(s) => Ok(s.into_py(py)),
        RowValue::Bytes(b) => Ok(PyBytes::new_bound(py, b.as_slice()).into_any().unbind()),
        RowValue::Uuid(u) => {
            let uuid_mod = py.import_bound("uuid")?;
            let uuid_cls = uuid_mod.getattr("UUID")?;
            let kwargs = PyDict::new_bound(py);
            kwargs.set_item("bytes", PyBytes::new_bound(py, u))?;
            let uu = uuid_cls.call((), Some(&kwargs))?;
            Ok(uu.unbind())
        }
        RowValue::Timestamp(t) => Ok(t.into_py(py)),
        RowValue::None => {
            let n: Option<i32> = None;
            Ok(n.into_py(py))
        }
        RowValue::List(items) => {
            let list = PyList::empty_bound(py);
            for item in items {
                list.append(row_value_to_py(py, item)?)?;
            }
            Ok(list.into_any().unbind())
        }
        RowValue::Object(m) => {
            let d = PyDict::new_bound(py);
            for (k, rv) in m {
                d.set_item(k, row_value_to_py(py, rv)?)?;
            }
            Ok(d.into_any().unbind())
        }
    }
}
