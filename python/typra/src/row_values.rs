//! Convert Python objects to [`typra_core::record::RowValue`] using each field's [`typra_core::schema::Type`].

use std::collections::BTreeMap;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyBytes, PyDict, PyDictMethods, PyList, PyListMethods};
use typra_core::catalog::CollectionInfo;
use typra_core::record::{RowValue, ScalarValue};
use typra_core::schema::{FieldDef, Type};

/// Build a full row map from a Python `dict` using top-level field names from `col`.
pub fn row_from_dict(
    py: Python<'_>,
    dict: &Bound<'_, PyDict>,
    col: &CollectionInfo,
) -> PyResult<BTreeMap<String, RowValue>> {
    let has_multi_segment_schema = col.fields.iter().any(|f| f.path.0.len() != 1);
    if !has_multi_segment_schema {
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
        return Ok(out);
    }

    let pk_name = col.primary_field.as_deref();

    // Group schema defs by root segment.
    let mut by_root: BTreeMap<String, Vec<&FieldDef>> = BTreeMap::new();
    for f in &col.fields {
        if f.path.0.is_empty() {
            continue;
        }
        let root = f.path.0[0].as_ref().to_string();
        by_root.entry(root).or_default().push(f);
    }

    fn build_nested_object(
        py: Python<'_>,
        obj: &Bound<'_, PyAny>,
        defs: &[&FieldDef],
        prefix_len: usize,
    ) -> PyResult<RowValue> {
        let d = obj
            .cast::<PyDict>()
            .map_err(|_| PyValueError::new_err("expected dict for nested object"))?;
        let mut out = BTreeMap::new();

        // Group by next segment.
        let mut by_seg: BTreeMap<String, Vec<&FieldDef>> = BTreeMap::new();
        for def in defs {
            if def.path.0.len() <= prefix_len {
                continue;
            }
            let seg = def.path.0[prefix_len].as_ref().to_string();
            by_seg.entry(seg).or_default().push(*def);
        }

        // Convert known keys.
        for (seg, group) in &by_seg {
            // If there is an exact leaf def at this level, use its type directly.
            let leaf_def = group.iter().find(|f| f.path.0.len() == prefix_len + 1);
            match d.get_item(seg)? {
                None => {
                    if let Some(ld) = leaf_def {
                        if matches!(ld.ty, Type::Optional(_)) {
                            out.insert(seg.clone(), RowValue::None);
                            continue;
                        }
                    }
                    // If there are deeper defs, missing object implies missing required leafs unless all are optional.
                    let all_optional = group.iter().all(|f| matches!(f.ty, Type::Optional(_)));
                    if all_optional {
                        out.insert(seg.clone(), RowValue::None);
                        continue;
                    }
                    return Err(PyValueError::new_err(format!("missing field {seg:?}")));
                }
                Some(v) => {
                    if let Some(ld) = leaf_def {
                        out.insert(seg.clone(), value_from_py(py, &v, &ld.ty)?);
                    } else {
                        out.insert(
                            seg.clone(),
                            build_nested_object(py, &v, group, prefix_len + 1)?,
                        );
                    }
                }
            }
        }

        // Reject unknown keys in this object.
        for (k, _v) in d.iter() {
            let ks: String = k.extract()?;
            if !by_seg.contains_key(&ks) {
                return Err(PyValueError::new_err(format!(
                    "unknown field in object: {ks:?}"
                )));
            }
        }

        Ok(RowValue::Object(out))
    }

    let mut out = BTreeMap::new();
    for (k, v) in dict.iter() {
        let name: String = k.extract()?;
        let Some(defs) = by_root.get(&name) else {
            return Err(PyValueError::new_err(format!("unknown field {name:?}")));
        };
        // PK must remain a top-level scalar field def.
        if pk_name == Some(name.as_str()) {
            let pk_def = defs
                .iter()
                .find(|d| d.path.0.len() == 1)
                .ok_or_else(|| PyValueError::new_err("primary field not in schema"))?;
            out.insert(name, value_from_py(py, &v, &pk_def.ty)?);
            continue;
        }
        let top_level_def = defs.iter().find(|d| d.path.0.len() == 1);
        if let Some(d0) = top_level_def {
            out.insert(name, value_from_py(py, &v, &d0.ty)?);
        } else {
            out.insert(name, build_nested_object(py, &v, defs, 1)?);
        }
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
                .cast::<PyList>()
                .map_err(|_| PyValueError::new_err("expected list for list type"))?;
            let mut items = Vec::with_capacity(list.len());
            for item in list.iter() {
                items.push(value_from_py(py, &item, inner)?);
            }
            Ok(RowValue::List(items))
        }
        Type::Object(fields) => {
            let d = obj
                .cast::<PyDict>()
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
            let uuid_cls = py.import("uuid")?.getattr("UUID")?;
            let bytes: Vec<u8> = if let Ok(b) = obj.extract::<[u8; 16]>() {
                b.to_vec()
            } else if let Ok(b) = obj.cast::<PyBytes>() {
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
    let d = PyDict::new(py);
    for (k, v) in row {
        d.set_item(k, row_value_to_py(py, v)?)?;
    }
    Ok(d)
}

fn row_value_to_py(py: Python<'_>, v: &RowValue) -> PyResult<Py<PyAny>> {
    match v {
        RowValue::Bool(b) => Ok(b.into_pyobject(py)?.to_owned().unbind().into()),
        RowValue::Int64(n) => Ok(n.into_pyobject(py)?.unbind().into()),
        RowValue::Uint64(n) => Ok(n.into_pyobject(py)?.unbind().into()),
        RowValue::Float64(n) => Ok(n.into_pyobject(py)?.unbind().into()),
        RowValue::String(s) => Ok(s.into_pyobject(py)?.unbind().into()),
        RowValue::Bytes(b) => Ok(PyBytes::new(py, b.as_slice()).into_any().unbind()),
        RowValue::Uuid(u) => {
            let uuid_mod = py.import("uuid")?;
            let uuid_cls = uuid_mod.getattr("UUID")?;
            let kwargs = PyDict::new(py);
            kwargs.set_item("bytes", PyBytes::new(py, u))?;
            let uu = uuid_cls.call((), Some(&kwargs))?;
            Ok(uu.unbind())
        }
        RowValue::Timestamp(t) => Ok(t.into_pyobject(py)?.unbind().into()),
        RowValue::None => {
            let n: Option<i32> = None;
            Ok(n.into_pyobject(py)?.unbind().into())
        }
        RowValue::List(items) => {
            let list = PyList::empty(py);
            for item in items {
                list.append(row_value_to_py(py, item)?)?;
            }
            Ok(list.into_any().unbind())
        }
        RowValue::Object(m) => {
            let d = PyDict::new(py);
            for (k, rv) in m {
                d.set_item(k, row_value_to_py(py, rv)?)?;
            }
            Ok(d.into_any().unbind())
        }
    }
}
