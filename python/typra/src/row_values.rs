//! Convert Python objects to [`typra_core::record::ScalarValue`] using each field's [`typra_core::schema::Type`].
//!
//! v1 rows support only top-level primitive fields; composite types raise `ValueError` until implemented.

use std::collections::BTreeMap;

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use pyo3::types::{PyAnyMethods, PyBytes, PyDict, PyDictMethods};
use typra_core::catalog::CollectionInfo;
use typra_core::record::ScalarValue;
use typra_core::schema::Type;

/// Build a full row map from a Python `dict` using top-level field names from `col`.
pub fn row_from_dict(
    py: Python<'_>,
    dict: &Bound<'_, PyDict>,
    col: &CollectionInfo,
) -> PyResult<BTreeMap<String, ScalarValue>> {
    let mut out = BTreeMap::new();
    for (k, v) in dict.iter() {
        let name: String = k.extract()?;
        let def = col
            .fields
            .iter()
            .find(|f| f.path.0.len() == 1 && f.path.0[0] == name.as_str())
            .ok_or_else(|| PyValueError::new_err(format!("unknown field {name:?}")))?;
        let sv = scalar_from_py(py, &v, &def.ty)?;
        out.insert(name, sv);
    }
    Ok(out)
}

/// Coerce `obj` to a [`ScalarValue`] according to `ty` (including UUID and timestamp rules).
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
            PyValueError::new_err("composite types are not supported in record row v1 yet"),
        ),
    }
}

/// Serialize a row map to a new Python `dict` (UUIDs become `uuid.UUID` instances).
pub fn row_to_dict<'py>(
    py: Python<'py>,
    row: &BTreeMap<String, ScalarValue>,
) -> PyResult<Bound<'py, PyDict>> {
    let d = PyDict::new_bound(py);
    for (k, v) in row {
        d.set_item(k, scalar_to_pyobject(py, v)?)?;
    }
    Ok(d)
}

fn scalar_to_pyobject(py: Python<'_>, v: &ScalarValue) -> PyResult<Py<PyAny>> {
    match v {
        ScalarValue::Bool(b) => Ok(b.into_py(py)),
        ScalarValue::Int64(n) => Ok(n.into_py(py)),
        ScalarValue::Uint64(n) => Ok(n.into_py(py)),
        ScalarValue::Float64(n) => Ok(n.into_py(py)),
        ScalarValue::String(s) => Ok(s.into_py(py)),
        ScalarValue::Bytes(b) => Ok(PyBytes::new_bound(py, b.as_slice()).into_any().unbind()),
        ScalarValue::Uuid(u) => {
            let uuid_mod = py.import_bound("uuid")?;
            let uuid_cls = uuid_mod.getattr("UUID")?;
            let kwargs = PyDict::new_bound(py);
            kwargs.set_item("bytes", PyBytes::new_bound(py, u))?;
            let uu = uuid_cls.call((), Some(&kwargs))?;
            Ok(uu.unbind())
        }
        ScalarValue::Timestamp(t) => Ok(t.into_py(py)),
    }
}
