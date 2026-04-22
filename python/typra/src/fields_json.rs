//! Parse `fields_json` for [`crate::Database::register_collection`] (v1 schema subset).

use std::borrow::Cow;

use serde_json::Value;
use typra_core::schema::{FieldDef, FieldPath, Type};

pub fn fields_from_json(s: &str) -> Result<Vec<FieldDef>, String> {
    let v: Value = serde_json::from_str(s).map_err(|e| e.to_string())?;
    let arr = v
        .as_array()
        .ok_or_else(|| "fields_json must be a JSON array".to_string())?;
    let mut out = Vec::with_capacity(arr.len());
    for item in arr {
        out.push(field_def_from_json_value(item)?);
    }
    Ok(out)
}

fn field_def_from_json_value(v: &Value) -> Result<FieldDef, String> {
    let obj = v
        .as_object()
        .ok_or_else(|| "each field must be a JSON object".to_string())?;
    let path = obj
        .get("path")
        .ok_or_else(|| "missing \"path\"".to_string())?;
    let parts: Vec<Cow<'static, str>> = path
        .as_array()
        .ok_or_else(|| "\"path\" must be an array of strings".to_string())?
        .iter()
        .map(|p| {
            p.as_str()
                .map(|s| Cow::Owned(s.to_string()))
                .ok_or_else(|| "path segment must be a string".to_string())
        })
        .collect::<Result<_, _>>()?;
    let path = FieldPath::new(parts).map_err(|e| e.to_string())?;
    let ty = obj
        .get("type")
        .ok_or_else(|| "missing \"type\"".to_string())?;
    let ty = type_from_json_value(ty)?;
    Ok(FieldDef { path, ty })
}

fn type_from_json_value(v: &Value) -> Result<Type, String> {
    if let Some(s) = v.as_str() {
        return primitive_type(s);
    }
    let o = v
        .as_object()
        .ok_or_else(|| "\"type\" must be a string or object".to_string())?;
    if let Some(inner) = o.get("optional") {
        return Ok(Type::Optional(Box::new(type_from_json_value(inner)?)));
    }
    if let Some(inner) = o.get("list") {
        return Ok(Type::List(Box::new(type_from_json_value(inner)?)));
    }
    if let Some(arr) = o.get("object").and_then(|x| x.as_array()) {
        let mut fields = Vec::with_capacity(arr.len());
        for item in arr {
            fields.push(field_def_from_json_value(item)?);
        }
        return Ok(Type::Object(fields));
    }
    if let Some(arr) = o.get("enum").and_then(|x| x.as_array()) {
        let mut variants = Vec::with_capacity(arr.len());
        for item in arr {
            let s = item
                .as_str()
                .ok_or_else(|| "enum variant must be a string".to_string())?;
            variants.push(s.to_string());
        }
        return Ok(Type::Enum(variants));
    }
    Err("unsupported \"type\" shape".to_string())
}

fn primitive_type(s: &str) -> Result<Type, String> {
    Ok(match s {
        "bool" => Type::Bool,
        "int64" => Type::Int64,
        "uint64" => Type::Uint64,
        "float64" => Type::Float64,
        "string" => Type::String,
        "bytes" => Type::Bytes,
        "uuid" => Type::Uuid,
        "timestamp" => Type::Timestamp,
        _ => return Err(format!("unknown primitive type {s:?}")),
    })
}
