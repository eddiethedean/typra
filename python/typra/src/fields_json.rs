//! JSON parsing for ``register_collection`` on the Python [`crate::database::Database`].
//!
//! Accepts the v1 subset of field definitions (paths, primitives, optional/list/object/enum).

use std::borrow::Cow;

use serde_json::Value;
use typra_core::schema::{Constraint, FieldDef, FieldPath, Type};

/// Parse the `fields_json` string passed to ``Database.register_collection`` into engine [`FieldDef`] values.
///
/// On failure returns a human-readable message string (surfaced as `ValueError` in Python).
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
    let constraints = obj
        .get("constraints")
        .map(constraints_from_json_value)
        .transpose()?
        .unwrap_or_default();
    Ok(FieldDef {
        path,
        ty,
        constraints,
    })
}

fn constraints_from_json_value(v: &Value) -> Result<Vec<Constraint>, String> {
    let arr = v
        .as_array()
        .ok_or_else(|| "\"constraints\" must be an array".to_string())?;
    let mut out = Vec::new();
    for item in arr {
        let o = item
            .as_object()
            .ok_or_else(|| "each constraint must be a JSON object".to_string())?;
        if let Some(n) = o.get("min_i64").and_then(|x| x.as_i64()) {
            out.push(Constraint::MinI64(n));
            continue;
        }
        if let Some(n) = o.get("max_i64").and_then(|x| x.as_i64()) {
            out.push(Constraint::MaxI64(n));
            continue;
        }
        if let Some(n) = o.get("min_u64").and_then(|x| x.as_u64()) {
            out.push(Constraint::MinU64(n));
            continue;
        }
        if let Some(n) = o.get("max_u64").and_then(|x| x.as_u64()) {
            out.push(Constraint::MaxU64(n));
            continue;
        }
        if let Some(n) = o.get("min_f64").and_then(|x| x.as_f64()) {
            out.push(Constraint::MinF64(n));
            continue;
        }
        if let Some(n) = o.get("max_f64").and_then(|x| x.as_f64()) {
            out.push(Constraint::MaxF64(n));
            continue;
        }
        if let Some(n) = o.get("min_length").and_then(|x| x.as_u64()) {
            out.push(Constraint::MinLength(n));
            continue;
        }
        if let Some(n) = o.get("max_length").and_then(|x| x.as_u64()) {
            out.push(Constraint::MaxLength(n));
            continue;
        }
        if let Some(s) = o.get("regex").and_then(|x| x.as_str()) {
            out.push(Constraint::Regex(s.to_string()));
            continue;
        }
        if o.get("email").and_then(|x| x.as_bool()) == Some(true) {
            out.push(Constraint::Email);
            continue;
        }
        if o.get("url").and_then(|x| x.as_bool()) == Some(true) {
            out.push(Constraint::Url);
            continue;
        }
        if o.get("nonempty").and_then(|x| x.as_bool()) == Some(true) {
            out.push(Constraint::NonEmpty);
            continue;
        }
        return Err("unknown constraint shape".to_string());
    }
    Ok(out)
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
