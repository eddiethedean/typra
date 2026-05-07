//! Validation at write time: types, nesting, and field constraints (0.6+).
//!
//! See [`docs/07_record_encoding_v2.md`](../../docs/07_record_encoding_v2.md) and [`ROADMAP.md`](../../ROADMAP.md).

use regex::Regex;

use crate::error::{DbError, ValidationError};
use crate::record::RowValue;
use crate::schema::{Constraint, FieldDef, Type};

fn err(path: &[String], msg: impl Into<String>) -> DbError {
    DbError::Validation(ValidationError {
        path: path.to_vec(),
        message: msg.into(),
    })
}

/// Primary key types must be flat primitives (not optional/composite).
pub fn ensure_pk_type_primitive(ty: &Type) -> Result<(), DbError> {
    match ty {
        Type::Bool
        | Type::Int64
        | Type::Uint64
        | Type::Float64
        | Type::String
        | Type::Bytes
        | Type::Uuid
        | Type::Timestamp => Ok(()),
        Type::Optional(_) | Type::List(_) | Type::Object(_) | Type::Enum(_) => {
            Err(DbError::Validation(ValidationError {
                path: vec![],
                message:
                    "primary key field must use a primitive type (not optional/list/object/enum)"
                        .into(),
            }))
        }
    }
}

/// Whether a missing map key is treated as absent (`Optional` only).
pub fn allows_absent_root(ty: &Type) -> bool {
    matches!(ty, Type::Optional(_))
}

/// Validate a row value against `ty` and apply `constraints`.
pub fn validate_value(
    path: &mut Vec<String>,
    ty: &Type,
    constraints: &[Constraint],
    v: &RowValue,
) -> Result<(), DbError> {
    match ty {
        Type::Optional(inner) => {
            if matches!(v, RowValue::None) {
                return Ok(());
            }
            validate_value(path, inner, &[], v)?;
            apply_constraints(path, ty, constraints, v)
        }
        Type::Bool => {
            let RowValue::Bool(_) = v else {
                return Err(err(path, "expected bool"));
            };
            apply_constraints(path, ty, constraints, v)
        }
        Type::Int64 => {
            let RowValue::Int64(_) = v else {
                return Err(err(path, "expected int64"));
            };
            apply_constraints(path, ty, constraints, v)
        }
        Type::Uint64 => {
            let RowValue::Uint64(_) = v else {
                return Err(err(path, "expected uint64"));
            };
            apply_constraints(path, ty, constraints, v)
        }
        Type::Float64 => {
            let RowValue::Float64(_) = v else {
                return Err(err(path, "expected float64"));
            };
            apply_constraints(path, ty, constraints, v)
        }
        Type::String => {
            let RowValue::String(_) = v else {
                return Err(err(path, "expected string"));
            };
            apply_constraints(path, ty, constraints, v)
        }
        Type::Bytes => {
            let RowValue::Bytes(_) = v else {
                return Err(err(path, "expected bytes"));
            };
            apply_constraints(path, ty, constraints, v)
        }
        Type::Uuid => {
            let RowValue::Uuid(_) = v else {
                return Err(err(path, "expected uuid"));
            };
            apply_constraints(path, ty, constraints, v)
        }
        Type::Timestamp => {
            let RowValue::Timestamp(_) = v else {
                return Err(err(path, "expected timestamp"));
            };
            apply_constraints(path, ty, constraints, v)
        }
        Type::List(inner) => {
            let RowValue::List(items) = v else {
                return Err(err(path, "expected list"));
            };
            for (i, item) in items.iter().enumerate() {
                path.push(format!("{i}"));
                validate_value(path, inner, &[], item)?;
                path.pop();
            }
            apply_constraints(path, ty, constraints, v)
        }
        Type::Object(fields) => {
            let RowValue::Object(m) = v else {
                return Err(err(path, "expected object"));
            };
            for sub in fields {
                let key = sub.path.0[0].to_string();
                let absent_ok = allows_absent_root(&sub.ty);
                let none = RowValue::None;
                let child: &RowValue = match m.get(&key) {
                    None if absent_ok => &none,
                    None => {
                        path.push(key.clone());
                        return Err(err(path, "missing object field"));
                    }
                    Some(x) => x,
                };
                path.push(key);
                validate_value(path, &sub.ty, &sub.constraints, child)?;
                path.pop();
            }
            for k in m.keys() {
                if !fields.iter().any(|f| f.path.0[0].as_ref() == k.as_str()) {
                    path.push(k.clone());
                    return Err(err(path, "unknown field in object"));
                }
            }
            apply_constraints(path, ty, constraints, v)
        }
        Type::Enum(variants) => {
            let RowValue::String(s) = v else {
                return Err(err(path, "expected string (enum)"));
            };
            if !variants.iter().any(|x| x == s) {
                return Err(err(
                    path,
                    format!("enum value must be one of {:?}", variants),
                ));
            }
            apply_constraints(path, ty, constraints, v)
        }
    }
}

fn apply_constraints(
    path: &[String],
    _ty: &Type,
    constraints: &[Constraint],
    v: &RowValue,
) -> Result<(), DbError> {
    for c in constraints {
        match c {
            Constraint::MinI64(min) => {
                let RowValue::Int64(n) = v else {
                    return Err(err(path, "MinI64 constraint requires int64"));
                };
                if *n < *min {
                    return Err(err(path, format!("value {n} is below minimum {min}")));
                }
            }
            Constraint::MaxI64(max) => {
                let RowValue::Int64(n) = v else {
                    return Err(err(path, "MaxI64 constraint requires int64"));
                };
                if *n > *max {
                    return Err(err(path, format!("value {n} is above maximum {max}")));
                }
            }
            Constraint::MinU64(min) => {
                let RowValue::Uint64(n) = v else {
                    return Err(err(path, "MinU64 constraint requires uint64"));
                };
                if *n < *min {
                    return Err(err(path, format!("value {n} is below minimum {min}")));
                }
            }
            Constraint::MaxU64(max) => {
                let RowValue::Uint64(n) = v else {
                    return Err(err(path, "MaxU64 constraint requires uint64"));
                };
                if *n > *max {
                    return Err(err(path, format!("value {n} is above maximum {max}")));
                }
            }
            Constraint::MinF64(min) => {
                let RowValue::Float64(n) = v else {
                    return Err(err(path, "MinF64 constraint requires float64"));
                };
                if *n < *min {
                    return Err(err(path, format!("value {n} is below minimum {min}")));
                }
            }
            Constraint::MaxF64(max) => {
                let RowValue::Float64(n) = v else {
                    return Err(err(path, "MaxF64 constraint requires float64"));
                };
                if *n > *max {
                    return Err(err(path, format!("value {n} is above maximum {max}")));
                }
            }
            Constraint::MinLength(min) => match v {
                RowValue::String(s) => {
                    if (s.len() as u64) < *min {
                        return Err(err(
                            path,
                            format!("string length {} is below minimum {}", s.len(), min),
                        ));
                    }
                }
                RowValue::Bytes(b) => {
                    if (b.len() as u64) < *min {
                        return Err(err(
                            path,
                            format!("bytes length {} is below minimum {}", b.len(), min),
                        ));
                    }
                }
                RowValue::List(items) => {
                    if (items.len() as u64) < *min {
                        return Err(err(
                            path,
                            format!("list length {} is below minimum {}", items.len(), min),
                        ));
                    }
                }
                _ => return Err(err(path, "MinLength applies to string, bytes, or list")),
            },
            Constraint::MaxLength(max) => match v {
                RowValue::String(s) => {
                    if (s.len() as u64) > *max {
                        return Err(err(
                            path,
                            format!("string length {} is above maximum {}", s.len(), max),
                        ));
                    }
                }
                RowValue::Bytes(b) => {
                    if (b.len() as u64) > *max {
                        return Err(err(
                            path,
                            format!("bytes length {} is above maximum {}", b.len(), max),
                        ));
                    }
                }
                RowValue::List(items) => {
                    if (items.len() as u64) > *max {
                        return Err(err(
                            path,
                            format!("list length {} is above maximum {}", items.len(), max),
                        ));
                    }
                }
                _ => return Err(err(path, "MaxLength applies to string, bytes, or list")),
            },
            Constraint::Regex(pattern) => {
                let RowValue::String(s) = v else {
                    return Err(err(path, "Regex constraint requires string"));
                };
                let re = Regex::new(pattern).map_err(|e| {
                    DbError::Validation(ValidationError {
                        path: path.to_vec(),
                        message: format!("invalid regex in schema: {e}"),
                    })
                })?;
                if !re.is_match(s) {
                    return Err(err(path, "string does not match regex"));
                }
            }
            Constraint::Email => {
                let RowValue::String(s) = v else {
                    return Err(err(path, "Email constraint requires string"));
                };
                if !s.contains('@') || !s.contains('.') {
                    return Err(err(path, "string is not a valid email shape"));
                }
            }
            Constraint::Url => {
                let RowValue::String(s) = v else {
                    return Err(err(path, "Url constraint requires string"));
                };
                if !s.starts_with("http://") && !s.starts_with("https://") {
                    return Err(err(path, "string must be an http(s) URL"));
                }
            }
            Constraint::NonEmpty => match v {
                RowValue::String(s) if s.is_empty() => {
                    return Err(err(path, "string must be non-empty"));
                }
                RowValue::Bytes(b) if b.is_empty() => {
                    return Err(err(path, "bytes must be non-empty"));
                }
                RowValue::List(items) if items.is_empty() => {
                    return Err(err(path, "list must be non-empty"));
                }
                RowValue::String(_) | RowValue::Bytes(_) | RowValue::List(_) => {}
                _ => return Err(err(path, "NonEmpty applies to string, bytes, or list")),
            },
        }
    }

    Ok(())
}

/// Validate top-level insert row: unknown fields, missing fields, types, constraints.
/// `row` must contain every top-level field (including the primary key).
pub fn validate_top_level_row(
    fields: &[FieldDef],
    pk_name: &str,
    row: &std::collections::BTreeMap<String, RowValue>,
) -> Result<(), DbError> {
    for k in row.keys() {
        if !fields
            .iter()
            .any(|f| f.path.0.len() == 1 && f.path.0[0].as_ref() == k.as_str())
        {
            return Err(DbError::Validation(ValidationError {
                path: vec![k.clone()],
                message: "unknown field".into(),
            }));
        }
    }

    for def in fields {
        let name = def.path.0[0].to_string();
        if name == pk_name {
            continue;
        }
        let absent_ok = allows_absent_root(&def.ty);
        let none = RowValue::None;
        let v: &RowValue = match row.get(&name) {
            None if absent_ok => &none,
            None => {
                return Err(DbError::Validation(ValidationError {
                    path: vec![name.clone()],
                    message: "missing field".into(),
                }));
            }
            Some(x) => x,
        };
        if matches!(v, RowValue::None) && !absent_ok {
            return Err(DbError::Validation(ValidationError {
                path: vec![name.clone()],
                message: "unexpected null for required field".into(),
            }));
        }
        let mut path = vec![name.clone()];
        validate_value(&mut path, &def.ty, &def.constraints, v)?;
    }
    Ok(())
}
