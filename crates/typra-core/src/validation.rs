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

fn must_int64(path: &[String], v: &RowValue, requirement: &'static str) -> Result<i64, DbError> {
    let RowValue::Int64(n) = v else {
        return Err(err(path, requirement));
    };
    Ok(*n)
}

fn must_uint64(path: &[String], v: &RowValue, requirement: &'static str) -> Result<u64, DbError> {
    let RowValue::Uint64(n) = v else {
        return Err(err(path, requirement));
    };
    Ok(*n)
}

fn must_f64(path: &[String], v: &RowValue, requirement: &'static str) -> Result<f64, DbError> {
    let RowValue::Float64(n) = v else {
        return Err(err(path, requirement));
    };
    Ok(*n)
}

fn constrain_min_i64(path: &[String], n: i64, min: i64) -> Result<(), DbError> {
    if n < min {
        Err(err(path, format!("value {n} is below minimum {min}")))
    } else {
        Ok(())
    }
}

fn constrain_max_i64(path: &[String], n: i64, max: i64) -> Result<(), DbError> {
    if n > max {
        Err(err(path, format!("value {n} is above maximum {max}")))
    } else {
        Ok(())
    }
}

fn constrain_min_u64(path: &[String], n: u64, min: u64) -> Result<(), DbError> {
    if n < min {
        Err(err(path, format!("value {n} is below minimum {min}")))
    } else {
        Ok(())
    }
}

fn constrain_max_u64(path: &[String], n: u64, max: u64) -> Result<(), DbError> {
    if n > max {
        Err(err(path, format!("value {n} is above maximum {max}")))
    } else {
        Ok(())
    }
}

fn constrain_min_f64(path: &[String], n: f64, min: f64) -> Result<(), DbError> {
    if n < min {
        Err(err(path, format!("value {n} is below minimum {min}")))
    } else {
        Ok(())
    }
}

fn constrain_max_f64(path: &[String], n: f64, max: f64) -> Result<(), DbError> {
    if n > max {
        Err(err(path, format!("value {n} is above maximum {max}")))
    } else {
        Ok(())
    }
}

fn constrain_min_byte_len(
    path: &[String],
    len: usize,
    min: u64,
    kind: &str,
) -> Result<(), DbError> {
    if (len as u64) < min {
        Err(err(
            path,
            format!("{kind} length {len} is below minimum {min}"),
        ))
    } else {
        Ok(())
    }
}

fn constrain_max_byte_len(
    path: &[String],
    len: usize,
    max: u64,
    kind: &str,
) -> Result<(), DbError> {
    if (len as u64) > max {
        Err(err(
            path,
            format!("{kind} length {len} is above maximum {max}"),
        ))
    } else {
        Ok(())
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
                let n = must_int64(path, v, "MinI64 constraint requires int64")?;
                constrain_min_i64(path, n, *min)?;
            }
            Constraint::MaxI64(max) => {
                let n = must_int64(path, v, "MaxI64 constraint requires int64")?;
                constrain_max_i64(path, n, *max)?;
            }
            Constraint::MinU64(min) => {
                let n = must_uint64(path, v, "MinU64 constraint requires uint64")?;
                constrain_min_u64(path, n, *min)?;
            }
            Constraint::MaxU64(max) => {
                let n = must_uint64(path, v, "MaxU64 constraint requires uint64")?;
                constrain_max_u64(path, n, *max)?;
            }
            Constraint::MinF64(min) => {
                let n = must_f64(path, v, "MinF64 constraint requires float64")?;
                constrain_min_f64(path, n, *min)?;
            }
            Constraint::MaxF64(max) => {
                let n = must_f64(path, v, "MaxF64 constraint requires float64")?;
                constrain_max_f64(path, n, *max)?;
            }
            Constraint::MinLength(min) => match v {
                RowValue::String(s) => constrain_min_byte_len(path, s.len(), *min, "string")?,
                RowValue::Bytes(b) => constrain_min_byte_len(path, b.len(), *min, "bytes")?,
                RowValue::List(items) => constrain_min_byte_len(path, items.len(), *min, "list")?,
                _ => return Err(err(path, "MinLength applies to string, bytes, or list")),
            },
            Constraint::MaxLength(max) => match v {
                RowValue::String(s) => constrain_max_byte_len(path, s.len(), *max, "string")?,
                RowValue::Bytes(b) => constrain_max_byte_len(path, b.len(), *max, "bytes")?,
                RowValue::List(items) => constrain_max_byte_len(path, items.len(), *max, "list")?,
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

#[cfg(test)]
mod constraint_helper_cover_tests {
    use super::*;
    use crate::error::DbError;

    #[test]
    fn constrain_helpers_accept_in_range_values() {
        let path = vec!["z".into()];
        constrain_min_i64(&path, 5, 1).unwrap();
        constrain_max_i64(&path, 1, 10).unwrap();
        constrain_min_u64(&path, 5, 1).unwrap();
        constrain_max_u64(&path, 1, 10).unwrap();
        constrain_min_f64(&path, 5.0, 1.0).unwrap();
        constrain_max_f64(&path, 1.0, 10.0).unwrap();
        constrain_min_byte_len(&path, "abcde".len(), 1, "string").unwrap();
        constrain_max_byte_len(&path, "ab".len(), 10, "string").unwrap();
        constrain_min_byte_len(&path, vec![1u8, 2, 3].len(), 2, "bytes").unwrap();
        constrain_max_byte_len(&path, vec![1u8].len(), 4, "bytes").unwrap();
    }

    #[test]
    fn constrain_max_numeric_helpers_surface_above_max_messages() {
        let path = vec!["x".into()];

        let e = constrain_max_i64(&path, 3, 1).unwrap_err();
        assert!(matches!(
            &e,
            DbError::Validation(v) if v.path == path && v.message.contains("above maximum"),
        ));

        let e = constrain_max_u64(&path, 5, 1).unwrap_err();
        assert!(matches!(
            &e,
            DbError::Validation(v) if v.message.contains("above maximum"),
        ));

        let e = constrain_max_f64(&path, 3.5, 1.25).unwrap_err();
        assert!(matches!(
            &e,
            DbError::Validation(v) if v.message.contains("above maximum"),
        ));
    }

    #[test]
    fn constrain_max_byte_len_string_and_bytes_surface_above_max() {
        let path = vec!["f".into()];

        let e = constrain_max_byte_len(&path, "ab".len(), 1, "string").unwrap_err();
        assert!(matches!(
            &e,
            DbError::Validation(v) if v.message.contains("above maximum"),
        ));

        let e = constrain_max_byte_len(&path, vec![1u8, 2].len(), 1, "bytes").unwrap_err();
        assert!(matches!(
            &e,
            DbError::Validation(v) if v.message.contains("above maximum"),
        ));
    }
}
