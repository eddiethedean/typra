//! Row values for record payload v2: primitives, optionals, lists, objects, enums (as strings).

use std::collections::BTreeMap;

use crate::error::{DbError, FormatError};
use crate::record::scalar::{decode_tagged_scalar, encode_tagged_scalar, Cursor, ScalarValue};
use crate::schema::{FieldDef, Type};

/// In-memory value for a row field (including nested structures).
#[derive(Debug, Clone, PartialEq)]
pub enum RowValue {
    Bool(bool),
    Int64(i64),
    Uint64(u64),
    Float64(f64),
    String(String),
    Bytes(Vec<u8>),
    Uuid([u8; 16]),
    Timestamp(i64),
    /// Absent `Optional<T>` (omitted key or explicit null).
    None,
    List(Vec<RowValue>),
    Object(BTreeMap<String, RowValue>),
}

impl RowValue {
    /// Convert a primitive [`ScalarValue`] to a row value (for PK and v1 interop).
    pub fn from_scalar(s: ScalarValue) -> Self {
        match s {
            ScalarValue::Bool(b) => RowValue::Bool(b),
            ScalarValue::Int64(n) => RowValue::Int64(n),
            ScalarValue::Uint64(n) => RowValue::Uint64(n),
            ScalarValue::Float64(n) => RowValue::Float64(n),
            ScalarValue::String(x) => RowValue::String(x),
            ScalarValue::Bytes(b) => RowValue::Bytes(b),
            ScalarValue::Uuid(u) => RowValue::Uuid(u),
            ScalarValue::Timestamp(t) => RowValue::Timestamp(t),
        }
    }

    /// If this row value is a primitive, return its scalar form (for PK encoding).
    pub fn as_scalar(&self) -> Option<ScalarValue> {
        Some(match self {
            RowValue::Bool(b) => ScalarValue::Bool(*b),
            RowValue::Int64(n) => ScalarValue::Int64(*n),
            RowValue::Uint64(n) => ScalarValue::Uint64(*n),
            RowValue::Float64(n) => ScalarValue::Float64(*n),
            RowValue::String(s) => ScalarValue::String(s.clone()),
            RowValue::Bytes(b) => ScalarValue::Bytes(b.clone()),
            RowValue::Uuid(u) => ScalarValue::Uuid(*u),
            RowValue::Timestamp(t) => ScalarValue::Timestamp(*t),
            _ => return None,
        })
    }

    /// Require a primitive scalar (for `get` / PK lookup parameters).
    pub fn into_scalar(self) -> Result<ScalarValue, DbError> {
        self.as_scalar()
            .ok_or(DbError::Format(FormatError::RecordPayloadTypeMismatch))
    }
}

/// Encode a row value according to `ty` (record payload v2).
pub fn encode_row_value(out: &mut Vec<u8>, v: &RowValue, ty: &Type) -> Result<(), DbError> {
    match ty {
        Type::Bool
        | Type::Int64
        | Type::Uint64
        | Type::Float64
        | Type::String
        | Type::Bytes
        | Type::Uuid
        | Type::Timestamp => {
            let s = v
                .as_scalar()
                .ok_or(DbError::Format(FormatError::RecordPayloadTypeMismatch))?;
            encode_tagged_scalar(out, &s, ty)
        }
        Type::Optional(inner) => {
            if matches!(v, RowValue::None) {
                out.push(0);
                Ok(())
            } else {
                out.push(1);
                encode_row_value(out, v, inner)
            }
        }
        Type::List(inner) => {
            let RowValue::List(items) = v else {
                return Err(DbError::Format(FormatError::RecordPayloadTypeMismatch));
            };
            out.extend_from_slice(&(items.len() as u32).to_le_bytes());
            for item in items {
                encode_row_value(out, item, inner)?;
            }
            Ok(())
        }
        Type::Object(fields) => {
            let RowValue::Object(map) = v else {
                return Err(DbError::Format(FormatError::RecordPayloadTypeMismatch));
            };
            for def in fields {
                let key = def.path.0[0].as_ref();
                let fv = map
                    .get(key)
                    .ok_or(DbError::Format(FormatError::TruncatedRecordPayload))?;
                encode_row_value(out, fv, &def.ty)?;
            }
            Ok(())
        }
        Type::Enum(_) => {
            let RowValue::String(s) = v else {
                return Err(DbError::Format(FormatError::RecordPayloadTypeMismatch));
            };
            encode_tagged_scalar(out, &ScalarValue::String(s.clone()), &Type::String)
        }
    }
}

/// Decode a row value according to `ty` (record payload v2).
pub fn decode_row_value(cur: &mut Cursor<'_>, ty: &Type) -> Result<RowValue, DbError> {
    Ok(match ty {
        Type::Bool
        | Type::Int64
        | Type::Uint64
        | Type::Float64
        | Type::String
        | Type::Bytes
        | Type::Uuid
        | Type::Timestamp => RowValue::from_scalar(decode_tagged_scalar(cur, ty)?),
        Type::Optional(inner) => {
            let pres = cur.take_u8()?;
            match pres {
                0 => RowValue::None,
                1 => decode_row_value(cur, inner)?,
                _ => return Err(DbError::Format(FormatError::RecordPayloadTypeMismatch)),
            }
        }
        Type::List(inner) => {
            let n = cur.take_u32()? as usize;
            let mut items = Vec::with_capacity(n.min(1_048_576));
            for _ in 0..n {
                items.push(decode_row_value(cur, inner)?);
            }
            RowValue::List(items)
        }
        Type::Object(fields) => {
            let mut map = BTreeMap::new();
            for def in fields {
                let key = def.path.0[0].to_string();
                let val = decode_row_value(cur, &def.ty)?;
                map.insert(key, val);
            }
            RowValue::Object(map)
        }
        Type::Enum(_) => {
            let s = match decode_tagged_scalar(cur, &Type::String)? {
                ScalarValue::String(s) => s,
                _ => return Err(DbError::Format(FormatError::RecordPayloadTypeMismatch)),
            };
            RowValue::String(s)
        }
    })
}

/// Ordered non-PK field definitions (schema order, excluding primary key column).
pub fn non_pk_defs_in_order<'a>(fields: &'a [FieldDef], pk_name: &str) -> Vec<&'a FieldDef> {
    fields
        .iter()
        .filter(|f| f.path.0.len() == 1 && f.path.0[0] != pk_name)
        .collect()
}
