//! Typed scalar values for record payloads (v1).

use crate::error::{DbError, FormatError};
use crate::schema::Type;

#[derive(Debug, Clone, PartialEq)]
pub enum ScalarValue {
    Bool(bool),
    Int64(i64),
    Uint64(u64),
    Float64(f64),
    String(String),
    Bytes(Vec<u8>),
    Uuid([u8; 16]),
    /// Unix microseconds (same convention as elsewhere in Typra).
    Timestamp(i64),
}

impl ScalarValue {
    /// Canonical bytes for indexing (last insert wins per key).
    pub fn canonical_key_bytes(&self) -> Vec<u8> {
        match self {
            ScalarValue::Bool(b) => vec![0, if *b { 1 } else { 0 }],
            ScalarValue::Int64(v) => v.to_le_bytes().to_vec(),
            ScalarValue::Uint64(v) => v.to_le_bytes().to_vec(),
            ScalarValue::Float64(v) => v.to_le_bytes().to_vec(),
            ScalarValue::String(s) => s.as_bytes().to_vec(),
            ScalarValue::Bytes(b) => b.clone(),
            ScalarValue::Uuid(u) => u.to_vec(),
            ScalarValue::Timestamp(t) => t.to_le_bytes().to_vec(),
        }
    }

    pub fn ty_matches(&self, ty: &Type) -> bool {
        matches!(
            (self, ty),
            (ScalarValue::Bool(_), Type::Bool)
                | (ScalarValue::Int64(_), Type::Int64)
                | (ScalarValue::Uint64(_), Type::Uint64)
                | (ScalarValue::Float64(_), Type::Float64)
                | (ScalarValue::String(_), Type::String)
                | (ScalarValue::Bytes(_), Type::Bytes)
                | (ScalarValue::Uuid(_), Type::Uuid)
                | (ScalarValue::Timestamp(_), Type::Timestamp)
        )
    }
}

/// Encode a scalar with a leading type tag (must match `ty`).
pub fn encode_tagged_scalar(out: &mut Vec<u8>, v: &ScalarValue, ty: &Type) -> Result<(), DbError> {
    if !v.ty_matches(ty) {
        return Err(DbError::Format(FormatError::RecordPayloadTypeMismatch));
    }
    match (v, ty) {
        (ScalarValue::Bool(b), Type::Bool) => {
            out.push(0);
            out.push(if *b { 1 } else { 0 });
        }
        (ScalarValue::Int64(n), Type::Int64) => {
            out.push(1);
            out.extend_from_slice(&n.to_le_bytes());
        }
        (ScalarValue::Uint64(n), Type::Uint64) => {
            out.push(2);
            out.extend_from_slice(&n.to_le_bytes());
        }
        (ScalarValue::Float64(n), Type::Float64) => {
            out.push(3);
            out.extend_from_slice(&n.to_le_bytes());
        }
        (ScalarValue::String(s), Type::String) => {
            out.push(4);
            let b = s.as_bytes();
            out.extend_from_slice(&(b.len() as u32).to_le_bytes());
            out.extend_from_slice(b);
        }
        (ScalarValue::Bytes(b), Type::Bytes) => {
            out.push(5);
            out.extend_from_slice(&(b.len() as u32).to_le_bytes());
            out.extend_from_slice(b);
        }
        (ScalarValue::Uuid(u), Type::Uuid) => {
            out.push(6);
            out.extend_from_slice(u);
        }
        (ScalarValue::Timestamp(t), Type::Timestamp) => {
            out.push(7);
            out.extend_from_slice(&t.to_le_bytes());
        }
        _ => return Err(DbError::Format(FormatError::RecordPayloadTypeMismatch)),
    }
    Ok(())
}

pub struct Cursor<'a> {
    pub bytes: &'a [u8],
    pub pos: usize,
}

impl<'a> Cursor<'a> {
    pub fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    pub fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.pos)
    }

    pub fn take_u16(&mut self) -> Result<u16, DbError> {
        if self.remaining() < 2 {
            return Err(DbError::Format(FormatError::TruncatedRecordPayload));
        }
        let v = u16::from_le_bytes([self.bytes[self.pos], self.bytes[self.pos + 1]]);
        self.pos += 2;
        Ok(v)
    }

    pub fn take_u8(&mut self) -> Result<u8, DbError> {
        if self.pos >= self.bytes.len() {
            return Err(DbError::Format(FormatError::TruncatedRecordPayload));
        }
        let b = self.bytes[self.pos];
        self.pos += 1;
        Ok(b)
    }

    pub fn take_u32(&mut self) -> Result<u32, DbError> {
        if self.remaining() < 4 {
            return Err(DbError::Format(FormatError::TruncatedRecordPayload));
        }
        let v = u32::from_le_bytes([
            self.bytes[self.pos],
            self.bytes[self.pos + 1],
            self.bytes[self.pos + 2],
            self.bytes[self.pos + 3],
        ]);
        self.pos += 4;
        Ok(v)
    }

    pub fn take_i64(&mut self) -> Result<i64, DbError> {
        if self.remaining() < 8 {
            return Err(DbError::Format(FormatError::TruncatedRecordPayload));
        }
        let v = i64::from_le_bytes([
            self.bytes[self.pos],
            self.bytes[self.pos + 1],
            self.bytes[self.pos + 2],
            self.bytes[self.pos + 3],
            self.bytes[self.pos + 4],
            self.bytes[self.pos + 5],
            self.bytes[self.pos + 6],
            self.bytes[self.pos + 7],
        ]);
        self.pos += 8;
        Ok(v)
    }

    pub fn take_u64(&mut self) -> Result<u64, DbError> {
        if self.remaining() < 8 {
            return Err(DbError::Format(FormatError::TruncatedRecordPayload));
        }
        let v = u64::from_le_bytes([
            self.bytes[self.pos],
            self.bytes[self.pos + 1],
            self.bytes[self.pos + 2],
            self.bytes[self.pos + 3],
            self.bytes[self.pos + 4],
            self.bytes[self.pos + 5],
            self.bytes[self.pos + 6],
            self.bytes[self.pos + 7],
        ]);
        self.pos += 8;
        Ok(v)
    }

    pub fn take_f64(&mut self) -> Result<f64, DbError> {
        Ok(f64::from_bits(self.take_u64()?))
    }

    pub fn take_bytes(&mut self, n: usize) -> Result<Vec<u8>, DbError> {
        if self.remaining() < n {
            return Err(DbError::Format(FormatError::TruncatedRecordPayload));
        }
        let slice = &self.bytes[self.pos..self.pos + n];
        self.pos += n;
        Ok(slice.to_vec())
    }
}

pub fn decode_tagged_scalar(cur: &mut Cursor<'_>, ty: &Type) -> Result<ScalarValue, DbError> {
    let tag = cur.take_u8()?;
    Ok(match ty {
        Type::Bool => {
            if tag != 0 {
                return Err(DbError::Format(FormatError::RecordPayloadTypeMismatch));
            }
            let b = cur.take_u8()?;
            ScalarValue::Bool(b != 0)
        }
        Type::Int64 => {
            if tag != 1 {
                return Err(DbError::Format(FormatError::RecordPayloadTypeMismatch));
            }
            ScalarValue::Int64(cur.take_i64()?)
        }
        Type::Uint64 => {
            if tag != 2 {
                return Err(DbError::Format(FormatError::RecordPayloadTypeMismatch));
            }
            ScalarValue::Uint64(cur.take_u64()?)
        }
        Type::Float64 => {
            if tag != 3 {
                return Err(DbError::Format(FormatError::RecordPayloadTypeMismatch));
            }
            ScalarValue::Float64(cur.take_f64()?)
        }
        Type::String => {
            if tag != 4 {
                return Err(DbError::Format(FormatError::RecordPayloadTypeMismatch));
            }
            let n = cur.take_u32()? as usize;
            let b = cur.take_bytes(n)?;
            ScalarValue::String(
                String::from_utf8(b)
                    .map_err(|_| DbError::Format(FormatError::InvalidRecordUtf8))?,
            )
        }
        Type::Bytes => {
            if tag != 5 {
                return Err(DbError::Format(FormatError::RecordPayloadTypeMismatch));
            }
            let n = cur.take_u32()? as usize;
            ScalarValue::Bytes(cur.take_bytes(n)?)
        }
        Type::Uuid => {
            if tag != 6 {
                return Err(DbError::Format(FormatError::RecordPayloadTypeMismatch));
            }
            let b = cur.take_bytes(16)?;
            let mut a = [0u8; 16];
            a.copy_from_slice(&b);
            ScalarValue::Uuid(a)
        }
        Type::Timestamp => {
            if tag != 7 {
                return Err(DbError::Format(FormatError::RecordPayloadTypeMismatch));
            }
            ScalarValue::Timestamp(cur.take_i64()?)
        }
        _ => return Err(DbError::Format(FormatError::RecordPayloadUnsupportedType)),
    })
}
