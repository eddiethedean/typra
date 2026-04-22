//! Binary encoding for catalog payloads embedded in `SegmentType::Schema` segments.

use std::borrow::Cow;

use crate::error::{DbError, FormatError};
use crate::schema::{FieldDef, FieldPath, Type};

/// Maximum UTF-8 length for a collection name (exclusive upper bound is 1024 bytes).
pub const MAX_COLLECTION_NAME_BYTES: usize = 1023;

/// Legacy catalog payload (no primary key on create).
pub const CATALOG_PAYLOAD_VERSION_V1: u16 = 1;
/// Current catalog payload version (optional `primary_field` on create).
pub const CATALOG_PAYLOAD_VERSION: u16 = 2;

pub const ENTRY_KIND_CREATE_COLLECTION: u16 = 1;
pub const ENTRY_KIND_NEW_SCHEMA_VERSION: u16 = 2;

/// Maximum nesting depth for `Type` when encoding/decoding (prevents stack overflow on hostile input).
pub const MAX_TYPE_NESTING_DEPTH: u32 = 32;

#[derive(Debug, Clone, PartialEq)]
pub enum CatalogDecodeError {
    UnexpectedEof,
    UnknownCatalogPayloadVersion { got: u16 },
    UnknownEntryKind { got: u16 },
    TrailingBytes,
    TypeNestingTooDeep { max: u32 },
    InvalidUtf8,
    CollectionNameTooLong { got: usize },
    EmptyCollectionName,
    InvalidCreateSchemaVersion { got: u32 },
}

impl From<CatalogDecodeError> for DbError {
    fn from(e: CatalogDecodeError) -> Self {
        DbError::Format(FormatError::InvalidCatalogPayload {
            message: format!("{e:?}"),
        })
    }
}

/// Encode a catalog record as segment payload bytes (current [`CATALOG_PAYLOAD_VERSION`]).
pub fn encode_catalog_payload(record: &CatalogRecordWire) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&CATALOG_PAYLOAD_VERSION.to_le_bytes());
    match record {
        CatalogRecordWire::CreateCollection {
            collection_id,
            name,
            schema_version,
            fields,
            primary_field,
        } => {
            out.extend_from_slice(&ENTRY_KIND_CREATE_COLLECTION.to_le_bytes());
            out.extend_from_slice(&collection_id.to_le_bytes());
            encode_name(&mut out, name);
            out.extend_from_slice(&schema_version.to_le_bytes());
            encode_fields(&mut out, fields);
            encode_optional_primary_name(&mut out, primary_field.as_deref());
        }
        CatalogRecordWire::NewSchemaVersion {
            collection_id,
            schema_version,
            fields,
        } => {
            out.extend_from_slice(&ENTRY_KIND_NEW_SCHEMA_VERSION.to_le_bytes());
            out.extend_from_slice(&collection_id.to_le_bytes());
            out.extend_from_slice(&schema_version.to_le_bytes());
            encode_fields(&mut out, fields);
        }
    }
    out
}

/// Wire representation for encoding (mirrors on-disk entry kinds).
#[derive(Debug, Clone, PartialEq)]
pub enum CatalogRecordWire {
    CreateCollection {
        collection_id: u32,
        name: String,
        schema_version: u32,
        fields: Vec<FieldDef>,
        /// Top-level segment name for the primary key (`None` for legacy v1 catalog segments).
        primary_field: Option<String>,
    },
    NewSchemaVersion {
        collection_id: u32,
        schema_version: u32,
        fields: Vec<FieldDef>,
    },
}

pub fn decode_catalog_payload(bytes: &[u8]) -> Result<CatalogRecordWire, DbError> {
    let mut cur = Cursor::new(bytes);
    let ver = cur.take_u16()?;
    if ver != CATALOG_PAYLOAD_VERSION && ver != CATALOG_PAYLOAD_VERSION_V1 {
        return Err(CatalogDecodeError::UnknownCatalogPayloadVersion { got: ver }.into());
    }
    let kind = cur.take_u16()?;
    match kind {
        ENTRY_KIND_CREATE_COLLECTION => {
            let collection_id = cur.take_u32()?;
            let name = decode_name(&mut cur)?;
            let schema_version = cur.take_u32()?;
            let fields = decode_fields(&mut cur)?;
            let primary_field = if ver == CATALOG_PAYLOAD_VERSION {
                decode_optional_primary_name(&mut cur)?
            } else {
                None
            };
            if cur.remaining() != 0 {
                return Err(CatalogDecodeError::TrailingBytes.into());
            }
            Ok(CatalogRecordWire::CreateCollection {
                collection_id,
                name,
                schema_version,
                fields,
                primary_field,
            })
        }
        ENTRY_KIND_NEW_SCHEMA_VERSION => {
            let collection_id = cur.take_u32()?;
            let schema_version = cur.take_u32()?;
            let fields = decode_fields(&mut cur)?;
            if cur.remaining() != 0 {
                return Err(CatalogDecodeError::TrailingBytes.into());
            }
            Ok(CatalogRecordWire::NewSchemaVersion {
                collection_id,
                schema_version,
                fields,
            })
        }
        _ => Err(CatalogDecodeError::UnknownEntryKind { got: kind }.into()),
    }
}

fn encode_optional_primary_name(out: &mut Vec<u8>, primary: Option<&str>) {
    match primary {
        None => out.extend_from_slice(&0u32.to_le_bytes()),
        Some(s) => {
            let b = s.as_bytes();
            out.extend_from_slice(&(b.len() as u32).to_le_bytes());
            out.extend_from_slice(b);
        }
    }
}

fn decode_optional_primary_name(cur: &mut Cursor<'_>) -> Result<Option<String>, DbError> {
    let n = cur.take_u32()? as usize;
    if n == 0 {
        return Ok(None);
    }
    if n > MAX_COLLECTION_NAME_BYTES {
        return Err(CatalogDecodeError::CollectionNameTooLong { got: n }.into());
    }
    let bytes = cur.take_bytes(n)?;
    let s = String::from_utf8(bytes).map_err(|_| CatalogDecodeError::InvalidUtf8)?;
    if s.is_empty() {
        return Err(CatalogDecodeError::EmptyCollectionName.into());
    }
    Ok(Some(s))
}

fn encode_name(out: &mut Vec<u8>, name: &str) {
    let b = name.as_bytes();
    out.extend_from_slice(&(b.len() as u32).to_le_bytes());
    out.extend_from_slice(b);
}

fn decode_name(cur: &mut Cursor<'_>) -> Result<String, DbError> {
    let n = cur.take_u32()? as usize;
    if n == 0 {
        return Err(CatalogDecodeError::EmptyCollectionName.into());
    }
    if n > MAX_COLLECTION_NAME_BYTES {
        return Err(CatalogDecodeError::CollectionNameTooLong { got: n }.into());
    }
    let bytes = cur.take_bytes(n)?;
    String::from_utf8(bytes).map_err(|_| CatalogDecodeError::InvalidUtf8.into())
}

fn encode_fields(out: &mut Vec<u8>, fields: &[FieldDef]) {
    out.extend_from_slice(&(fields.len() as u32).to_le_bytes());
    for f in fields {
        encode_field_path(out, &f.path);
        encode_type(out, &f.ty, 0);
    }
}

fn decode_fields(cur: &mut Cursor<'_>) -> Result<Vec<FieldDef>, DbError> {
    let n = cur.take_u32()? as usize;
    let mut v = Vec::with_capacity(n.min(1024));
    for _ in 0..n {
        let path = decode_field_path(cur)?;
        let ty = decode_type(cur, 0)?;
        v.push(FieldDef { path, ty });
    }
    Ok(v)
}

fn encode_field_path(out: &mut Vec<u8>, path: &FieldPath) {
    let parts = &path.0;
    out.extend_from_slice(&(parts.len() as u32).to_le_bytes());
    for p in parts {
        let b = p.as_bytes();
        out.extend_from_slice(&(b.len() as u32).to_le_bytes());
        out.extend_from_slice(b);
    }
}

fn decode_field_path(cur: &mut Cursor<'_>) -> Result<FieldPath, DbError> {
    let n = cur.take_u32()? as usize;
    if n == 0 {
        return Err(DbError::Schema(crate::error::SchemaError::InvalidFieldPath));
    }
    let mut parts = Vec::with_capacity(n.min(64));
    for _ in 0..n {
        let len = cur.take_u32()? as usize;
        let bytes = cur.take_bytes(len)?;
        let s = String::from_utf8(bytes).map_err(|_| CatalogDecodeError::InvalidUtf8)?;
        if s.is_empty() {
            return Err(DbError::Schema(crate::error::SchemaError::InvalidFieldPath));
        }
        parts.push(Cow::Owned(s));
    }
    Ok(FieldPath(parts))
}

const TAG_BOOL: u8 = 0;
const TAG_INT64: u8 = 1;
const TAG_UINT64: u8 = 2;
const TAG_FLOAT64: u8 = 3;
const TAG_STRING: u8 = 4;
const TAG_BYTES: u8 = 5;
const TAG_UUID: u8 = 6;
const TAG_TIMESTAMP: u8 = 7;
const TAG_OPTIONAL: u8 = 8;
const TAG_LIST: u8 = 9;
const TAG_OBJECT: u8 = 10;
const TAG_ENUM: u8 = 11;

// `depth` is only read when recursing into nested types; clippy does not see cross-call use.
#[allow(clippy::only_used_in_recursion)]
fn encode_type(out: &mut Vec<u8>, ty: &Type, depth: u32) {
    match ty {
        Type::Bool => out.push(TAG_BOOL),
        Type::Int64 => out.push(TAG_INT64),
        Type::Uint64 => out.push(TAG_UINT64),
        Type::Float64 => out.push(TAG_FLOAT64),
        Type::String => out.push(TAG_STRING),
        Type::Bytes => out.push(TAG_BYTES),
        Type::Uuid => out.push(TAG_UUID),
        Type::Timestamp => out.push(TAG_TIMESTAMP),
        Type::Optional(inner) => {
            out.push(TAG_OPTIONAL);
            encode_type(out, inner, depth + 1);
        }
        Type::List(inner) => {
            out.push(TAG_LIST);
            encode_type(out, inner, depth + 1);
        }
        Type::Object(fields) => {
            out.push(TAG_OBJECT);
            out.extend_from_slice(&(fields.len() as u32).to_le_bytes());
            for f in fields {
                encode_field_path(out, &f.path);
                encode_type(out, &f.ty, depth + 1);
            }
        }
        Type::Enum(variants) => {
            out.push(TAG_ENUM);
            out.extend_from_slice(&(variants.len() as u32).to_le_bytes());
            for s in variants {
                let b = s.as_bytes();
                out.extend_from_slice(&(b.len() as u32).to_le_bytes());
                out.extend_from_slice(b);
            }
        }
    }
}

fn decode_type(cur: &mut Cursor<'_>, depth: u32) -> Result<Type, DbError> {
    if depth > MAX_TYPE_NESTING_DEPTH {
        return Err(CatalogDecodeError::TypeNestingTooDeep {
            max: MAX_TYPE_NESTING_DEPTH,
        }
        .into());
    }
    let tag = cur.take_u8()?;
    Ok(match tag {
        TAG_BOOL => Type::Bool,
        TAG_INT64 => Type::Int64,
        TAG_UINT64 => Type::Uint64,
        TAG_FLOAT64 => Type::Float64,
        TAG_STRING => Type::String,
        TAG_BYTES => Type::Bytes,
        TAG_UUID => Type::Uuid,
        TAG_TIMESTAMP => Type::Timestamp,
        TAG_OPTIONAL => Type::Optional(Box::new(decode_type(cur, depth + 1)?)),
        TAG_LIST => Type::List(Box::new(decode_type(cur, depth + 1)?)),
        TAG_OBJECT => {
            let n = cur.take_u32()? as usize;
            let mut fields = Vec::with_capacity(n.min(1024));
            for _ in 0..n {
                let path = decode_field_path(cur)?;
                let ty = decode_type(cur, depth + 1)?;
                fields.push(FieldDef { path, ty });
            }
            Type::Object(fields)
        }
        TAG_ENUM => {
            let n = cur.take_u32()? as usize;
            let mut variants = Vec::with_capacity(n.min(1024));
            for _ in 0..n {
                let len = cur.take_u32()? as usize;
                let bytes = cur.take_bytes(len)?;
                let s = String::from_utf8(bytes).map_err(|_| CatalogDecodeError::InvalidUtf8)?;
                variants.push(s);
            }
            Type::Enum(variants)
        }
        _ => {
            return Err(DbError::Format(FormatError::InvalidCatalogPayload {
                message: format!("unknown type tag {tag}"),
            }))
        }
    })
}

struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.pos)
    }

    fn take_u8(&mut self) -> Result<u8, DbError> {
        if self.pos >= self.bytes.len() {
            return Err(CatalogDecodeError::UnexpectedEof.into());
        }
        let b = self.bytes[self.pos];
        self.pos += 1;
        Ok(b)
    }

    fn take_u16(&mut self) -> Result<u16, DbError> {
        if self.remaining() < 2 {
            return Err(CatalogDecodeError::UnexpectedEof.into());
        }
        let v = u16::from_le_bytes([self.bytes[self.pos], self.bytes[self.pos + 1]]);
        self.pos += 2;
        Ok(v)
    }

    fn take_u32(&mut self) -> Result<u32, DbError> {
        if self.remaining() < 4 {
            return Err(CatalogDecodeError::UnexpectedEof.into());
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

    fn take_bytes(&mut self, n: usize) -> Result<Vec<u8>, DbError> {
        if self.remaining() < n {
            return Err(CatalogDecodeError::UnexpectedEof.into());
        }
        let slice = &self.bytes[self.pos..self.pos + n];
        self.pos += n;
        Ok(slice.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::FieldPath;

    fn path(parts: &[&str]) -> FieldPath {
        FieldPath(parts.iter().map(|s| Cow::Owned(s.to_string())).collect())
    }

    #[test]
    fn roundtrip_create_collection() {
        let rec = CatalogRecordWire::CreateCollection {
            collection_id: 1,
            name: "books".to_string(),
            schema_version: 1,
            fields: vec![FieldDef {
                path: path(&["title"]),
                ty: Type::String,
            }],
            primary_field: Some("title".to_string()),
        };
        let bytes = encode_catalog_payload(&rec);
        let got = decode_catalog_payload(&bytes).unwrap();
        assert_eq!(got, rec);
    }

    #[test]
    fn roundtrip_new_version() {
        let rec = CatalogRecordWire::NewSchemaVersion {
            collection_id: 1,
            schema_version: 2,
            fields: vec![],
        };
        let bytes = encode_catalog_payload(&rec);
        let got = decode_catalog_payload(&bytes).unwrap();
        assert_eq!(got, rec);
    }

    #[test]
    fn nested_type_depth_limit() {
        let mut t = Type::Bool;
        for _ in 0..40 {
            t = Type::Optional(Box::new(t));
        }
        let mut out = Vec::new();
        encode_type(&mut out, &t, 0);
        let mut cur = Cursor::new(&out);
        let err = decode_type(&mut cur, 0);
        assert!(err.is_err());
    }
}
