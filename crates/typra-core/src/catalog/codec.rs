//! Binary encoding for catalog payloads embedded in `SegmentType::Schema` segments.

use std::borrow::Cow;

use crate::error::{DbError, FormatError};
use crate::schema::{Constraint, FieldDef, FieldPath, IndexDef, IndexKind, Type};

/// Maximum UTF-8 length for a collection name (exclusive upper bound is 1024 bytes).
pub const MAX_COLLECTION_NAME_BYTES: usize = 1023;

/// Legacy catalog payload (no primary key on create).
pub const CATALOG_PAYLOAD_VERSION_V1: u16 = 1;
/// Catalog with optional `primary_field` on create, no per-field constraints.
pub const CATALOG_PAYLOAD_VERSION_V2: u16 = 2;
/// Current catalog write version: `primary_field` + [`FieldDef::constraints`].
pub const CATALOG_PAYLOAD_VERSION_V3: u16 = 3;
/// Catalog with `indexes` definitions (secondary indexes).
pub const CATALOG_PAYLOAD_VERSION_V4: u16 = 4;
/// What [`encode_catalog_payload`] writes (latest).
pub const CATALOG_PAYLOAD_VERSION: u16 = CATALOG_PAYLOAD_VERSION_V4;

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
    IndexNameTooLong { got: usize },
    EmptyIndexName,
    UnknownIndexKind { got: u8 },
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
            indexes,
            primary_field,
        } => {
            out.extend_from_slice(&ENTRY_KIND_CREATE_COLLECTION.to_le_bytes());
            out.extend_from_slice(&collection_id.to_le_bytes());
            encode_name(&mut out, name);
            out.extend_from_slice(&schema_version.to_le_bytes());
            encode_fields_v3(&mut out, fields);
            encode_indexes(&mut out, indexes);
            encode_optional_primary_name(&mut out, primary_field.as_deref());
        }
        CatalogRecordWire::NewSchemaVersion {
            collection_id,
            schema_version,
            fields,
            indexes,
        } => {
            out.extend_from_slice(&ENTRY_KIND_NEW_SCHEMA_VERSION.to_le_bytes());
            out.extend_from_slice(&collection_id.to_le_bytes());
            out.extend_from_slice(&schema_version.to_le_bytes());
            encode_fields_v3(&mut out, fields);
            encode_indexes(&mut out, indexes);
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
        indexes: Vec<IndexDef>,
        /// Top-level segment name for the primary key (`None` for legacy v1 catalog segments).
        primary_field: Option<String>,
    },
    NewSchemaVersion {
        collection_id: u32,
        schema_version: u32,
        fields: Vec<FieldDef>,
        indexes: Vec<IndexDef>,
    },
}

pub fn decode_catalog_payload(bytes: &[u8]) -> Result<CatalogRecordWire, DbError> {
    let mut cur = Cursor::new(bytes);
    let ver = cur.take_u16()?;
    match ver {
        CATALOG_PAYLOAD_VERSION_V1
        | CATALOG_PAYLOAD_VERSION_V2
        | CATALOG_PAYLOAD_VERSION_V3
        | CATALOG_PAYLOAD_VERSION_V4 => {}
        _ => return Err(CatalogDecodeError::UnknownCatalogPayloadVersion { got: ver }.into()),
    }
    let kind = cur.take_u16()?;
    match kind {
        ENTRY_KIND_CREATE_COLLECTION => {
            let collection_id = cur.take_u32()?;
            let name = decode_name(&mut cur)?;
            let schema_version = cur.take_u32()?;
            let fields = decode_fields(&mut cur, ver)?;
            let indexes = match ver {
                CATALOG_PAYLOAD_VERSION_V4 => decode_indexes(&mut cur)?,
                _ => Vec::new(),
            };
            let primary_field = match ver {
                CATALOG_PAYLOAD_VERSION_V2 | CATALOG_PAYLOAD_VERSION_V3 | CATALOG_PAYLOAD_VERSION_V4 => {
                    decode_optional_primary_name(&mut cur)?
                }
                _ => None,
            };
            if cur.remaining() != 0 {
                return Err(CatalogDecodeError::TrailingBytes.into());
            }
            Ok(CatalogRecordWire::CreateCollection {
                collection_id,
                name,
                schema_version,
                fields,
                indexes,
                primary_field,
            })
        }
        ENTRY_KIND_NEW_SCHEMA_VERSION => {
            let collection_id = cur.take_u32()?;
            let schema_version = cur.take_u32()?;
            let fields = decode_fields(&mut cur, ver)?;
            let indexes = match ver {
                CATALOG_PAYLOAD_VERSION_V4 => decode_indexes(&mut cur)?,
                _ => Vec::new(),
            };
            if cur.remaining() != 0 {
                return Err(CatalogDecodeError::TrailingBytes.into());
            }
            Ok(CatalogRecordWire::NewSchemaVersion {
                collection_id,
                schema_version,
                fields,
                indexes,
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
    debug_assert!(!s.is_empty());
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

fn encode_indexes(out: &mut Vec<u8>, indexes: &[IndexDef]) {
    out.extend_from_slice(&(indexes.len() as u32).to_le_bytes());
    for idx in indexes {
        match idx.kind {
            IndexKind::Unique => out.push(1),
            IndexKind::NonUnique => out.push(2),
        }
        encode_field_path(out, &idx.path);
        let b = idx.name.as_bytes();
        out.extend_from_slice(&(b.len() as u32).to_le_bytes());
        out.extend_from_slice(b);
    }
}

fn decode_indexes(cur: &mut Cursor<'_>) -> Result<Vec<IndexDef>, DbError> {
    let n = cur.take_u32()? as usize;
    let mut v = Vec::with_capacity(n.min(1024));
    for _ in 0..n {
        let kind_tag = cur.take_u8()?;
        let kind = match kind_tag {
            1 => IndexKind::Unique,
            2 => IndexKind::NonUnique,
            _ => return Err(CatalogDecodeError::UnknownIndexKind { got: kind_tag }.into()),
        };
        let path = decode_field_path(cur)?;
        let name_len = cur.take_u32()? as usize;
        if name_len == 0 {
            return Err(CatalogDecodeError::EmptyIndexName.into());
        }
        if name_len > MAX_COLLECTION_NAME_BYTES {
            return Err(CatalogDecodeError::IndexNameTooLong { got: name_len }.into());
        }
        let bytes = cur.take_bytes(name_len)?;
        let name = String::from_utf8(bytes).map_err(|_| CatalogDecodeError::InvalidUtf8)?;
        debug_assert!(!name.is_empty());
        v.push(IndexDef { name, path, kind });
    }
    Ok(v)
}

fn encode_fields_v3(out: &mut Vec<u8>, fields: &[FieldDef]) {
    out.extend_from_slice(&(fields.len() as u32).to_le_bytes());
    for f in fields {
        encode_field_path(out, &f.path);
        encode_type(out, &f.ty, 0);
        encode_constraints(out, &f.constraints);
    }
}

fn decode_fields(cur: &mut Cursor<'_>, catalog_ver: u16) -> Result<Vec<FieldDef>, DbError> {
    let n = cur.take_u32()? as usize;
    let mut v = Vec::with_capacity(n.min(1024));
    for _ in 0..n {
        let path = decode_field_path(cur)?;
        let ty = decode_type(cur, 0)?;
        let constraints = match catalog_ver {
            CATALOG_PAYLOAD_VERSION_V1 | CATALOG_PAYLOAD_VERSION_V2 => Vec::new(),
            _ => decode_constraints(cur)?,
        };
        v.push(FieldDef {
            path,
            ty,
            constraints,
        });
    }
    Ok(v)
}

const CT_MIN_I64: u8 = 1;
const CT_MAX_I64: u8 = 2;
const CT_MIN_U64: u8 = 3;
const CT_MAX_U64: u8 = 4;
const CT_MIN_F64: u8 = 5;
const CT_MAX_F64: u8 = 6;
const CT_MIN_LEN: u8 = 7;
const CT_MAX_LEN: u8 = 8;
const CT_REGEX: u8 = 9;
const CT_EMAIL: u8 = 10;
const CT_URL: u8 = 11;
const CT_NONEMPTY: u8 = 12;

fn encode_constraints(out: &mut Vec<u8>, c: &[Constraint]) {
    out.extend_from_slice(&(c.len() as u32).to_le_bytes());
    for x in c {
        match x {
            Constraint::MinI64(n) => {
                out.push(CT_MIN_I64);
                out.extend_from_slice(&n.to_le_bytes());
            }
            Constraint::MaxI64(n) => {
                out.push(CT_MAX_I64);
                out.extend_from_slice(&n.to_le_bytes());
            }
            Constraint::MinU64(n) => {
                out.push(CT_MIN_U64);
                out.extend_from_slice(&n.to_le_bytes());
            }
            Constraint::MaxU64(n) => {
                out.push(CT_MAX_U64);
                out.extend_from_slice(&n.to_le_bytes());
            }
            Constraint::MinF64(n) => {
                out.push(CT_MIN_F64);
                out.extend_from_slice(&n.to_le_bytes());
            }
            Constraint::MaxF64(n) => {
                out.push(CT_MAX_F64);
                out.extend_from_slice(&n.to_le_bytes());
            }
            Constraint::MinLength(n) => {
                out.push(CT_MIN_LEN);
                out.extend_from_slice(&n.to_le_bytes());
            }
            Constraint::MaxLength(n) => {
                out.push(CT_MAX_LEN);
                out.extend_from_slice(&n.to_le_bytes());
            }
            Constraint::Regex(s) => {
                out.push(CT_REGEX);
                let b = s.as_bytes();
                out.extend_from_slice(&(b.len() as u32).to_le_bytes());
                out.extend_from_slice(b);
            }
            Constraint::Email => out.push(CT_EMAIL),
            Constraint::Url => out.push(CT_URL),
            Constraint::NonEmpty => out.push(CT_NONEMPTY),
        }
    }
}

fn decode_constraints(cur: &mut Cursor<'_>) -> Result<Vec<Constraint>, DbError> {
    let n = cur.take_u32()? as usize;
    let mut v = Vec::with_capacity(n.min(4096));
    for _ in 0..n {
        let tag = cur.take_u8()?;
        let c = match tag {
            CT_MIN_I64 => Constraint::MinI64(cur.take_i64()?),
            CT_MAX_I64 => Constraint::MaxI64(cur.take_i64()?),
            CT_MIN_U64 => Constraint::MinU64(cur.take_u64()?),
            CT_MAX_U64 => Constraint::MaxU64(cur.take_u64()?),
            CT_MIN_F64 => Constraint::MinF64(f64::from_bits(cur.take_u64()?)),
            CT_MAX_F64 => Constraint::MaxF64(f64::from_bits(cur.take_u64()?)),
            CT_MIN_LEN => Constraint::MinLength(cur.take_u64()?),
            CT_MAX_LEN => Constraint::MaxLength(cur.take_u64()?),
            CT_REGEX => {
                let len = cur.take_u32()? as usize;
                let bytes = cur.take_bytes(len)?;
                Constraint::Regex(
                    String::from_utf8(bytes).map_err(|_| CatalogDecodeError::InvalidUtf8)?,
                )
            }
            CT_EMAIL => Constraint::Email,
            CT_URL => Constraint::Url,
            CT_NONEMPTY => Constraint::NonEmpty,
            _ => {
                return Err(DbError::Format(FormatError::InvalidCatalogPayload {
                    message: format!("unknown constraint tag {tag}"),
                }))
            }
        };
        v.push(c);
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
                fields.push(FieldDef {
                    path,
                    ty,
                    constraints: Vec::new(),
                });
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

    fn take_u64(&mut self) -> Result<u64, DbError> {
        if self.remaining() < 8 {
            return Err(CatalogDecodeError::UnexpectedEof.into());
        }
        let mut b = [0u8; 8];
        b.copy_from_slice(&self.bytes[self.pos..self.pos + 8]);
        self.pos += 8;
        Ok(u64::from_le_bytes(b))
    }

    fn take_i64(&mut self) -> Result<i64, DbError> {
        Ok(self.take_u64()? as i64)
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

    fn encode_field_path_test(out: &mut Vec<u8>, parts: &[&str]) {
        out.extend_from_slice(&(parts.len() as u32).to_le_bytes());
        for p in parts {
            let b = p.as_bytes();
            out.extend_from_slice(&(b.len() as u32).to_le_bytes());
            out.extend_from_slice(b);
        }
    }

    fn encode_fields_legacy(out: &mut Vec<u8>, fields: &[(&[&str], Type)]) {
        out.extend_from_slice(&(fields.len() as u32).to_le_bytes());
        for (p, ty) in fields {
            encode_field_path_test(out, p);
            encode_type(out, ty, 0);
        }
    }

    #[test]
    fn decode_accepts_all_supported_catalog_versions() {
        // v1: create collection, no indexes, no primary field, no constraints.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&CATALOG_PAYLOAD_VERSION_V1.to_le_bytes());
        bytes.extend_from_slice(&ENTRY_KIND_CREATE_COLLECTION.to_le_bytes());
        bytes.extend_from_slice(&1u32.to_le_bytes());
        encode_name(&mut bytes, "t");
        bytes.extend_from_slice(&1u32.to_le_bytes());
        encode_fields_legacy(&mut bytes, &[(&["id"], Type::Int64)]);
        let got = decode_catalog_payload(&bytes).unwrap();
        match got {
            CatalogRecordWire::CreateCollection { name, primary_field, indexes, .. } => {
                assert_eq!(name, "t");
                assert_eq!(primary_field, None);
                assert!(indexes.is_empty());
            }
            _ => panic!("expected CreateCollection"),
        }

        // v2: create collection, includes primary field name, no constraints, no indexes.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&CATALOG_PAYLOAD_VERSION_V2.to_le_bytes());
        bytes.extend_from_slice(&ENTRY_KIND_CREATE_COLLECTION.to_le_bytes());
        bytes.extend_from_slice(&2u32.to_le_bytes());
        encode_name(&mut bytes, "t2");
        bytes.extend_from_slice(&1u32.to_le_bytes());
        encode_fields_legacy(&mut bytes, &[(&["id"], Type::Int64)]);
        encode_optional_primary_name(&mut bytes, Some("id"));
        let got = decode_catalog_payload(&bytes).unwrap();
        match got {
            CatalogRecordWire::CreateCollection { name, primary_field, .. } => {
                assert_eq!(name, "t2");
                assert_eq!(primary_field, Some("id".to_string()));
            }
            _ => panic!("expected CreateCollection"),
        }

        // v3: new schema version, includes constraints.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&CATALOG_PAYLOAD_VERSION_V3.to_le_bytes());
        bytes.extend_from_slice(&ENTRY_KIND_NEW_SCHEMA_VERSION.to_le_bytes());
        bytes.extend_from_slice(&3u32.to_le_bytes());
        bytes.extend_from_slice(&2u32.to_le_bytes());
        encode_fields_v3(
            &mut bytes,
            &[FieldDef {
                path: path(&["id"]),
                ty: Type::Int64,
                constraints: vec![Constraint::MinI64(1)],
            }],
        );
        let got = decode_catalog_payload(&bytes).unwrap();
        assert!(matches!(got, CatalogRecordWire::NewSchemaVersion { .. }));

        // v4: create collection, includes indexes and primary.
        let rec = CatalogRecordWire::CreateCollection {
            collection_id: 4,
            name: "t4".to_string(),
            schema_version: 1,
            fields: vec![FieldDef {
                path: path(&["id"]),
                ty: Type::Int64,
                constraints: Vec::new(),
            }],
            indexes: vec![IndexDef {
                name: "i".to_string(),
                path: path(&["id"]),
                kind: IndexKind::NonUnique,
            }],
            primary_field: Some("id".to_string()),
        };
        let bytes = encode_catalog_payload(&rec);
        assert_eq!(decode_catalog_payload(&bytes).unwrap(), rec);
    }

    #[test]
    fn decode_v4_new_schema_version_decodes_indexes() {
        // Hit the v4-only indexes path for ENTRY_KIND_NEW_SCHEMA_VERSION.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&CATALOG_PAYLOAD_VERSION_V4.to_le_bytes());
        bytes.extend_from_slice(&ENTRY_KIND_NEW_SCHEMA_VERSION.to_le_bytes());
        bytes.extend_from_slice(&1u32.to_le_bytes()); // collection_id
        bytes.extend_from_slice(&2u32.to_le_bytes()); // schema_version
        encode_fields_v3(
            &mut bytes,
            &[FieldDef {
                path: path(&["id"]),
                ty: Type::Int64,
                constraints: vec![],
            }],
        );
        encode_indexes(
            &mut bytes,
            &[IndexDef {
                name: "i".to_string(),
                path: path(&["id"]),
                kind: IndexKind::Unique,
            }],
        );
        let got = decode_catalog_payload(&bytes).unwrap();
        match got {
            CatalogRecordWire::NewSchemaVersion { indexes, .. } => {
                assert_eq!(indexes.len(), 1);
                assert_eq!(indexes[0].name, "i");
            }
            _ => panic!("expected NewSchemaVersion"),
        }
    }

    #[test]
    fn decode_indexes_errors_on_unexpected_eof_before_kind_tag() {
        // v4 create collection with indexes count=1 but no kind tag byte.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&CATALOG_PAYLOAD_VERSION_V4.to_le_bytes());
        bytes.extend_from_slice(&ENTRY_KIND_CREATE_COLLECTION.to_le_bytes());
        bytes.extend_from_slice(&1u32.to_le_bytes());
        encode_name(&mut bytes, "t");
        bytes.extend_from_slice(&1u32.to_le_bytes());
        encode_fields_v3(
            &mut bytes,
            &[FieldDef {
                path: path(&["id"]),
                ty: Type::Int64,
                constraints: Vec::new(),
            }],
        );
        bytes.extend_from_slice(&1u32.to_le_bytes()); // indexes count
        // truncated here: kind_tag should be next.
        assert!(decode_catalog_payload(&bytes).is_err());
    }

    #[test]
    fn decode_constraints_errors_on_unexpected_eof_mid_u64() {
        // v3 new schema version with constraints count=1 but missing the u64 payload.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&CATALOG_PAYLOAD_VERSION_V3.to_le_bytes());
        bytes.extend_from_slice(&ENTRY_KIND_NEW_SCHEMA_VERSION.to_le_bytes());
        bytes.extend_from_slice(&1u32.to_le_bytes()); // collection_id
        bytes.extend_from_slice(&2u32.to_le_bytes()); // schema_version
        bytes.extend_from_slice(&1u32.to_le_bytes()); // fields count
        encode_field_path_test(&mut bytes, &["id"]);
        encode_type(&mut bytes, &Type::Uint64, 0);
        bytes.extend_from_slice(&1u32.to_le_bytes()); // constraints count
        bytes.push(CT_MIN_U64); // tag, but no u64 follows
        assert!(decode_catalog_payload(&bytes).is_err());
    }

    #[test]
    fn decode_indexes_rejects_empty_index_name_len() {
        // v4 create collection with one index whose name_len is 0.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&CATALOG_PAYLOAD_VERSION_V4.to_le_bytes());
        bytes.extend_from_slice(&ENTRY_KIND_CREATE_COLLECTION.to_le_bytes());
        bytes.extend_from_slice(&1u32.to_le_bytes());
        encode_name(&mut bytes, "t");
        bytes.extend_from_slice(&1u32.to_le_bytes());
        encode_fields_v3(
            &mut bytes,
            &[FieldDef {
                path: path(&["id"]),
                ty: Type::Int64,
                constraints: Vec::new(),
            }],
        );
        bytes.extend_from_slice(&1u32.to_le_bytes()); // indexes count
        bytes.push(1); // kind_tag = Unique
        encode_field_path_test(&mut bytes, &["id"]);
        bytes.extend_from_slice(&0u32.to_le_bytes()); // name_len == 0
        assert!(decode_catalog_payload(&bytes).is_err());
    }

    #[test]
    fn decode_indexes_rejects_too_long_index_name_len() {
        // v4 create collection with one index whose name_len exceeds MAX_COLLECTION_NAME_BYTES.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&CATALOG_PAYLOAD_VERSION_V4.to_le_bytes());
        bytes.extend_from_slice(&ENTRY_KIND_CREATE_COLLECTION.to_le_bytes());
        bytes.extend_from_slice(&1u32.to_le_bytes());
        encode_name(&mut bytes, "t");
        bytes.extend_from_slice(&1u32.to_le_bytes());
        encode_fields_v3(
            &mut bytes,
            &[FieldDef {
                path: path(&["id"]),
                ty: Type::Int64,
                constraints: Vec::new(),
            }],
        );
        bytes.extend_from_slice(&1u32.to_le_bytes()); // indexes count
        bytes.push(2); // kind_tag = NonUnique
        encode_field_path_test(&mut bytes, &["id"]);
        bytes.extend_from_slice(&((MAX_COLLECTION_NAME_BYTES + 1) as u32).to_le_bytes());
        assert!(decode_catalog_payload(&bytes).is_err());
    }

    #[test]
    fn decode_indexes_rejects_unknown_index_kind_tag() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&CATALOG_PAYLOAD_VERSION_V4.to_le_bytes());
        bytes.extend_from_slice(&ENTRY_KIND_CREATE_COLLECTION.to_le_bytes());
        bytes.extend_from_slice(&1u32.to_le_bytes());
        encode_name(&mut bytes, "t");
        bytes.extend_from_slice(&1u32.to_le_bytes());
        encode_fields_v3(
            &mut bytes,
            &[FieldDef {
                path: path(&["id"]),
                ty: Type::Int64,
                constraints: Vec::new(),
            }],
        );
        bytes.extend_from_slice(&1u32.to_le_bytes()); // indexes count
        bytes.push(99); // unknown kind_tag
        assert!(decode_catalog_payload(&bytes).is_err());
    }

    #[test]
    fn decode_constraints_rejects_unknown_constraint_tag() {
        // v3 new schema version with one constraint tag that isn't recognized.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&CATALOG_PAYLOAD_VERSION_V3.to_le_bytes());
        bytes.extend_from_slice(&ENTRY_KIND_NEW_SCHEMA_VERSION.to_le_bytes());
        bytes.extend_from_slice(&1u32.to_le_bytes()); // collection_id
        bytes.extend_from_slice(&2u32.to_le_bytes()); // schema_version
        bytes.extend_from_slice(&1u32.to_le_bytes()); // fields count
        encode_field_path_test(&mut bytes, &["id"]);
        encode_type(&mut bytes, &Type::Int64, 0);
        bytes.extend_from_slice(&1u32.to_le_bytes()); // constraints count
        bytes.push(255); // unknown constraint tag
        assert!(decode_catalog_payload(&bytes).is_err());
    }

    #[test]
    fn decode_type_rejects_unknown_type_tag() {
        // v1 create collection with one field whose type tag is unknown.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&CATALOG_PAYLOAD_VERSION_V1.to_le_bytes());
        bytes.extend_from_slice(&ENTRY_KIND_CREATE_COLLECTION.to_le_bytes());
        bytes.extend_from_slice(&1u32.to_le_bytes());
        encode_name(&mut bytes, "t");
        bytes.extend_from_slice(&1u32.to_le_bytes());
        bytes.extend_from_slice(&1u32.to_le_bytes()); // fields count
        encode_field_path_test(&mut bytes, &["id"]);
        bytes.push(250); // unknown type tag
        assert!(decode_catalog_payload(&bytes).is_err());
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
                constraints: Vec::new(),
            }],
            indexes: vec![],
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
            indexes: vec![],
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
