//! Collection identity, field paths, logical [`Type`] values, and the [`DbModel`] marker trait.

use std::borrow::Cow;

use crate::error::{DbError, SchemaError};

/// Stable numeric id for a registered collection (assigned at create time, starting at `1`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CollectionId(pub u32);

/// Monotonic schema version for one collection (starts at `1` on create; bumps on each new version).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SchemaVersion(pub u32);

/// Dot-style path segments for a field (v1 rows use single-segment top-level names only).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FieldPath(pub Vec<Cow<'static, str>>);

impl FieldPath {
    /// Build a path from non-empty UTF-8 segments (rejects empty paths or empty segments).
    pub fn new(parts: impl IntoIterator<Item = Cow<'static, str>>) -> Result<Self, DbError> {
        let parts: Vec<Cow<'static, str>> = parts.into_iter().collect();
        if parts.is_empty() || parts.iter().any(|p| p.is_empty()) {
            return Err(DbError::Schema(SchemaError::InvalidFieldPath));
        }
        Ok(Self(parts))
    }
}

/// Logical type of a field in the catalog (mirrors encoding in record payloads where supported).
#[derive(Debug, Clone, PartialEq)]
pub enum Type {
    /// Boolean.
    Bool,
    /// Signed 64-bit integer.
    Int64,
    /// Unsigned 64-bit integer.
    Uint64,
    /// IEEE-754 double.
    Float64,
    /// UTF-8 string.
    String,
    /// Raw bytes.
    Bytes,
    /// 16-byte UUID (canonical record encoding uses tagged bytes).
    Uuid,
    /// Signed epoch milliseconds (or engine-defined timestamp unit).
    Timestamp,
    /// Value may be absent (`None`).
    Optional(Box<Type>),
    /// Homogeneous list.
    List(Box<Type>),
    /// Fixed set of nested fields (struct-like).
    Object(Vec<FieldDef>),
    /// Tagged union of string variants.
    Enum(Vec<String>),
}

/// One field’s path and type within a collection schema.
#[derive(Debug, Clone, PartialEq)]
pub struct FieldDef {
    pub path: FieldPath,
    pub ty: Type,
}

/// High-level description of a collection (name, version, fields); used by tooling and derives.
#[derive(Debug, Clone, PartialEq)]
pub struct CollectionSchema {
    pub name: String,
    pub version: SchemaVersion,
    pub fields: Vec<FieldDef>,
    pub id: Option<CollectionId>,
}

/// Marker trait for Rust types that map to Typra collection records.
///
/// Implement via `#[derive(DbModel)]` from the optional `typra-derive` crate (re-exported by the
/// `typra` facade when the **`derive`** feature is enabled).
pub trait DbModel {}
