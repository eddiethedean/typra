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

/// Declarative constraint on a field (0.6+). Evaluated on insert after type checks.
#[derive(Debug, Clone, PartialEq)]
pub enum Constraint {
    /// Minimum inclusive for signed integers (`Int64`).
    MinI64(i64),
    /// Maximum inclusive for signed integers (`Int64`).
    MaxI64(i64),
    /// Minimum inclusive for unsigned integers (`Uint64`).
    MinU64(u64),
    /// Maximum inclusive for unsigned integers (`Uint64`).
    MaxU64(u64),
    /// Minimum inclusive for floats (`Float64`).
    MinF64(f64),
    /// Maximum inclusive for floats (`Float64`).
    MaxF64(f64),
    /// Minimum UTF-8 byte length (`String`) or element count (`List`).
    MinLength(u64),
    /// Maximum UTF-8 byte length (`String`) or element count (`List`).
    MaxLength(u64),
    /// Rust regex syntax (applied to `String`).
    Regex(String),
    /// Loose email shape check (`String`).
    Email,
    /// `http`/`https` URL prefix check (`String`).
    Url,
    /// Non-empty string, bytes, or list.
    NonEmpty,
}

/// One field’s path, type, and optional constraints within a collection schema.
#[derive(Debug, Clone, PartialEq)]
pub struct FieldDef {
    pub path: FieldPath,
    pub ty: Type,
    pub constraints: Vec<Constraint>,
}

impl FieldDef {
    pub fn new(path: FieldPath, ty: Type) -> Self {
        Self {
            path,
            ty,
            constraints: Vec::new(),
        }
    }
}

/// Kind of secondary index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexKind {
    /// Enforces a uniqueness constraint: one primary key per indexed value.
    Unique,
    /// Non-unique index: many primary keys per indexed value.
    NonUnique,
}

/// Secondary index definition for one collection schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexDef {
    /// Stable identifier within a collection schema (e.g. `"email_unique"`).
    pub name: String,
    /// Field path whose scalar value is indexed (may be nested, e.g. `["profile","timezone"]`).
    pub path: FieldPath,
    pub kind: IndexKind,
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
pub trait DbModel {
    fn collection_name() -> &'static str;
    fn fields() -> Vec<FieldDef>;
    fn primary_field() -> &'static str;
    fn indexes() -> Vec<IndexDef> {
        Vec::new()
    }
}
