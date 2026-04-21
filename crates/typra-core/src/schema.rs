use std::borrow::Cow;

use crate::error::{DbError, SchemaError};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CollectionId(pub u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SchemaVersion(pub u32);

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FieldPath(pub Vec<Cow<'static, str>>);

impl FieldPath {
    pub fn new(parts: impl IntoIterator<Item = Cow<'static, str>>) -> Result<Self, DbError> {
        let parts: Vec<Cow<'static, str>> = parts.into_iter().collect();
        if parts.is_empty() || parts.iter().any(|p| p.is_empty()) {
            return Err(DbError::Schema(SchemaError::InvalidFieldPath));
        }
        Ok(Self(parts))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Type {
    Bool,
    Int64,
    Uint64,
    Float64,
    String,
    Bytes,
    Uuid,
    Timestamp,
    Optional(Box<Type>),
    List(Box<Type>),
    Object(Vec<FieldDef>),
    Enum(Vec<String>),
}

#[derive(Debug, Clone, PartialEq)]
pub struct FieldDef {
    pub path: FieldPath,
    pub ty: Type,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CollectionSchema {
    pub name: String,
    pub version: SchemaVersion,
    pub fields: Vec<FieldDef>,
    pub id: Option<CollectionId>,
}

/// Marker trait for Rust types that map to Typra collection records.
///
/// Implement via `#[derive(DbModel)]` from the `typra-derive` crate.
pub trait DbModel {}
