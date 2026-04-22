//! Embedded Typra database engine: append-only segments, versioned schema catalog, and
//! last-write-wins row storage keyed by primary values.
//!
//! Start with [`Database`] for open, register, insert, and get. Supporting types include
//! [`Catalog`], [`ScalarValue`], [`FieldDef`], and [`DbError`]. For a small import set, use
//! [`prelude`].

pub mod catalog;
pub mod checksum;
pub mod config;
pub mod db;
pub mod error;
pub mod file_format;
pub mod manifest;
pub mod publish;
pub mod record;
pub mod schema;
pub mod segments;
pub mod storage;
pub mod superblock;
pub mod validation;

pub use catalog::{Catalog, CatalogRecord, CollectionInfo};
pub use db::Database;
pub use error::DbError;
pub use error::SchemaError;
pub use record::ScalarValue;
pub use schema::CollectionId;
pub use schema::CollectionSchema;
pub use schema::DbModel;
pub use schema::FieldDef;
pub use schema::SchemaVersion;
pub use schema::Type;

/// Convenient re-exports for typical application code (`Database`, schema types, [`DbError`]).
pub mod prelude {
    pub use crate::catalog::{Catalog, CollectionInfo};
    pub use crate::db::Database;
    pub use crate::error::DbError;
    pub use crate::record::ScalarValue;
    pub use crate::schema::CollectionId;
    pub use crate::schema::CollectionSchema;
    pub use crate::schema::DbModel;
    pub use crate::schema::FieldDef;
    pub use crate::schema::SchemaVersion;
}
