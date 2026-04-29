//! Embedded Typra database engine: append-only segments, versioned schema catalog, and
//! last-write-wins row storage keyed by primary values.
//!
//! Start with [`Database`] for open, register, insert, and get. Supporting types include
//! [`Catalog`], [`ScalarValue`], [`FieldDef`], and [`DbError`]. For a small import set, use
//! [`prelude`].

pub mod catalog;
pub mod checkpoint;
pub mod checksum;
pub mod config;
pub use config::{OpenMode, OpenOptions, RecoveryMode};
pub mod db;
pub mod error;
pub mod file_format;
pub mod index;
pub mod manifest;
pub mod migration;
pub mod pager;
pub mod publish;
pub mod query;
pub mod record;
pub mod schema;
pub mod segments;
pub mod spill;
pub mod sql;
pub mod storage;
pub mod superblock;
pub mod txn;
pub mod validation;

pub use catalog::{Catalog, CatalogRecord, CollectionInfo};
pub use db::Database;
pub use db::LatestMap;
pub use error::DbError;
pub use error::DbErrorKind;
pub use error::FormatError;
pub use error::SchemaError;
pub use error::TransactionError;
pub use error::ValidationError;
pub use migration::{MigrationPlan, MigrationStep};
pub use record::RowValue;
pub use record::ScalarValue;
pub use schema::CollectionId;
pub use schema::CollectionSchema;
pub use schema::Constraint;
pub use schema::DbModel;
pub use schema::FieldDef;
pub use schema::IndexDef;
pub use schema::IndexKind;
pub use schema::SchemaVersion;
pub use schema::Type;

/// Convenient re-exports for typical application code (`Database`, schema types, [`DbError`]).
pub mod prelude {
    pub use crate::catalog::{Catalog, CollectionInfo};
    pub use crate::db::Database;
    pub use crate::error::DbError;
    pub use crate::record::RowValue;
    pub use crate::record::ScalarValue;
    pub use crate::schema::CollectionId;
    pub use crate::schema::CollectionSchema;
    pub use crate::schema::DbModel;
    pub use crate::schema::FieldDef;
    pub use crate::schema::IndexDef;
    pub use crate::schema::IndexKind;
    pub use crate::schema::SchemaVersion;
}
