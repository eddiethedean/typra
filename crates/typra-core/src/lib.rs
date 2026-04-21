pub mod catalog;
pub mod checksum;
pub mod config;
pub mod db;
pub mod error;
pub mod file_format;
pub mod manifest;
pub mod publish;
pub mod schema;
pub mod segments;
pub mod storage;
pub mod superblock;
pub mod validation;

pub use catalog::{Catalog, CatalogRecord, CollectionInfo};
pub use db::Database;
pub use error::DbError;
pub use error::SchemaError;
pub use schema::CollectionId;
pub use schema::CollectionSchema;
pub use schema::DbModel;
pub use schema::FieldDef;
pub use schema::SchemaVersion;
pub use schema::Type;

/// Commonly used types and traits.
pub mod prelude {
    pub use crate::catalog::{Catalog, CollectionInfo};
    pub use crate::db::Database;
    pub use crate::error::DbError;
    pub use crate::schema::CollectionId;
    pub use crate::schema::CollectionSchema;
    pub use crate::schema::DbModel;
    pub use crate::schema::FieldDef;
    pub use crate::schema::SchemaVersion;
}
