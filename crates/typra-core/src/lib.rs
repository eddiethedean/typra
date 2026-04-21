pub mod checksum;
pub mod config;
pub mod db;
pub mod error;
pub mod file_format;
pub mod schema;
pub mod segments;
pub mod storage;
pub mod superblock;
pub mod validation;

pub use db::Database;
pub use error::DbError;
pub use schema::CollectionSchema;
pub use schema::DbModel;

/// Commonly used types and traits.
pub mod prelude {
    pub use crate::db::Database;
    pub use crate::error::DbError;
    pub use crate::schema::CollectionSchema;
    pub use crate::schema::DbModel;
}
