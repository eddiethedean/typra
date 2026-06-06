//! Application-facing ModelVault API: curated re-exports from [`modelvault_core`] and optionally
//! the [`DbModel`](modelvault_derive::DbModel) derive.
//!
//! Add to `Cargo.toml`:
//!
//! ```toml
//! [dependencies]
//! modelvault = "0.16"
//! ```
//!
//! Use [`prelude`] for common imports. For full control over dependencies, depend on the
//! `modelvault-core` and `modelvault-derive` crates directly.

pub use modelvault_core::catalog::{Catalog, CatalogRecord, CollectionInfo};
pub use modelvault_core::config::{
    OpenMode, OpenOptions, OpenOptionsBuilder, OpenRecoveryInfo, RecoveryMode,
};
pub use modelvault_core::db::Database;
pub use modelvault_core::error::{
    DbError, DbErrorKind, FormatError, SchemaError, TransactionError, ValidationError,
};
pub use modelvault_core::index::IndexState;
pub use modelvault_core::migration::{MigrationPlan, MigrationStep};
pub use modelvault_core::query::{OrderBy, OrderDirection, Query, QueryRowIter};
pub use modelvault_core::record::{RowValue, ScalarValue};
pub use modelvault_core::schema::DbModel;
pub use modelvault_core::schema::{
    CollectionId, CollectionSchema, Constraint, FieldDef, FieldPath, IndexDef, IndexKind,
    SchemaVersion, Type,
};
pub use modelvault_core::schema_compat::{
    classify_schema_update, validate_model_fields_against_catalog,
};

/// Schema types (same symbols as root re-exports; stable path for docs and examples).
pub mod schema {
    pub use modelvault_core::schema::DbModel;
    pub use modelvault_core::schema::{
        CollectionId, CollectionSchema, Constraint, FieldDef, FieldPath, IndexDef, IndexKind,
        SchemaVersion, Type,
    };
}
pub use modelvault_core::sql;
pub use modelvault_core::storage::{FileStore, Store, VecStore};

#[cfg(feature = "derive")]
pub use modelvault_derive::DbModel;

#[cfg(feature = "async")]
pub mod async_api;
#[cfg(feature = "async")]
mod db_guard;
#[cfg(feature = "async")]
pub use async_api::AsyncDatabase;

/// Advanced / unstable engine modules (same as `modelvault_core` hidden modules).
#[doc(hidden)]
pub mod internal {
    pub use modelvault_core::checkpoint;
    pub use modelvault_core::checksum;
    pub use modelvault_core::file_format;
    pub use modelvault_core::manifest;
    pub use modelvault_core::pager;
    pub use modelvault_core::publish;
    pub use modelvault_core::segments;
    pub use modelvault_core::spill;
    pub use modelvault_core::superblock;
    pub use modelvault_core::txn;
}

/// Re-exports [`modelvault_core::prelude`] plus [`DbModel`](modelvault_derive::DbModel) when **`derive`** is enabled.
pub mod prelude {
    pub use modelvault_core::prelude::*;

    #[cfg(feature = "derive")]
    pub use crate::DbModel;

    #[cfg(feature = "async")]
    pub use crate::AsyncDatabase;
}
