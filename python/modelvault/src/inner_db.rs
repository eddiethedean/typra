//! Dispatches engine calls to either [`modelvault_core::storage::FileStore`] or [`modelvault_core::storage::VecStore`].

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use std::collections::BTreeMap;

use modelvault_core::config::{OpenMode, OpenOptions, RecoveryMode};
use modelvault_core::query::Query;
use modelvault_core::query::QueryRowIter;
use modelvault_core::record::{RowValue, ScalarValue};
use modelvault_core::schema::{CollectionId, FieldDef, IndexDef, SchemaVersion};
use modelvault_core::storage::{FileStore, VecStore};
use modelvault_core::Database as CoreDatabase;
use modelvault_core::MigrationPlan;

use crate::errors::db_error_to_py;

pub(crate) enum InnerDb {
    File(CoreDatabase<FileStore>),
    Mem(CoreDatabase<VecStore>),
}

impl InnerDb {
    pub(crate) fn parse_recovery_mode(recovery: Option<&str>) -> Result<RecoveryMode, PyErr> {
        match recovery {
            Some("strict") => Ok(RecoveryMode::Strict),
            Some("auto_truncate") => Ok(RecoveryMode::AutoTruncate),
            Some(other) => Err(PyValueError::new_err(format!(
                "recovery must be 'strict' or 'auto_truncate', got {other:?}"
            ))),
            None => Ok(RecoveryMode::AutoTruncate),
        }
    }

    /// Match Rust defaults: read-write opens salvage torn tails; read-only opens stay strict.
    pub(crate) fn resolve_recovery_mode(
        read_only: bool,
        recovery: Option<&str>,
    ) -> Result<RecoveryMode, PyErr> {
        match (read_only, recovery) {
            (true, None) => Ok(RecoveryMode::Strict),
            (_, Some(_)) => Self::parse_recovery_mode(recovery),
            (false, None) => Ok(RecoveryMode::AutoTruncate),
        }
    }

    pub(crate) fn open_path(
        path: &str,
        read_only: bool,
        recovery: Option<&str>,
    ) -> Result<Self, PyErr> {
        Self::open_path_with_recovery(
            path,
            read_only,
            Self::resolve_recovery_mode(read_only, recovery)?,
        )
    }

    pub(crate) fn open_path_with_recovery(
        path: &str,
        read_only: bool,
        recovery: RecoveryMode,
    ) -> Result<Self, PyErr> {
        let opts = OpenOptions {
            mode: if read_only {
                OpenMode::ReadOnly
            } else {
                OpenMode::ReadWrite
            },
            recovery,
        };
        let db = CoreDatabase::open_with_options(path, opts).map_err(db_error_to_py)?;
        Ok(InnerDb::File(db))
    }

    pub(crate) fn open_in_memory() -> Result<Self, PyErr> {
        CoreDatabase::open_in_memory()
            .map(InnerDb::Mem)
            .map_err(db_error_to_py)
    }

    pub(crate) fn from_snapshot_bytes(data: Vec<u8>) -> Result<Self, PyErr> {
        CoreDatabase::from_snapshot_bytes(data)
            .map(InnerDb::Mem)
            .map_err(db_error_to_py)
    }

    pub(crate) fn open_snapshot_path(path: &str) -> Result<Self, PyErr> {
        CoreDatabase::open_snapshot_path(path)
            .map(InnerDb::Mem)
            .map_err(db_error_to_py)
    }

    pub(crate) fn register_collection_with_indexes(
        &mut self,
        name: &str,
        fields: Vec<FieldDef>,
        indexes: Vec<IndexDef>,
        primary_field: &str,
    ) -> Result<(CollectionId, SchemaVersion), modelvault_core::DbError> {
        match self {
            InnerDb::File(d) => {
                d.register_collection_with_indexes(name, fields, indexes, primary_field)
            }
            InnerDb::Mem(d) => {
                d.register_collection_with_indexes(name, fields, indexes, primary_field)
            }
        }
    }

    pub(crate) fn collection_names(&self) -> Vec<String> {
        match self {
            InnerDb::File(d) => d.collection_names(),
            InnerDb::Mem(d) => d.collection_names(),
        }
    }

    pub(crate) fn collection_id_named(
        &self,
        name: &str,
    ) -> Result<CollectionId, modelvault_core::DbError> {
        match self {
            InnerDb::File(d) => d.collection_id_named(name),
            InnerDb::Mem(d) => d.collection_id_named(name),
        }
    }

    pub(crate) fn catalog(&self) -> modelvault_core::Catalog {
        match self {
            InnerDb::File(d) => d.snapshot_catalog(),
            InnerDb::Mem(d) => d.catalog().clone(),
        }
    }

    pub(crate) fn insert(
        &mut self,
        id: CollectionId,
        row: BTreeMap<String, RowValue>,
    ) -> Result<(), modelvault_core::DbError> {
        match self {
            InnerDb::File(d) => d.insert(id, row),
            InnerDb::Mem(d) => d.insert(id, row),
        }
    }

    pub(crate) fn delete(
        &mut self,
        id: CollectionId,
        pk: &ScalarValue,
    ) -> Result<(), modelvault_core::DbError> {
        match self {
            InnerDb::File(d) => d.delete(id, pk),
            InnerDb::Mem(d) => d.delete(id, pk),
        }
    }

    pub(crate) fn register_schema_version_with_indexes(
        &mut self,
        id: CollectionId,
        fields: Vec<FieldDef>,
        indexes: Vec<IndexDef>,
    ) -> Result<SchemaVersion, modelvault_core::DbError> {
        match self {
            InnerDb::File(d) => d.register_schema_version_with_indexes(id, fields, indexes),
            InnerDb::Mem(d) => d.register_schema_version_with_indexes(id, fields, indexes),
        }
    }

    pub(crate) fn register_schema_version_with_indexes_force(
        &mut self,
        id: CollectionId,
        fields: Vec<FieldDef>,
        indexes: Vec<IndexDef>,
    ) -> Result<SchemaVersion, modelvault_core::DbError> {
        match self {
            InnerDb::File(d) => d.register_schema_version_with_indexes_force(id, fields, indexes),
            InnerDb::Mem(d) => d.register_schema_version_with_indexes_force(id, fields, indexes),
        }
    }

    pub(crate) fn plan_schema_version_with_indexes(
        &self,
        id: CollectionId,
        fields: Vec<FieldDef>,
        indexes: Vec<IndexDef>,
    ) -> Result<MigrationPlan, modelvault_core::DbError> {
        match self {
            InnerDb::File(d) => d.plan_schema_version_with_indexes(id, fields, indexes),
            InnerDb::Mem(d) => d.plan_schema_version_with_indexes(id, fields, indexes),
        }
    }

    pub(crate) fn backfill_top_level_field_with_value(
        &mut self,
        id: CollectionId,
        field: &str,
        value: RowValue,
    ) -> Result<(), modelvault_core::DbError> {
        match self {
            InnerDb::File(d) => d.backfill_top_level_field_with_value(id, field, value),
            InnerDb::Mem(d) => d.backfill_top_level_field_with_value(id, field, value),
        }
    }

    pub(crate) fn backfill_field_at_path_with_value(
        &mut self,
        id: CollectionId,
        path: &modelvault_core::schema::FieldPath,
        value: RowValue,
    ) -> Result<(), modelvault_core::DbError> {
        match self {
            InnerDb::File(d) => d.backfill_field_at_path_with_value(id, path, value),
            InnerDb::Mem(d) => d.backfill_field_at_path_with_value(id, path, value),
        }
    }

    pub(crate) fn rebuild_indexes_for_collection(
        &mut self,
        id: CollectionId,
    ) -> Result<(), modelvault_core::DbError> {
        match self {
            InnerDb::File(d) => d.rebuild_indexes_for_collection(id),
            InnerDb::Mem(d) => d.rebuild_indexes_for_collection(id),
        }
    }

    pub(crate) fn get(
        &self,
        id: CollectionId,
        pk: &ScalarValue,
    ) -> Result<Option<BTreeMap<String, RowValue>>, modelvault_core::DbError> {
        match self {
            InnerDb::File(d) => d.get(id, pk),
            InnerDb::Mem(d) => d.get(id, pk),
        }
    }

    pub(crate) fn query(
        &self,
        q: &Query,
    ) -> Result<Vec<BTreeMap<String, RowValue>>, modelvault_core::DbError> {
        match self {
            InnerDb::File(d) => d.query(q),
            InnerDb::Mem(d) => d.query(q),
        }
    }

    pub(crate) fn query_iter(
        &self,
        q: &Query,
    ) -> Result<QueryRowIter<'_>, modelvault_core::DbError> {
        match self {
            InnerDb::File(d) => d.query_iter(q),
            InnerDb::Mem(d) => d.query_iter(q),
        }
    }

    pub(crate) fn explain_query(&self, q: &Query) -> Result<String, modelvault_core::DbError> {
        match self {
            InnerDb::File(d) => d.explain_query(q),
            InnerDb::Mem(d) => d.explain_query(q),
        }
    }

    pub(crate) fn path_display(&self) -> String {
        match self {
            InnerDb::File(d) => d.path().display().to_string(),
            InnerDb::Mem(d) => d.path().display().to_string(),
        }
    }

    pub(crate) fn recovery_info(&self) -> modelvault_core::OpenRecoveryInfo {
        match self {
            InnerDb::File(d) => d.recovery_info().clone(),
            InnerDb::Mem(_) => modelvault_core::OpenRecoveryInfo::default(),
        }
    }

    pub(crate) fn snapshot_bytes(&self) -> Result<Vec<u8>, PyErr> {
        match self {
            InnerDb::File(_) => Err(PyValueError::new_err(
                "snapshot_bytes is only supported for in-memory databases",
            )),
            InnerDb::Mem(d) => Ok(d.snapshot_bytes()),
        }
    }

    pub(crate) fn compact_to(&self, dest_path: &str) -> Result<(), PyErr> {
        match self {
            InnerDb::File(d) => d.compact_to(dest_path).map_err(db_error_to_py),
            InnerDb::Mem(_) => Err(PyValueError::new_err(
                "compact_to is only supported for file-backed databases",
            )),
        }
    }

    pub(crate) fn compact_in_place(&mut self) -> Result<(), PyErr> {
        match self {
            InnerDb::File(d) => d.compact_in_place().map_err(db_error_to_py),
            InnerDb::Mem(_) => Err(PyValueError::new_err(
                "compact_in_place is only supported for file-backed databases",
            )),
        }
    }

    pub(crate) fn export_snapshot_to_path(&mut self, dest_path: &str) -> PyResult<()> {
        match self {
            InnerDb::File(d) => d.export_snapshot_to_path(dest_path).map_err(db_error_to_py),
            InnerDb::Mem(d) => d.export_snapshot_to_path(dest_path).map_err(db_error_to_py),
        }
    }

    pub(crate) fn restore_snapshot_to_path(
        snapshot_path: &str,
        dest_path: &str,
    ) -> Result<(), PyErr> {
        modelvault_core::Database::<modelvault_core::storage::FileStore>::restore_snapshot_to_path(
            snapshot_path,
            dest_path,
        )
        .map_err(db_error_to_py)
    }

    pub(crate) fn begin_transaction(&mut self) -> Result<(), modelvault_core::DbError> {
        match self {
            InnerDb::File(d) => d.begin_transaction(),
            InnerDb::Mem(d) => d.begin_transaction(),
        }
    }

    pub(crate) fn commit_transaction(&mut self) -> Result<(), modelvault_core::DbError> {
        match self {
            InnerDb::File(d) => d.commit_transaction(),
            InnerDb::Mem(d) => d.commit_transaction(),
        }
    }

    pub(crate) fn rollback_transaction(&mut self) {
        match self {
            InnerDb::File(d) => d.rollback_transaction(),
            InnerDb::Mem(d) => d.rollback_transaction(),
        }
    }
}
