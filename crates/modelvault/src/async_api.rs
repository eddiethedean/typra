use std::path::Path;

use modelvault_core::storage::{FileStore, Store, VecStore};
use modelvault_core::{Database, DbError, OpenOptions, RowValue, ScalarValue};

use crate::db_guard::{read_db, write_db, DbState, SharedDbState};

fn map_join_err(e: tokio::task::JoinError) -> DbError {
    DbError::Io(std::io::Error::other(format!(
        "tokio spawn_blocking join error: {e}"
    )))
}

/// Async wrapper over [`Database`].
///
/// This is an integration convenience for async applications. Internally, operations execute on
/// a Tokio blocking thread via [`tokio::task::spawn_blocking`].
///
/// Reads may run concurrently; writes and open transactions take an exclusive lock.
pub struct AsyncDatabase<S: Store = FileStore> {
    inner: SharedDbState<S>,
}

impl AsyncDatabase<FileStore> {
    pub async fn open(path: impl AsRef<Path> + Send + 'static) -> Result<Self, DbError> {
        let path = path.as_ref().to_path_buf();
        let db = tokio::task::spawn_blocking(move || Database::open(path))
            .await
            .map_err(map_join_err)??;
        Ok(Self {
            inner: std::sync::Arc::new(DbState::new(db)),
        })
    }

    pub async fn open_with_options(
        path: impl AsRef<Path> + Send + 'static,
        opts: OpenOptions,
    ) -> Result<Self, DbError> {
        let path = path.as_ref().to_path_buf();
        let db = tokio::task::spawn_blocking(move || Database::open_with_options(path, opts))
            .await
            .map_err(map_join_err)??;
        Ok(Self {
            inner: std::sync::Arc::new(DbState::new(db)),
        })
    }
}

impl AsyncDatabase<VecStore> {
    pub async fn open_in_memory() -> Result<Self, DbError> {
        let db = tokio::task::spawn_blocking(Database::<VecStore>::open_in_memory)
            .await
            .map_err(map_join_err)??;
        Ok(Self {
            inner: std::sync::Arc::new(DbState::new(db)),
        })
    }

    pub async fn open_snapshot_bytes(data: Vec<u8>) -> Result<Self, DbError> {
        let db =
            tokio::task::spawn_blocking(move || Database::<VecStore>::from_snapshot_bytes(data))
                .await
                .map_err(map_join_err)??;
        Ok(Self {
            inner: std::sync::Arc::new(DbState::new(db)),
        })
    }
}

impl<S: Store + Send + Sync + 'static> AsyncDatabase<S> {
    pub fn clone_handle(&self) -> Self {
        Self {
            inner: std::sync::Arc::clone(&self.inner),
        }
    }

    pub async fn path_string(&self) -> String {
        let inner = std::sync::Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || {
            read_db(inner.as_ref())
                .map(|db| db.path().display().to_string())
                .unwrap_or_else(|e| format!("error:{e:?}"))
        })
        .await
        .unwrap_or_else(|e| format!("error:{:?}", map_join_err(e)))
    }

    pub async fn collection_names(&self) -> Result<Vec<String>, DbError> {
        let inner = std::sync::Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || {
            let db = read_db(inner.as_ref())?;
            Ok(db.collection_names())
        })
        .await
        .map_err(map_join_err)?
    }

    pub async fn register_collection(
        &self,
        name: String,
        fields: Vec<modelvault_core::FieldDef>,
        primary_field: String,
    ) -> Result<
        (
            modelvault_core::CollectionId,
            modelvault_core::SchemaVersion,
        ),
        DbError,
    > {
        let inner = std::sync::Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || {
            let mut db = write_db(inner.as_ref())?;
            db.register_collection(&name, fields, &primary_field)
        })
        .await
        .map_err(map_join_err)?
    }

    pub async fn insert(
        &self,
        collection_id: modelvault_core::CollectionId,
        row: std::collections::BTreeMap<String, RowValue>,
    ) -> Result<(), DbError> {
        let inner = std::sync::Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || {
            let mut db = write_db(inner.as_ref())?;
            db.insert(collection_id, row)
        })
        .await
        .map_err(map_join_err)?
    }

    pub async fn get(
        &self,
        collection_id: modelvault_core::CollectionId,
        pk: ScalarValue,
    ) -> Result<Option<std::collections::BTreeMap<String, RowValue>>, DbError> {
        let inner = std::sync::Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || {
            let db = read_db(inner.as_ref())?;
            db.get(collection_id, &pk)
        })
        .await
        .map_err(map_join_err)?
    }

    pub async fn delete(
        &self,
        collection_id: modelvault_core::CollectionId,
        pk: ScalarValue,
    ) -> Result<(), DbError> {
        let inner = std::sync::Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || {
            let mut db = write_db(inner.as_ref())?;
            db.delete(collection_id, &pk)
        })
        .await
        .map_err(map_join_err)?
    }

    pub async fn transaction<R: Send + 'static>(
        &self,
        f: impl FnOnce(&mut Database<S>) -> Result<R, DbError> + Send + 'static,
    ) -> Result<R, DbError> {
        let inner = std::sync::Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || {
            let mut db = write_db(inner.as_ref())?;
            inner.txn_enter();
            let result = db.transaction(f);
            inner.txn_exit();
            result
        })
        .await
        .map_err(map_join_err)?
    }
}
