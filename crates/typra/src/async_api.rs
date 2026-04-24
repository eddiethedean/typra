use std::path::Path;
use std::sync::{Arc, Mutex};

use typra_core::storage::{FileStore, Store, VecStore};
use typra_core::{Database, DbError, OpenOptions, RowValue, ScalarValue};

/// Async wrapper over [`Database`].
///
/// This is an integration convenience for async applications. Internally, operations execute on
/// a Tokio blocking thread via [`tokio::task::spawn_blocking`].
pub struct AsyncDatabase<S: Store = FileStore> {
    inner: Arc<Mutex<Database<S>>>,
}

impl AsyncDatabase<FileStore> {
    pub async fn open(path: impl AsRef<Path> + Send + 'static) -> Result<Self, DbError> {
        let path = path.as_ref().to_path_buf();
        let db = tokio::task::spawn_blocking(move || Database::open(path))
            .await
            .expect("spawn_blocking failed")?;
        Ok(Self {
            inner: Arc::new(Mutex::new(db)),
        })
    }

    pub async fn open_with_options(
        path: impl AsRef<Path> + Send + 'static,
        opts: OpenOptions,
    ) -> Result<Self, DbError> {
        let path = path.as_ref().to_path_buf();
        let db = tokio::task::spawn_blocking(move || Database::open_with_options(path, opts))
            .await
            .expect("spawn_blocking failed")?;
        Ok(Self {
            inner: Arc::new(Mutex::new(db)),
        })
    }
}

impl AsyncDatabase<VecStore> {
    pub async fn open_in_memory() -> Result<Self, DbError> {
        let db = tokio::task::spawn_blocking(Database::<VecStore>::open_in_memory)
            .await
            .expect("spawn_blocking failed")?;
        Ok(Self {
            inner: Arc::new(Mutex::new(db)),
        })
    }

    pub async fn open_snapshot_bytes(data: Vec<u8>) -> Result<Self, DbError> {
        let db =
            tokio::task::spawn_blocking(move || Database::<VecStore>::from_snapshot_bytes(data))
                .await
                .expect("spawn_blocking failed")?;
        Ok(Self {
            inner: Arc::new(Mutex::new(db)),
        })
    }
}

impl<S: Store + Send + 'static> AsyncDatabase<S> {
    pub fn clone_handle(&self) -> Self {
        Self {
            inner: Arc::clone(&self.inner),
        }
    }

    pub async fn path_string(&self) -> String {
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || {
            inner
                .lock()
                .expect("typra db mutex poisoned")
                .path()
                .display()
                .to_string()
        })
        .await
        .expect("spawn_blocking failed")
    }

    pub async fn collection_names(&self) -> Result<Vec<String>, DbError> {
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || {
            let db = inner.lock().expect("typra db mutex poisoned");
            Ok(db.collection_names())
        })
        .await
        .expect("spawn_blocking failed")
    }

    pub async fn register_collection(
        &self,
        name: String,
        fields: Vec<typra_core::FieldDef>,
        primary_field: String,
    ) -> Result<(typra_core::CollectionId, typra_core::SchemaVersion), DbError> {
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || {
            let mut db = inner.lock().expect("typra db mutex poisoned");
            db.register_collection(&name, fields, &primary_field)
        })
        .await
        .expect("spawn_blocking failed")
    }

    pub async fn insert(
        &self,
        collection_id: typra_core::CollectionId,
        row: std::collections::BTreeMap<String, RowValue>,
    ) -> Result<(), DbError> {
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || {
            let mut db = inner.lock().expect("typra db mutex poisoned");
            db.insert(collection_id, row)
        })
        .await
        .expect("spawn_blocking failed")
    }

    pub async fn get(
        &self,
        collection_id: typra_core::CollectionId,
        pk: ScalarValue,
    ) -> Result<Option<std::collections::BTreeMap<String, RowValue>>, DbError> {
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || {
            let db = inner.lock().expect("typra db mutex poisoned");
            db.get(collection_id, &pk)
        })
        .await
        .expect("spawn_blocking failed")
    }

    pub async fn delete(
        &self,
        collection_id: typra_core::CollectionId,
        pk: ScalarValue,
    ) -> Result<(), DbError> {
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || {
            let mut db = inner.lock().expect("typra db mutex poisoned");
            db.delete(collection_id, &pk)
        })
        .await
        .expect("spawn_blocking failed")
    }

    pub async fn transaction<R: Send + 'static>(
        &self,
        f: impl FnOnce(&mut Database<S>) -> Result<R, DbError> + Send + 'static,
    ) -> Result<R, DbError> {
        let inner = Arc::clone(&self.inner);
        tokio::task::spawn_blocking(move || {
            let mut db = inner.lock().expect("typra db mutex poisoned");
            db.transaction(f)
        })
        .await
        .expect("spawn_blocking failed")
    }
}
