use std::path::{Path, PathBuf};

use crate::error::DbError;

/// Handle to an on-disk Typra database file.
///
/// Version 0.1 only ensures the backing file exists and is openable; the storage engine is still under development.
pub struct Database {
    path: PathBuf,
}

impl Database {
    /// Open or create a database at `path`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, DbError> {
        let path = path.as_ref().to_path_buf();
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(&path)?;
        Ok(Self { path })
    }

    /// Path passed to [`Database::open`](Self::open).
    pub fn path(&self) -> &Path {
        &self.path
    }
}
