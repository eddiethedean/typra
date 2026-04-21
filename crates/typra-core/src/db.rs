use std::path::{Path, PathBuf};

use crate::error::{DbError, FormatError};
use crate::file_format::{decode_header, FileHeader, FILE_HEADER_SIZE};
use crate::storage::{FileStore, Store};

/// Handle to an on-disk Typra database file.
///
/// Version 0.1 only ensures the backing file exists and is openable; the storage engine is still under development.
pub struct Database {
    path: PathBuf,
    _store: FileStore,
}

impl Database {
    /// Open or create a database at `path`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, DbError> {
        let path = path.as_ref().to_path_buf();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;

        let mut store = FileStore::new(file);

        let len = store.len()?;
        if len == 0 {
            let header = FileHeader::new_v0_2();
            store.write_all_at(0, &header.encode())?;
            store.sync()?;
        } else if len < FILE_HEADER_SIZE as u64 {
            return Err(DbError::Format(FormatError::TruncatedHeader {
                got: len as usize,
                expected: FILE_HEADER_SIZE,
            }));
        } else {
            let mut buf = [0u8; FILE_HEADER_SIZE];
            store.read_exact_at(0, &mut buf)?;
            let _header = decode_header(&buf)?;
        }

        Ok(Self { path, _store: store })
    }

    /// Path passed to [`Database::open`](Self::open).
    pub fn path(&self) -> &Path {
        &self.path
    }
}
