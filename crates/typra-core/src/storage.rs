use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};

use crate::error::DbError;

/// Random-access byte image of a Typra database (length, read, write, sync).
///
/// Implemented by [`FileStore`] (real files) and [`VecStore`] (in-memory snapshots). A future
/// read-only store split is deferred until a second consumer needs a smaller surface.
pub trait Store {
    fn len(&self) -> Result<u64, DbError>;
    fn is_empty(&self) -> Result<bool, DbError> {
        Ok(self.len()? == 0)
    }
    fn read_exact_at(&mut self, offset: u64, buf: &mut [u8]) -> Result<(), DbError>;
    fn write_all_at(&mut self, offset: u64, buf: &[u8]) -> Result<(), DbError>;
    fn sync(&mut self) -> Result<(), DbError>;
    /// Shrink or grow the logical file to `len` bytes (used for crash recovery truncation).
    fn truncate(&mut self, len: u64) -> Result<(), DbError>;
}

// In 0.2.x this is intentionally internal scaffolding.
// The public API should not expose storage mechanics yet.
#[derive(Debug)]
struct RawFileStore {
    file: File,
}

impl RawFileStore {
    fn new(file: File) -> Self {
        Self { file }
    }
}

impl Store for RawFileStore {
    fn len(&self) -> Result<u64, DbError> {
        Ok(self.file.metadata()?.len())
    }

    fn read_exact_at(&mut self, offset: u64, buf: &mut [u8]) -> Result<(), DbError> {
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.read_exact(buf)?;
        Ok(())
    }

    fn write_all_at(&mut self, offset: u64, buf: &[u8]) -> Result<(), DbError> {
        self.file.seek(SeekFrom::Start(offset))?;
        self.file.write_all(buf)?;
        Ok(())
    }

    fn sync(&mut self) -> Result<(), DbError> {
        self.file.sync_all()?;
        Ok(())
    }

    fn truncate(&mut self, len: u64) -> Result<(), DbError> {
        self.file.set_len(len)?;
        Ok(())
    }
}

/// On-disk store: a real file wrapped in a fixed-size page cache.
///
/// This keeps the public `FileStore` name stable while introducing the 0.11.0 pager/buffer-pool
/// boundary via [`crate::pager::PagedStore`].
#[derive(Debug)]
pub struct FileStore {
    inner: crate::pager::PagedStore<RawFileStore>,
}

impl FileStore {
    pub fn new(file: File) -> Self {
        Self {
            inner: crate::pager::PagedStore::new(
                RawFileStore::new(file),
                crate::pager::DEFAULT_PAGE_SIZE,
            ),
        }
    }
}

impl Store for FileStore {
    fn len(&self) -> Result<u64, DbError> {
        self.inner.len()
    }

    fn read_exact_at(&mut self, offset: u64, buf: &mut [u8]) -> Result<(), DbError> {
        self.inner.read_exact_at(offset, buf)
    }

    fn write_all_at(&mut self, offset: u64, buf: &[u8]) -> Result<(), DbError> {
        self.inner.write_all_at(offset, buf)
    }

    fn sync(&mut self) -> Result<(), DbError> {
        self.inner.sync()
    }

    fn truncate(&mut self, len: u64) -> Result<(), DbError> {
        self.inner.truncate(len)
    }
}

/// In-memory growable byte store (same [`Store`] contract as [`FileStore`]).
#[derive(Debug, Default)]
pub struct VecStore {
    buf: Vec<u8>,
}

impl VecStore {
    pub fn new() -> Self {
        Self { buf: Vec::new() }
    }

    pub fn into_inner(self) -> Vec<u8> {
        self.buf
    }

    pub fn from_vec(buf: Vec<u8>) -> Self {
        Self { buf }
    }

    /// Full buffer (read-only image of the logical file).
    pub fn as_slice(&self) -> &[u8] {
        &self.buf
    }

    fn ensure_len(&mut self, end: u64) {
        let need = end as usize;
        if self.buf.len() < need {
            self.buf.resize(need, 0);
        }
    }
}

impl Store for VecStore {
    fn len(&self) -> Result<u64, DbError> {
        Ok(self.buf.len() as u64)
    }

    fn read_exact_at(&mut self, offset: u64, buf: &mut [u8]) -> Result<(), DbError> {
        let start = offset as usize;
        let end = start.saturating_add(buf.len());
        if end > self.buf.len() {
            return Err(DbError::Io(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "read past end of VecStore",
            )));
        }
        buf.copy_from_slice(&self.buf[start..end]);
        Ok(())
    }

    fn write_all_at(&mut self, offset: u64, data: &[u8]) -> Result<(), DbError> {
        let end = offset
            .checked_add(data.len() as u64)
            .ok_or_else(|| std::io::Error::new(std::io::ErrorKind::InvalidInput, "overflow"))?;
        self.ensure_len(end);
        let start = offset as usize;
        self.buf[start..start + data.len()].copy_from_slice(data);
        Ok(())
    }

    fn sync(&mut self) -> Result<(), DbError> {
        Ok(())
    }

    fn truncate(&mut self, len: u64) -> Result<(), DbError> {
        self.buf.truncate(len as usize);
        Ok(())
    }
}
