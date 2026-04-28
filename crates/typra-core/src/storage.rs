use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use crate::config::OpenMode;
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
    _writer_lock: Option<WriterLockGuard>,
    _reader_lock: Option<File>,
}

#[derive(Debug)]
struct WriterLockState {
    _file: File,
    refs: usize,
}

static WRITER_LOCKS: OnceLock<Mutex<std::collections::HashMap<PathBuf, WriterLockState>>> =
    OnceLock::new();

fn writer_locks() -> &'static Mutex<std::collections::HashMap<PathBuf, WriterLockState>> {
    WRITER_LOCKS.get_or_init(|| Mutex::new(std::collections::HashMap::new()))
}

#[derive(Debug)]
struct WriterLockGuard {
    lock_path: PathBuf,
}

impl Drop for WriterLockGuard {
    fn drop(&mut self) {
        let mut g = writer_locks().lock().unwrap_or_else(|e| e.into_inner());
        if let Some(st) = g.get_mut(&self.lock_path) { st.refs = st.refs.saturating_sub(1); if st.refs == 0 { g.remove(&self.lock_path); } }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use fs2::FileExt;

    #[test]
    fn open_locked_readonly_returns_wouldblock_if_lock_held_elsewhere() {
        let dir = std::env::temp_dir().join(format!(
            "typra-storage-test-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let db_path = dir.join("db.typra");
        std::fs::write(&db_path, b"").unwrap();

        let lock_path = FileStore::lock_path_for_db_path(&db_path);
        let lock_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .unwrap();
        lock_file.try_lock_exclusive().unwrap();

        let err = FileStore::open_locked(&db_path, OpenMode::ReadOnly).unwrap_err();
        assert!(matches!(
            err,
            DbError::Io(ref e) if e.kind() == std::io::ErrorKind::WouldBlock
        ));
    }
}

impl FileStore {
    pub fn new(file: File) -> Self {
        Self {
            inner: crate::pager::PagedStore::new(
                RawFileStore::new(file),
                crate::pager::DEFAULT_PAGE_SIZE,
            ),
            _writer_lock: None,
            _reader_lock: None,
        }
    }

    fn lock_path_for_db_path(db_path: &Path) -> PathBuf {
        // Sidecar lock file so writers can exclude other writers while read-only opens proceed.
        // This is advisory and best-effort; platforms differ in exact semantics.
        PathBuf::from(format!("{}.writer.lock", db_path.display()))
    }

    /// Open a file store and acquire the process-level lock for the database path.
    ///
    /// Locking policy (cross-process):
    /// - `ReadWrite`: takes an **exclusive** advisory lock on the sidecar file
    ///   `<db_path>.writer.lock` (fail-fast; does not block indefinitely).
    /// - `ReadOnly`: opens the database read-only and takes a **shared** advisory lock on the same
    ///   sidecar file. This prevents new writers from opening while readers are active.
    ///
    /// This excludes concurrent writers, but does not prevent read-only opens while a writer is
    /// active. Callers that require stronger coordination should implement it at a higher layer.
    pub fn open_locked(path: impl AsRef<Path>, mode: OpenMode) -> Result<Self, DbError> {
        use fs2::FileExt;

        let path = path.as_ref();
        let file = match mode {
            OpenMode::ReadWrite => std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(false)
                .open(path)?,
            OpenMode::ReadOnly => std::fs::OpenOptions::new().read(true).open(path)?,
        };

        let lock_path = Self::lock_path_for_db_path(path);

        let writer_lock = match mode {
            OpenMode::ReadOnly => None,
            OpenMode::ReadWrite => {
                let mut g = writer_locks()
                    .lock()
                    .map_err(|_| std::io::Error::other("lock poisoned"))?;
                if let Some(st) = g.get_mut(&lock_path) {
                    st.refs = st.refs.saturating_add(1);
                    Some(WriterLockGuard {
                        lock_path: lock_path.clone(),
                    })
                } else {
                    let lock_file = std::fs::OpenOptions::new()
                        .read(true)
                        .write(true)
                        .create(true)
                        .truncate(false)
                        .open(&lock_path)?;
                    // Fail fast: do not block indefinitely.
                    lock_file.try_lock_exclusive()?;
                    g.insert(
                        lock_path.clone(),
                        WriterLockState {
                            _file: lock_file,
                            refs: 1,
                        },
                    );
                    Some(WriterLockGuard {
                        lock_path: lock_path.clone(),
                    })
                }
            }
        };

        let reader_lock = match mode {
            OpenMode::ReadWrite => None,
            OpenMode::ReadOnly => {
                // Always attempt a shared lock for read-only opens so readers block new writers.
                //
                // Important: on some platforms, acquiring a second lock in the same process while
                // an exclusive lock is held may downgrade/replace the existing lock. We avoid that
                // foot-gun by failing explicitly if this process already holds the writer lock.
                let already_writer = writer_locks()
                    .lock()
                    .ok()
                    .and_then(|g| g.get(&lock_path).map(|_| ()))
                    .is_some();
                if already_writer {
                    return Err(DbError::Io(std::io::Error::other(
                        "cannot open read-only while holding writer lock in the same process",
                    )));
                }

                let lock_file = std::fs::OpenOptions::new()
                    .read(true)
                    .write(true)
                    .create(true)
                    .truncate(false)
                    .open(&lock_path)?;
                match lock_file.try_lock_shared() {
                    Ok(()) => Some(lock_file),
                    Err(std::fs::TryLockError::WouldBlock) | Err(std::fs::TryLockError::Error(_)) => {
                        return Err(DbError::Io(std::io::Error::new(
                            std::io::ErrorKind::WouldBlock,
                            "database is locked by another process",
                        )));
                    }
                }
            }
        };

        Ok(Self {
            inner: crate::pager::PagedStore::new(
                RawFileStore::new(file),
                crate::pager::DEFAULT_PAGE_SIZE,
            ),
            _writer_lock: writer_lock,
            _reader_lock: reader_lock,
        })
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
