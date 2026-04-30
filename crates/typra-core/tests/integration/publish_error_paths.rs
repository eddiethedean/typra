use typra_core::file_format::{FileHeader, FILE_HEADER_SIZE};
use typra_core::publish::append_manifest_and_publish;
use typra_core::storage::{FileStore, Store};
use typra_core::superblock::{Superblock, SUPERBLOCK_SIZE};
use typra_core::DbError;

struct FailingStore;

impl Store for FailingStore {
    fn len(&self) -> Result<u64, DbError> {
        Ok(0)
    }

    fn read_exact_at(&mut self, _offset: u64, _buf: &mut [u8]) -> Result<(), DbError> {
        Err(DbError::NotImplemented)
    }

    fn write_all_at(&mut self, _offset: u64, _buf: &[u8]) -> Result<(), DbError> {
        Err(DbError::NotImplemented)
    }

    fn sync(&mut self) -> Result<(), DbError> {
        Ok(())
    }

    fn truncate(&mut self, _len: u64) -> Result<(), DbError> {
        Ok(())
    }
}

#[test]
fn publish_returns_error_when_store_write_fails() {
    let mut store = FailingStore;
    let res = append_manifest_and_publish(&mut store, 0);
    assert!(res.is_err());
}

struct FailOnce<S: Store> {
    inner: S,
    fail_len: bool,
    fail_read: bool,
    fail_sync: bool,
}

impl<S: Store> Store for FailOnce<S> {
    fn len(&self) -> Result<u64, DbError> {
        if self.fail_len {
            return Err(DbError::NotImplemented);
        }
        self.inner.len()
    }

    fn read_exact_at(&mut self, offset: u64, buf: &mut [u8]) -> Result<(), DbError> {
        if self.fail_read {
            self.fail_read = false;
            return Err(DbError::NotImplemented);
        }
        self.inner.read_exact_at(offset, buf)
    }

    fn write_all_at(&mut self, offset: u64, buf: &[u8]) -> Result<(), DbError> {
        self.inner.write_all_at(offset, buf)
    }

    fn sync(&mut self) -> Result<(), DbError> {
        if self.fail_sync {
            return Err(DbError::NotImplemented);
        }
        self.inner.sync()
    }

    fn truncate(&mut self, len: u64) -> Result<(), DbError> {
        self.inner.truncate(len)
    }
}

fn new_filestore_with_superblocks() -> FileStore {
    let f = tempfile::NamedTempFile::new().unwrap();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(f.path())
        .unwrap();
    let mut store = FileStore::new(file);

    store
        .write_all_at(0, &FileHeader::new_v0_3().encode())
        .unwrap();
    let segment_start = (FILE_HEADER_SIZE + 2 * SUPERBLOCK_SIZE) as u64;
    store.write_all_at(segment_start - 1, &[0u8]).unwrap();
    store
        .write_all_at(
            FILE_HEADER_SIZE as u64,
            &Superblock {
                generation: 1,
                ..Superblock::empty()
            }
            .encode(),
        )
        .unwrap();
    store
        .write_all_at(
            (FILE_HEADER_SIZE + SUPERBLOCK_SIZE) as u64,
            &Superblock::empty().encode(),
        )
        .unwrap();

    store
}

#[test]
fn publish_returns_error_when_store_len_fails() {
    let store = new_filestore_with_superblocks();
    let mut store = FailOnce {
        inner: store,
        fail_len: true,
        fail_read: false,
        fail_sync: false,
    };
    let res = append_manifest_and_publish(&mut store, 0);
    assert!(res.is_err());
}

#[test]
fn publish_returns_error_when_superblock_read_fails() {
    let store = new_filestore_with_superblocks();
    let mut store = FailOnce {
        inner: store,
        fail_len: false,
        fail_read: true, // fail first read_exact_at (superblock read)
        fail_sync: false,
    };
    let res = append_manifest_and_publish(&mut store, 0);
    assert!(res.is_err());
}

#[test]
fn publish_returns_error_when_sync_fails() {
    let store = new_filestore_with_superblocks();
    let mut store = FailOnce {
        inner: store,
        fail_len: false,
        fail_read: false,
        fail_sync: true,
    };
    let res = append_manifest_and_publish(&mut store, 0);
    assert!(res.is_err());
}
