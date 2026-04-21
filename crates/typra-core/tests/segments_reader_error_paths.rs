use typra_core::segments::header::{SegmentHeader, SegmentType};
use typra_core::segments::reader::scan_segments;
use typra_core::storage::{FileStore, Store};
use typra_core::DbError;

struct FailLenStore<S: Store> {
    inner: S,
}

impl<S: Store> Store for FailLenStore<S> {
    fn len(&self) -> Result<u64, DbError> {
        Err(DbError::NotImplemented)
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
}

struct FailReadOnNthStore<S: Store> {
    inner: S,
    fail_on_read_n: u64,
    reads: u64,
}

impl<S: Store> Store for FailReadOnNthStore<S> {
    fn len(&self) -> Result<u64, DbError> {
        self.inner.len()
    }
    fn read_exact_at(&mut self, offset: u64, buf: &mut [u8]) -> Result<(), DbError> {
        self.reads += 1;
        if self.reads == self.fail_on_read_n {
            return Err(DbError::NotImplemented);
        }
        self.inner.read_exact_at(offset, buf)
    }
    fn write_all_at(&mut self, offset: u64, buf: &[u8]) -> Result<(), DbError> {
        self.inner.write_all_at(offset, buf)
    }
    fn sync(&mut self) -> Result<(), DbError> {
        self.inner.sync()
    }
}

fn new_store_with_one_segment() -> FileStore {
    let f = tempfile::NamedTempFile::new().unwrap();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(f.path())
        .unwrap();
    let mut store = FileStore::new(file);
    let start = 128u64;
    store.write_all_at(start - 1, &[0u8]).unwrap();
    let payload = b"abc";
    let payload_crc32c = crc32c::crc32c(payload);
    let header = SegmentHeader {
        segment_type: SegmentType::Schema,
        payload_len: payload.len() as u64,
        payload_crc32c,
    }
    .encode();
    store.write_all_at(start, &header).unwrap();
    store
        .write_all_at(start + header.len() as u64, payload)
        .unwrap();
    store
}

#[test]
fn scan_segments_returns_error_when_len_fails() {
    let store = new_store_with_one_segment();
    let mut store = FailLenStore { inner: store };
    assert!(scan_segments(&mut store, 128).is_err());
}

#[test]
fn scan_segments_returns_error_when_read_fails_in_crc_loop() {
    // Force read failure on the payload read during CRC checking.
    let store = new_store_with_one_segment();
    // scan_segments does 1 read for the segment header, then 1+ reads for payload chunks.
    let mut store = FailReadOnNthStore {
        inner: store,
        fail_on_read_n: 2, // fail the payload read (second read_exact_at)
        reads: 0,
    };

    assert!(scan_segments(&mut store, 128).is_err());
}

