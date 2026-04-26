//! Spill manager for bounded-memory query operators (0.12.0+; stabilized in 0.13.0).
//!
//! v0 implementation: append ephemeral `Temp` segments to the store and truncate them away on drop.

use crate::error::DbError;
use crate::segments::header::{SegmentHeader, SegmentType, SEGMENT_HEADER_LEN};
use crate::segments::writer::SegmentWriter;
use crate::storage::Store;

/// Owned spill file wrapper that truncates to `base_len` on drop.
///
/// This is used by streaming query operators that need a scratch store.
pub struct TempSpillFile<S: Store> {
    store: Option<S>,
    base_len: u64,
}

impl<S: Store> TempSpillFile<S> {
    pub fn new(store: S) -> Result<Self, DbError> {
        let base_len = store.len()?;
        Ok(Self {
            store: Some(store),
            base_len,
        })
    }

    pub fn store_mut(&mut self) -> &mut S {
        self.store.as_mut().expect("spill store already taken")
    }

    pub fn append_temp_segment(&mut self, payload: &[u8]) -> Result<u64, DbError> {
        let store = self.store_mut();
        let file_len = store.len()?;
        let mut writer = SegmentWriter::new(store, file_len);
        let off = writer.offset();
        writer.append(
            SegmentHeader {
                segment_type: SegmentType::Temp,
                payload_len: 0,
                payload_crc32c: 0,
            },
            payload,
        )?;
        Ok(off)
    }

    pub fn read_temp_payload(&mut self, offset: u64, len: u64) -> Result<Vec<u8>, DbError> {
        let mut buf = vec![0u8; len as usize];
        self.store_mut()
            .read_exact_at(offset + SEGMENT_HEADER_LEN as u64, &mut buf)?;
        Ok(buf)
    }

    /// Explicitly truncate away all temp spill data and return the inner store.
    pub fn finish(mut self) -> Result<S, DbError> {
        let mut store = self.store.take().expect("spill store already taken");
        store.truncate(self.base_len)?;
        store.sync()?;
        Ok(store)
    }
}

impl<S: Store> Drop for TempSpillFile<S> {
    fn drop(&mut self) {
        if let Some(store) = self.store.as_mut() {
            let _ = store.truncate(self.base_len);
            let _ = store.sync();
        }
    }
}

/// RAII guard that truncates the store back to `base_len` on drop.
pub struct TempSpillGuard<'a, S: Store> {
    store: &'a mut S,
    base_len: u64,
}

impl<'a, S: Store> TempSpillGuard<'a, S> {
    pub fn new(store: &'a mut S) -> Result<Self, DbError> {
        let base_len = store.len()?;
        Ok(Self { store, base_len })
    }

    pub fn store_mut(&mut self) -> &mut S {
        self.store
    }

    /// Append one `Temp` segment and return its offset.
    pub fn append_temp_segment(&mut self, payload: &[u8]) -> Result<u64, DbError> {
        let file_len = self.store.len()?;
        let mut writer = SegmentWriter::new(self.store, file_len);
        let off = writer.offset();
        writer.append(
            SegmentHeader {
                segment_type: SegmentType::Temp,
                payload_len: 0,
                payload_crc32c: 0,
            },
            payload,
        )?;
        Ok(off)
    }

    pub fn read_temp_payload(&mut self, offset: u64, len: u64) -> Result<Vec<u8>, DbError> {
        let mut buf = vec![0u8; len as usize];
        self.store
            .read_exact_at(offset + SEGMENT_HEADER_LEN as u64, &mut buf)?;
        Ok(buf)
    }

    pub fn base_len(&self) -> u64 {
        self.base_len
    }
}

impl<S: Store> Drop for TempSpillGuard<'_, S> {
    fn drop(&mut self) {
        let _ = self.store.truncate(self.base_len);
        let _ = self.store.sync();
    }
}

#[cfg(test)]
mod tests {
    use super::TempSpillFile;
    use super::TempSpillGuard;
    use crate::storage::{Store, VecStore};

    #[test]
    fn temp_spill_file_truncates_on_drop() {
        let mut base = VecStore::new();
        base.write_all_at(0, &[1u8; 10]).unwrap();
        let base_len = base.len().unwrap();

        let mut spill = TempSpillFile::new(base).unwrap();
        spill.append_temp_segment(b"hello").unwrap();
        assert!(spill.store_mut().len().unwrap() > base_len);

        let base = spill.finish().unwrap();
        assert_eq!(base.len().unwrap(), base_len);
    }

    #[test]
    fn temp_spill_guard_appends_and_reads_payload_and_truncates() {
        let mut base = VecStore::new();
        base.write_all_at(0, &[2u8; 8]).unwrap();
        let base_len = base.len().unwrap();

        {
            let mut guard = TempSpillGuard::new(&mut base).unwrap();
            let off = guard.append_temp_segment(b"abc").unwrap();
            let got = guard.read_temp_payload(off, 3).unwrap();
            assert_eq!(got, b"abc");
            assert!(guard.store_mut().len().unwrap() > base_len);
        }

        // Dropping the guard truncates to the original length.
        assert_eq!(base.len().unwrap(), base_len);
    }

    #[test]
    #[should_panic(expected = "spill store already taken")]
    fn temp_spill_file_store_mut_panics_when_taken() {
        let spill: TempSpillFile<VecStore> = TempSpillFile {
            store: None,
            base_len: 0,
        };
        let mut spill = spill;
        let _ = spill.store_mut();
    }
}
