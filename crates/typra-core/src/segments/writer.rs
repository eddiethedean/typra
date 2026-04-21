use crate::checksum::crc32c;
use crate::error::DbError;
use crate::segments::header::SegmentHeader;
use crate::storage::Store;

pub struct SegmentWriter<'a, S: Store> {
    store: &'a mut S,
    offset: u64,
}

impl<'a, S: Store> SegmentWriter<'a, S> {
    pub fn new(store: &'a mut S, offset: u64) -> Self {
        Self { store, offset }
    }

    pub fn offset(&self) -> u64 {
        self.offset
    }

    pub fn append(&mut self, header: SegmentHeader, payload: &[u8]) -> Result<u64, DbError> {
        let header = SegmentHeader {
            payload_len: payload.len() as u64,
            payload_crc32c: crc32c(payload),
            ..header
        };
        let encoded = header.encode();

        let start = self.offset;
        self.store.write_all_at(self.offset, &encoded)?;
        self.offset += encoded.len() as u64;
        self.store.write_all_at(self.offset, payload)?;
        self.offset += payload.len() as u64;

        Ok(start)
    }
}
