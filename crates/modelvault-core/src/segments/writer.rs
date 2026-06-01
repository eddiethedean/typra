//! Append [`SegmentHeader`] + payload bytes to a [`crate::storage::Store`], maintaining a running write cursor.
//!
//! Used by publish and write paths to extend the segment log; header fields `payload_len` and
//! `payload_crc32c` are filled from `payload` before encoding.

use crate::checksum::crc32c;
use crate::error::DbError;
use crate::segments::header::SegmentHeader;
use crate::storage::Store;

/// Sequential segment writer: holds the store and the next byte offset to write.
pub struct SegmentWriter<'a, S: Store> {
    store: &'a mut S,
    offset: u64,
}

impl<'a, S: Store> SegmentWriter<'a, S> {
    /// Start writing at `offset` (usually current file length or `segment_start`).
    pub fn new(store: &'a mut S, offset: u64) -> Self {
        Self { store, offset }
    }

    /// Current end offset (next write starts here).
    pub fn offset(&self) -> u64 {
        self.offset
    }

    /// Write one segment: encoded header (with `payload_len` / CRC filled from `payload`), then `payload`.
    ///
    /// Returns the **start** offset of this segment (where the header began).
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
