use crate::checksum::crc32c_append;
use crate::error::{DbError, FormatError};
use crate::segments::header::{decode_segment_header, SegmentHeader, SEGMENT_HEADER_LEN};
use crate::storage::Store;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SegmentMeta {
    pub offset: u64,
    pub header: SegmentHeader,
}

pub fn read_segment_header_at(
    store: &mut impl Store,
    offset: u64,
) -> Result<([u8; SEGMENT_HEADER_LEN], SegmentHeader), DbError> {
    let mut buf = [0u8; SEGMENT_HEADER_LEN];
    store.read_exact_at(offset, &mut buf)?;
    let header = decode_segment_header(&buf)?;
    Ok((buf, header))
}

pub fn scan_segments(store: &mut impl Store, start: u64) -> Result<Vec<SegmentMeta>, DbError> {
    let mut out = Vec::new();
    let mut offset = start;
    let file_len = store.len()?;

    while offset < file_len {
        if file_len - offset < SEGMENT_HEADER_LEN as u64 {
            return Err(DbError::Format(FormatError::TruncatedSegmentHeader {
                got: (file_len - offset) as usize,
                expected: SEGMENT_HEADER_LEN,
            }));
        }

        let (_, header) = read_segment_header_at(store, offset)?;
        let payload_start = offset + SEGMENT_HEADER_LEN as u64;
        let payload_end = payload_start + header.payload_len;
        if payload_end > file_len {
            return Err(DbError::Format(FormatError::SegmentPayloadPastEof));
        }

        // CRC check with bounded reads (no large allocations).
        let mut remaining = header.payload_len;
        let mut chunk = [0u8; 8192];
        let mut cursor = payload_start;
        let mut crc = 0u32;
        while remaining > 0 {
            let to_read = std::cmp::min(remaining as usize, chunk.len());
            store.read_exact_at(cursor, &mut chunk[..to_read])?;
            crc = crc32c_append(crc, &chunk[..to_read]);
            cursor += to_read as u64;
            remaining -= to_read as u64;
        }
        if crc != header.payload_crc32c {
            return Err(DbError::Format(FormatError::BadSegmentPayloadChecksum));
        }

        out.push(SegmentMeta { offset, header });
        offset = payload_end;
    }

    Ok(out)
}
