//! Compute a safe committed prefix end offset for crash recovery (format minor 6+).

use crate::error::{DbError, FormatError};
use crate::file_format::FORMAT_MINOR_V6;
use crate::segments::header::{SegmentType, SEGMENT_HEADER_LEN};
use crate::segments::reader::{read_segment_header_at, read_segment_payload, SegmentMeta};
use crate::storage::Store;
use crate::txn::decode_txn_payload_v0;

/// Scan from `start`, tolerating an incomplete trailing segment (drops trailing garbage).
pub(crate) fn scan_segments_allow_tail_tear(
    store: &mut impl Store,
    start: u64,
) -> Result<Vec<SegmentMeta>, DbError> {
    use crate::checksum::crc32c_append;

    let mut out = Vec::new();
    let mut offset = start;
    let file_len = store.len()?;

    while offset < file_len {
        if file_len - offset < SEGMENT_HEADER_LEN as u64 {
            break;
        }

        let (_, header) = read_segment_header_at(store, offset)?;
        let payload_start = offset + SEGMENT_HEADER_LEN as u64;
        let payload_end = payload_start + header.payload_len;
        if payload_end > file_len {
            break;
        }

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
        if header.segment_type != SegmentType::Checkpoint && crc != header.payload_crc32c {
            return Err(DbError::Format(FormatError::BadSegmentPayloadChecksum));
        }

        out.push(SegmentMeta { offset, header });
        offset = payload_end;
    }

    Ok(out)
}

/// First byte offset to **truncate away** (keep `[0, safe_end)`). If `safe_end == file_len`, nothing to drop.
pub(crate) fn truncate_end_for_recovery(
    store: &mut impl Store,
    segment_start: u64,
    format_minor: u16,
) -> Result<(u64, Option<&'static str>), DbError> {
    let file_len = store.len()?;
    let metas = scan_segments_allow_tail_tear(store, segment_start)?;

    if format_minor < FORMAT_MINOR_V6 {
        let safe = metas
            .last()
            .map(|m| m.offset + SEGMENT_HEADER_LEN as u64 + m.header.payload_len)
            .unwrap_or(segment_start);
        if safe < file_len {
            return Ok((safe, Some("torn_tail_pre_v6")));
        }
        return Ok((file_len, None));
    }

    let mut safe_prefix_end = segment_start;
    let mut txn_base: Option<u64> = None;
    let mut pending_txn_id: Option<u64> = None;

    for meta in &metas {
        let e = meta.offset + SEGMENT_HEADER_LEN as u64 + meta.header.payload_len;
        match meta.header.segment_type {
            SegmentType::TxnBegin => {
                if txn_base.is_some() {
                    return Err(DbError::Format(FormatError::InvalidTxnPayload {
                        message: "nested TxnBegin before TxnCommit".into(),
                    }));
                }
                let payload = read_segment_payload(store, meta)?;
                let id = decode_txn_payload_v0(&payload)?;
                txn_base = Some(meta.offset);
                pending_txn_id = Some(id);
                safe_prefix_end = meta.offset;
            }
            SegmentType::TxnCommit => {
                let payload = read_segment_payload(store, meta)?;
                let id = decode_txn_payload_v0(&payload)?;
                let Some(pt) = pending_txn_id else {
                    return Err(DbError::Format(FormatError::InvalidTxnPayload {
                        message: "TxnCommit outside transaction".into(),
                    }));
                };
                if id != pt {
                    return Err(DbError::Format(FormatError::InvalidTxnPayload {
                        message: "TxnCommit txn_id does not match TxnBegin".into(),
                    }));
                }
                txn_base = None;
                pending_txn_id = None;
                safe_prefix_end = e;
            }
            SegmentType::TxnAbort => {
                let _ = decode_txn_payload_v0(&read_segment_payload(store, meta)?)?;
                txn_base = None;
                pending_txn_id = None;
                safe_prefix_end = e;
            }
            SegmentType::Manifest | SegmentType::Checkpoint => {
                if txn_base.is_none() {
                    safe_prefix_end = e;
                }
            }
            SegmentType::Schema | SegmentType::Record | SegmentType::Index => {
                if txn_base.is_none() {
                    safe_prefix_end = e;
                }
            }
        }
    }

    if let Some(base) = txn_base {
        return Ok((base, Some("uncommitted_transaction")));
    }
    if safe_prefix_end < file_len {
        return Ok((safe_prefix_end, Some("torn_tail")));
    }
    Ok((file_len, None))
}
