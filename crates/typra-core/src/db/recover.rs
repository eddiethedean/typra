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
        match header.segment_type {
            SegmentType::Checkpoint | SegmentType::Temp => {}
            _ => {
                if crc != header.payload_crc32c {
                    return Err(DbError::Format(FormatError::BadSegmentPayloadChecksum));
                }
            }
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
        let safe = match metas.last() {
            None => segment_start,
            Some(m) => m.offset + SEGMENT_HEADER_LEN as u64 + m.header.payload_len,
        };
        return Ok(match safe.cmp(&file_len) {
            std::cmp::Ordering::Less => (safe, Some("torn_tail_pre_v6")),
            std::cmp::Ordering::Equal | std::cmp::Ordering::Greater => (file_len, None),
        });
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
                let pt = match pending_txn_id {
                    None => {
                        return Err(DbError::Format(FormatError::InvalidTxnPayload {
                            message: "TxnCommit outside transaction".into(),
                        }))
                    }
                    Some(pt) => pt,
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
            SegmentType::Manifest | SegmentType::Checkpoint | SegmentType::Temp => {
                match txn_base {
                    None => safe_prefix_end = e,
                    Some(_) => {}
                }
            }
            SegmentType::Schema | SegmentType::Record | SegmentType::Index => {
                match txn_base {
                    None => safe_prefix_end = e,
                    Some(_) => {}
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

#[cfg(test)]
mod tests {
    use super::{scan_segments_allow_tail_tear, truncate_end_for_recovery};
    use crate::error::{DbError, FormatError};
    use crate::file_format::FORMAT_MINOR_V6;
    use crate::segments::header::{SegmentHeader, SegmentType};
    use crate::segments::writer::SegmentWriter;
    use crate::storage::{Store, VecStore};
    use crate::txn::encode_txn_payload_v0;

    #[test]
    fn scan_errors_on_bad_crc_for_non_checkpoint_non_temp() {
        let mut store = VecStore::new();
        // Write a single Schema segment with a bad payload CRC.
        let hdr = SegmentHeader {
            segment_type: SegmentType::Schema,
            payload_len: 1,
            payload_crc32c: 123,
        }
        .encode();
        store.write_all_at(0, &hdr).unwrap();
        store.write_all_at(32, &[0xAA]).unwrap();

        let e = scan_segments_allow_tail_tear(&mut store, 0).unwrap_err();
        assert!(matches!(
            e,
            DbError::Format(FormatError::BadSegmentPayloadChecksum)
        ));
    }

    #[test]
    fn scan_allows_bad_crc_for_checkpoint_and_temp_segments() {
        let mut store = VecStore::new();
        // Checkpoint with bad CRC should be tolerated by scan.
        let hdr_ckpt = SegmentHeader {
            segment_type: SegmentType::Checkpoint,
            payload_len: 1,
            payload_crc32c: 123,
        }
        .encode();
        store.write_all_at(0, &hdr_ckpt).unwrap();
        store.write_all_at(32, &[0xAA]).unwrap();

        // Temp with bad CRC should also be tolerated.
        let hdr_temp = SegmentHeader {
            segment_type: SegmentType::Temp,
            payload_len: 1,
            payload_crc32c: 456,
        }
        .encode();
        store.write_all_at(33, &hdr_temp).unwrap();
        store.write_all_at(33 + 32, &[0xBB]).unwrap();

        let metas = scan_segments_allow_tail_tear(&mut store, 0).unwrap();
        assert_eq!(metas.len(), 2);
        assert_eq!(metas[0].header.segment_type, SegmentType::Checkpoint);
        assert_eq!(metas[1].header.segment_type, SegmentType::Temp);
    }

    #[test]
    fn scan_accepts_correct_crc_for_non_checkpoint_non_temp_segments() {
        let mut store = VecStore::new();
        let mut w = SegmentWriter::new(&mut store, 0);
        w.append(
            SegmentHeader {
                segment_type: SegmentType::Schema,
                payload_len: 0,
                payload_crc32c: 0,
            },
            &[1, 2, 3],
        )
        .unwrap();
        let metas = scan_segments_allow_tail_tear(&mut store, 0).unwrap();
        assert_eq!(metas.len(), 1);
        assert_eq!(metas[0].header.segment_type, SegmentType::Schema);
    }

    #[test]
    fn recovery_pre_v6_reports_torn_tail_when_extra_bytes_present() {
        let mut store = VecStore::new();
        let segment_start = 0u64;
        let mut w = SegmentWriter::new(&mut store, segment_start);
        w.append(
            SegmentHeader {
                segment_type: SegmentType::Schema,
                payload_len: 0,
                payload_crc32c: 0,
            },
            &[1, 2, 3],
        )
        .unwrap();
        // Add extra bytes that are not a full segment header.
        let end = store.len().unwrap();
        store.write_all_at(end, &[9, 9, 9]).unwrap();

        let (safe, reason) = truncate_end_for_recovery(&mut store, segment_start, 5).unwrap();
        assert!(safe < store.len().unwrap());
        assert_eq!(reason, Some("torn_tail_pre_v6"));
    }

    #[test]
    fn recovery_pre_v6_returns_file_len_when_clean() {
        let mut store = VecStore::new();
        let segment_start = 0u64;
        let mut w = SegmentWriter::new(&mut store, segment_start);
        w.append(
            SegmentHeader {
                segment_type: SegmentType::Schema,
                payload_len: 0,
                payload_crc32c: 0,
            },
            &[1, 2, 3],
        )
        .unwrap();

        let file_len = store.len().unwrap();
        let (safe, reason) = truncate_end_for_recovery(&mut store, segment_start, 5).unwrap();
        assert_eq!(safe, file_len);
        assert_eq!(reason, None);
    }

    #[test]
    fn recovery_pre_v6_reports_torn_tail_when_no_full_segments_present() {
        // file_len > segment_start, but not enough bytes for even one header => metas empty
        let mut store = VecStore::from_vec(vec![0u8; 10]);
        let (safe, reason) = truncate_end_for_recovery(&mut store, 0, 5).unwrap();
        assert_eq!(safe, 0);
        assert_eq!(reason, Some("torn_tail_pre_v6"));
    }

    #[test]
    fn recovery_pre_v6_empty_file_is_clean() {
        let mut store = VecStore::new();
        let (safe, reason) = truncate_end_for_recovery(&mut store, 0, 5).unwrap();
        assert_eq!(safe, 0);
        assert_eq!(reason, None);
    }

    #[test]
    fn recovery_pre_v6_segment_start_beyond_eof_is_clean() {
        let mut store = VecStore::new();
        let (safe, reason) = truncate_end_for_recovery(&mut store, 100, 5).unwrap();
        assert_eq!(safe, 0);
        assert_eq!(reason, None);
    }

    #[test]
    fn recovery_v6_advances_safe_prefix_for_manifest_outside_transaction() {
        let mut store = VecStore::new();
        let mut w = SegmentWriter::new(&mut store, 0);
        w.append(
            SegmentHeader {
                segment_type: SegmentType::Manifest,
                payload_len: 0,
                payload_crc32c: 0,
            },
            &[9, 8, 7],
        )
        .unwrap();

        let (safe, reason) = truncate_end_for_recovery(&mut store, 0, FORMAT_MINOR_V6).unwrap();
        assert_eq!(reason, None);
        assert_eq!(safe, store.len().unwrap());
    }

    #[test]
    fn recovery_v6_reports_torn_tail_when_extra_bytes_present() {
        let mut store = VecStore::new();
        let segment_start = 0u64;
        let mut w = SegmentWriter::new(&mut store, segment_start);
        let begin = encode_txn_payload_v0(1);
        let commit = encode_txn_payload_v0(1);
        w.append(
            SegmentHeader {
                segment_type: SegmentType::TxnBegin,
                payload_len: 0,
                payload_crc32c: 0,
            },
            begin.as_slice(),
        )
        .unwrap();
        w.append(
            SegmentHeader {
                segment_type: SegmentType::TxnCommit,
                payload_len: 0,
                payload_crc32c: 0,
            },
            commit.as_slice(),
        )
        .unwrap();
        let end = store.len().unwrap();
        store.write_all_at(end, &[0xFE, 0xED]).unwrap();

        let (safe, reason) =
            truncate_end_for_recovery(&mut store, segment_start, FORMAT_MINOR_V6).unwrap();
        assert_eq!(reason, Some("torn_tail"));
        assert!(safe < store.len().unwrap());
    }

    #[test]
    fn recovery_v6_updates_safe_prefix_only_outside_transactions() {
        // This targets the `Manifest | Checkpoint | Temp` and `Schema | Record | Index` arms where
        // `txn_base.is_none()` gates `safe_prefix_end` updates.
        let mut store = VecStore::new();
        let segment_start = 0u64;
        let mut w = SegmentWriter::new(&mut store, segment_start);

        // A schema segment before any transaction counts toward the safe prefix.
        w.append(
            SegmentHeader {
                segment_type: SegmentType::Schema,
                payload_len: 0,
                payload_crc32c: 0,
            },
            &[1, 2, 3],
        )
        .unwrap();
        let after_schema = w.offset();

        // Begin a transaction; safe prefix rewinds to the begin offset.
        let begin = encode_txn_payload_v0(9);
        let begin_off = w
            .append(
                SegmentHeader {
                    segment_type: SegmentType::TxnBegin,
                    payload_len: 0,
                    payload_crc32c: 0,
                },
                begin.as_slice(),
            )
            .unwrap();

        // A manifest segment inside a transaction must NOT advance the safe prefix.
        w.append(
            SegmentHeader {
                segment_type: SegmentType::Manifest,
                payload_len: 0,
                payload_crc32c: 0,
            },
            &[4, 5, 6],
        )
        .unwrap();

        // Commit closes the transaction and advances safe prefix to committed end.
        let commit = encode_txn_payload_v0(9);
        w.append(
            SegmentHeader {
                segment_type: SegmentType::TxnCommit,
                payload_len: 0,
                payload_crc32c: 0,
            },
            commit.as_slice(),
        )
        .unwrap();

        let (safe, reason) =
            truncate_end_for_recovery(&mut store, segment_start, FORMAT_MINOR_V6).unwrap();
        assert_eq!(reason, None);
        assert!(safe >= after_schema);
        assert!(safe > begin_off);
    }

    #[test]
    fn recovery_v6_does_not_advance_safe_prefix_for_schema_inside_transaction() {
        let mut store = VecStore::new();
        let segment_start = 0u64;
        let mut w = SegmentWriter::new(&mut store, segment_start);

        let begin = encode_txn_payload_v0(1);
        let begin_off = w
            .append(
                SegmentHeader {
                    segment_type: SegmentType::TxnBegin,
                    payload_len: 0,
                    payload_crc32c: 0,
                },
                begin.as_slice(),
            )
            .unwrap();

        // Schema inside a transaction should not advance safe_prefix_end.
        w.append(
            SegmentHeader {
                segment_type: SegmentType::Schema,
                payload_len: 0,
                payload_crc32c: 0,
            },
            &[1, 2, 3],
        )
        .unwrap();

        // Aborting closes the transaction and advances safe prefix to end of abort.
        let abort = encode_txn_payload_v0(1);
        w.append(
            SegmentHeader {
                segment_type: SegmentType::TxnAbort,
                payload_len: 0,
                payload_crc32c: 0,
            },
            abort.as_slice(),
        )
        .unwrap();

        let (safe, reason) =
            truncate_end_for_recovery(&mut store, segment_start, FORMAT_MINOR_V6).unwrap();
        assert_eq!(reason, None);
        assert!(safe > begin_off);
    }

    #[test]
    fn recovery_v6_rejects_nested_txn_begin() {
        let mut store = VecStore::new();
        let segment_start = 0u64;
        let mut w = SegmentWriter::new(&mut store, segment_start);
        let begin = encode_txn_payload_v0(1);
        w.append(
            SegmentHeader {
                segment_type: SegmentType::TxnBegin,
                payload_len: 0,
                payload_crc32c: 0,
            },
            begin.as_slice(),
        )
        .unwrap();
        w.append(
            SegmentHeader {
                segment_type: SegmentType::TxnBegin,
                payload_len: 0,
                payload_crc32c: 0,
            },
            begin.as_slice(),
        )
        .unwrap();

        let e = truncate_end_for_recovery(&mut store, segment_start, FORMAT_MINOR_V6).unwrap_err();
        assert!(matches!(
            e,
            DbError::Format(FormatError::InvalidTxnPayload { .. })
        ));
    }

    #[test]
    fn recovery_v6_rejects_commit_outside_transaction() {
        let mut store = VecStore::new();
        let segment_start = 0u64;
        let mut w = SegmentWriter::new(&mut store, segment_start);
        let commit = encode_txn_payload_v0(1);
        w.append(
            SegmentHeader {
                segment_type: SegmentType::TxnCommit,
                payload_len: 0,
                payload_crc32c: 0,
            },
            commit.as_slice(),
        )
        .unwrap();

        let e = truncate_end_for_recovery(&mut store, segment_start, FORMAT_MINOR_V6).unwrap_err();
        assert!(matches!(
            e,
            DbError::Format(FormatError::InvalidTxnPayload { .. })
        ));
    }

    #[test]
    fn recovery_v6_rejects_commit_id_mismatch() {
        let mut store = VecStore::new();
        let segment_start = 0u64;
        let mut w = SegmentWriter::new(&mut store, segment_start);
        let begin = encode_txn_payload_v0(1);
        let commit = encode_txn_payload_v0(2);
        w.append(
            SegmentHeader {
                segment_type: SegmentType::TxnBegin,
                payload_len: 0,
                payload_crc32c: 0,
            },
            begin.as_slice(),
        )
        .unwrap();
        w.append(
            SegmentHeader {
                segment_type: SegmentType::TxnCommit,
                payload_len: 0,
                payload_crc32c: 0,
            },
            commit.as_slice(),
        )
        .unwrap();

        let e = truncate_end_for_recovery(&mut store, segment_start, FORMAT_MINOR_V6).unwrap_err();
        assert!(matches!(
            e,
            DbError::Format(FormatError::InvalidTxnPayload { .. })
        ));
    }
}
