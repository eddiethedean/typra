    use super::*;
    use crate::segments::header::{SegmentHeader, SegmentType};
    use crate::segments::writer::SegmentWriter;
    use crate::storage::VecStore;
    use crate::txn::encode_txn_payload_v0;

    #[test]
    fn truncate_end_pre_v6_returns_torn_tail_reason() {
        let mut store = VecStore::new();
        let mut w = SegmentWriter::new(&mut store, 0);
        let _ = w
            .append(
                SegmentHeader {
                    segment_type: SegmentType::Temp,
                    payload_len: 0,
                    payload_crc32c: 0,
                },
                b"abc",
            )
            .unwrap();
        // Trailing garbage (incomplete next header) should be truncated.
        store.write_all_at(store.len().unwrap(), &[0xAA]).unwrap();

        let (safe_end, reason) = truncate_end_for_recovery(&mut store, 0, FORMAT_MINOR_V6 - 1).unwrap();
        assert!(safe_end < store.len().unwrap());
        assert_eq!(reason, Some("torn_tail_pre_v6"));
    }

    #[test]
    fn truncate_end_v6_errors_on_nested_begin_and_commit_outside_txn_and_mismatch() {
        // Nested TxnBegin.
        let mut store = VecStore::new();
        let mut w = SegmentWriter::new(&mut store, 0);
        let p1 = encode_txn_payload_v0(1);
        let p2 = encode_txn_payload_v0(2);
        let _ = w
            .append(
                SegmentHeader {
                    segment_type: SegmentType::TxnBegin,
                    payload_len: 0,
                    payload_crc32c: 0,
                },
                &p1,
            )
            .unwrap();
        let _ = w
            .append(
                SegmentHeader {
                    segment_type: SegmentType::TxnBegin,
                    payload_len: 0,
                    payload_crc32c: 0,
                },
                &p2,
            )
            .unwrap();
        let err = truncate_end_for_recovery(&mut store, 0, FORMAT_MINOR_V6).unwrap_err();
        assert!(matches!(err, DbError::Format(FormatError::InvalidTxnPayload { .. })));

        // TxnCommit outside transaction.
        let mut store2 = VecStore::new();
        let mut w2 = SegmentWriter::new(&mut store2, 0);
        let pc = encode_txn_payload_v0(1);
        let _ = w2
            .append(
                SegmentHeader {
                    segment_type: SegmentType::TxnCommit,
                    payload_len: 0,
                    payload_crc32c: 0,
                },
                &pc,
            )
            .unwrap();
        let err = truncate_end_for_recovery(&mut store2, 0, FORMAT_MINOR_V6).unwrap_err();
        assert!(matches!(err, DbError::Format(FormatError::InvalidTxnPayload { .. })));

        // Commit id mismatch.
        let mut store3 = VecStore::new();
        let mut w3 = SegmentWriter::new(&mut store3, 0);
        let pb = encode_txn_payload_v0(1);
        let pm = encode_txn_payload_v0(2);
        let _ = w3
            .append(
                SegmentHeader {
                    segment_type: SegmentType::TxnBegin,
                    payload_len: 0,
                    payload_crc32c: 0,
                },
                &pb,
            )
            .unwrap();
        let _ = w3
            .append(
                SegmentHeader {
                    segment_type: SegmentType::TxnCommit,
                    payload_len: 0,
                    payload_crc32c: 0,
                },
                &pm,
            )
            .unwrap();
        let err = truncate_end_for_recovery(&mut store3, 0, FORMAT_MINOR_V6).unwrap_err();
        assert!(matches!(err, DbError::Format(FormatError::InvalidTxnPayload { .. })));
    }

    #[test]
    fn truncate_end_v6_returns_base_for_uncommitted_txn() {
        let mut store = VecStore::new();
        let mut w = SegmentWriter::new(&mut store, 0);
        let pb = encode_txn_payload_v0(1);
        let begin_off = w
            .append(
                SegmentHeader {
                    segment_type: SegmentType::TxnBegin,
                    payload_len: 0,
                    payload_crc32c: 0,
                },
                &pb,
            )
            .unwrap();
        // Add an unframed committed-like segment inside txn.
        let _ = w
            .append(
                SegmentHeader {
                    segment_type: SegmentType::Schema,
                    payload_len: 0,
                    payload_crc32c: 0,
                },
                b"x",
            )
            .unwrap();

        let (safe_end, reason) = truncate_end_for_recovery(&mut store, 0, FORMAT_MINOR_V6).unwrap();
        assert_eq!(safe_end, begin_off);
        assert_eq!(reason, Some("uncommitted_transaction"));
    }

    #[test]
    fn scan_segments_allow_tail_tear_rejects_bad_checksum_for_non_temp_non_checkpoint() {
        let mut store = VecStore::new();
        let mut w = SegmentWriter::new(&mut store, 0);
        let _off = w
            .append(
                SegmentHeader {
                    segment_type: SegmentType::Schema,
                    payload_len: 0,
                    payload_crc32c: 0,
                },
                b"abc",
            )
            .unwrap();

        // Corrupt the stored header CRC so the scan detects it.
        let mut metas = scan_segments_allow_tail_tear(&mut store, 0).unwrap();
        let meta = metas.pop().unwrap();
        let mut hdr = meta.header;
        hdr.payload_crc32c = hdr.payload_crc32c.wrapping_add(1);
        store.write_all_at(meta.offset, &hdr.encode()).unwrap();

        let err = scan_segments_allow_tail_tear(&mut store, 0).unwrap_err();
        assert!(matches!(
            err,
            DbError::Format(FormatError::BadSegmentPayloadChecksum)
        ));
    }

    #[test]
    fn truncate_end_v6_abort_advances_safe_prefix_end() {
        let mut store = VecStore::new();
        let mut w = SegmentWriter::new(&mut store, 0);
        let pb = encode_txn_payload_v0(1);
        let _ = w
            .append(
                SegmentHeader {
                    segment_type: SegmentType::TxnBegin,
                    payload_len: 0,
                    payload_crc32c: 0,
                },
                &pb,
            )
            .unwrap();
        let _ = w
            .append(
                SegmentHeader {
                    segment_type: SegmentType::TxnAbort,
                    payload_len: 0,
                    payload_crc32c: 0,
                },
                &pb,
            )
            .unwrap();

        let (safe_end, reason) = truncate_end_for_recovery(&mut store, 0, FORMAT_MINOR_V6).unwrap();
        assert_eq!(reason, None);
        assert_eq!(safe_end, store.len().unwrap());
    }

    #[test]
    fn truncate_end_v6_schema_index_record_advance_safe_prefix_when_not_in_txn() {
        // Cover the `SegmentType::Schema|Record|Index` branch updating `safe_prefix_end` when
        // `txn_base.is_none()`.
        let mut store = VecStore::new();
        let mut w = SegmentWriter::new(&mut store, 0);
        let _ = w
            .append(
                SegmentHeader {
                    segment_type: SegmentType::Schema,
                    payload_len: 0,
                    payload_crc32c: 0,
                },
                b"a",
            )
            .unwrap();
        let _ = w
            .append(
                SegmentHeader {
                    segment_type: SegmentType::Index,
                    payload_len: 0,
                    payload_crc32c: 0,
                },
                b"b",
            )
            .unwrap();
        let _ = w
            .append(
                SegmentHeader {
                    segment_type: SegmentType::Record,
                    payload_len: 0,
                    payload_crc32c: 0,
                },
                b"c",
            )
            .unwrap();

        // Add trailing garbage to force "torn_tail" return.
        store.write_all_at(store.len().unwrap(), &[0xAA]).unwrap();
        let (safe_end, reason) = truncate_end_for_recovery(&mut store, 0, FORMAT_MINOR_V6).unwrap();
        assert_eq!(reason, Some("torn_tail"));
        assert!(safe_end < store.len().unwrap());
    }
