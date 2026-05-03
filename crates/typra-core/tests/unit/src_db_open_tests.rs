    use super::*;
    use crate::segments::header::SEGMENT_HEADER_LEN;
    use crate::segments::header::SegmentHeader;
    use crate::segments::writer::SegmentWriter;
    use crate::storage::{Store, VecStore};

    #[test]
    fn try_load_checkpoint_state_returns_none_for_out_of_range_and_wrong_type() {
        let segment_start = (FILE_HEADER_SIZE + 2 * SUPERBLOCK_SIZE) as u64;

        // Out-of-range checkpoint offset (< segment_start).
        let mut store = VecStore::from_vec(vec![0u8; (segment_start + 16) as usize]);
        let mut sb = Superblock::empty();
        sb.checkpoint_offset = segment_start - 1;
        sb.checkpoint_len = 1;
        assert!(try_load_checkpoint_state(&mut store, &sb, segment_start)
            .unwrap()
            .is_none());

        // Wrong segment type at checkpoint offset.
        let mut store2 = VecStore::new();
        store2.truncate(segment_start + 1024).unwrap();
        let header = SegmentHeader {
            segment_type: SegmentType::Temp,
            payload_len: 0,
            payload_crc32c: 0,
        };
        store2
            .write_all_at(segment_start, &header.encode())
            .unwrap();
        let mut sb2 = Superblock::empty();
        sb2.checkpoint_offset = segment_start;
        sb2.checkpoint_len = 1;
        assert!(try_load_checkpoint_state(&mut store2, &sb2, segment_start)
            .unwrap()
            .is_none());
    }

    #[test]
    fn try_load_checkpoint_state_rejects_bad_checksum() {
        let segment_start = (FILE_HEADER_SIZE + 2 * SUPERBLOCK_SIZE) as u64;
        let mut store = VecStore::new();
        store.truncate(segment_start + 4096).unwrap();

        let payload = vec![1u8, 2, 3, 4];
        let header = SegmentHeader {
            segment_type: SegmentType::Checkpoint,
            payload_len: payload.len() as u64,
            payload_crc32c: 0, // wrong on purpose
        };
        store.write_all_at(segment_start, &header.encode()).unwrap();
        store
            .write_all_at(segment_start + SEGMENT_HEADER_LEN as u64, &payload)
            .unwrap();

        let mut sb = Superblock::empty();
        sb.checkpoint_offset = segment_start;
        sb.checkpoint_len = payload.len() as u32;
        let err = try_load_checkpoint_state(&mut store, &sb, segment_start).unwrap_err();
        assert!(matches!(
            err,
            DbError::Format(FormatError::BadSegmentPayloadChecksum)
        ));
    }

    #[test]
    fn strict_open_rejects_torn_tail_as_unclean_log_tail() {
        let segment_start = (FILE_HEADER_SIZE + 2 * SUPERBLOCK_SIZE) as u64;

        // Create a minimal valid DB image (format minor v6).
        let mut store = VecStore::new();
        let header = FileHeader::new_v0_8();
        store.write_all_at(0, &header.encode()).unwrap();
        init_superblocks(&mut store, segment_start).unwrap();
        let _ = append_manifest_and_publish(&mut store, segment_start).unwrap();

        // Append one byte of garbage past the committed prefix.
        let mut bytes = store.into_inner();
        bytes.push(0xAA);
        let store2 = VecStore::from_vec(bytes);

        let err = open_with_store(
            PathBuf::from(":memory:"),
            store2,
            OpenOptions {
                recovery: RecoveryMode::Strict,
                mode: crate::config::OpenMode::ReadWrite,
            },
        )
        .err()
        .expect("expected strict open to error");
        assert!(matches!(err, DbError::Format(FormatError::UncleanLogTail { .. })));
    }

    #[test]
    fn auto_truncate_falls_back_from_bad_checkpoint_and_can_replay_tail_after_good_checkpoint() {
        let segment_start = (FILE_HEADER_SIZE + 2 * SUPERBLOCK_SIZE) as u64;

        // --- 1) Bad checkpoint: try_load_checkpoint_state errors, AutoTruncate falls back to replay.
        let mut store = VecStore::new();
        let header = FileHeader::new_v0_8();
        store.write_all_at(0, &header.encode()).unwrap();
        init_superblocks(&mut store, segment_start).unwrap();

        // Append a checkpoint segment with a deliberately bad CRC.
        let file_len = store.len().unwrap();
        let mut w = SegmentWriter::new(&mut store, file_len.max(segment_start));
        let checkpoint_offset = w.offset();
        let payload = vec![1u8, 2, 3, 4];
        let hdr = SegmentHeader {
            segment_type: SegmentType::Checkpoint,
            payload_len: payload.len() as u64,
            payload_crc32c: 0, // wrong on purpose
        };
        w.append(hdr, &payload).unwrap();

        let _ = crate::publish::append_manifest_and_publish_with_checkpoint(
            &mut store,
            segment_start,
            Some((checkpoint_offset, payload.len() as u32)),
        )
        .unwrap();
        store.sync().unwrap();

        // Open should succeed via replay fallback (not checkpoint).
        let _db = open_with_store(PathBuf::from(":memory:"), store, OpenOptions::default()).unwrap();

        // --- 2) Good checkpoint + tail: open loads checkpoint, then replays tail segment.
        let mut store2 = VecStore::new();
        let header2 = FileHeader::new_v0_8();
        store2.write_all_at(0, &header2.encode()).unwrap();
        init_superblocks(&mut store2, segment_start).unwrap();

        let file_len2 = store2.len().unwrap();
        let mut w2 = SegmentWriter::new(&mut store2, file_len2.max(segment_start));
        let checkpoint_offset2 = w2.offset();

        let mut cp = crate::checkpoint::checkpoint_from_state(
            &crate::catalog::Catalog::default(),
            &std::collections::HashMap::new(),
            &crate::index::IndexState::default(),
        )
        .unwrap();
        // Fill replay_from so open knows where to start tail replay.
        let payload2_len = crate::checkpoint::encode_checkpoint_payload_v0(&cp).len() as u64;
        let replay_from = checkpoint_offset2 + SEGMENT_HEADER_LEN as u64 + payload2_len;
        cp.replay_from_offset = replay_from;
        let payload2 = crate::checkpoint::encode_checkpoint_payload_v0(&cp);
        let crc = crate::checksum::crc32c(&payload2);
        let hdr2 = SegmentHeader {
            segment_type: SegmentType::Checkpoint,
            payload_len: payload2.len() as u64,
            payload_crc32c: crc,
        };
        w2.append(hdr2, &payload2).unwrap();

        // Append one Temp segment after the checkpoint so replay_tail_into is exercised.
        let _ = w2
            .append(
                SegmentHeader {
                    segment_type: SegmentType::Temp,
                    payload_len: 0,
                    payload_crc32c: 0,
                },
                b"x",
            )
            .unwrap();

        let _ = crate::publish::append_manifest_and_publish_with_checkpoint(
            &mut store2,
            segment_start,
            Some((checkpoint_offset2, payload2.len() as u32)),
        )
        .unwrap();
        store2.sync().unwrap();

        let _db2 =
            open_with_store(PathBuf::from(":memory:"), store2, OpenOptions::default()).unwrap();
    }

    #[test]
    fn open_with_store_flen_zero_branch_is_coverable_with_lying_store() {
        #[derive(Debug)]
        struct ZeroLenStore {
            inner: VecStore,
        }

        impl Store for ZeroLenStore {
            fn len(&self) -> Result<u64, DbError> {
                Ok(0)
            }
            fn read_exact_at(&mut self, offset: u64, buf: &mut [u8]) -> Result<(), DbError> {
                self.inner.read_exact_at(offset, buf)
            }
            fn write_all_at(&mut self, offset: u64, buf: &[u8]) -> Result<(), DbError> {
                self.inner.write_all_at(offset, buf)
            }
            fn sync(&mut self) -> Result<(), DbError> {
                Ok(())
            }
            fn truncate(&mut self, _len: u64) -> Result<(), DbError> {
                Ok(())
            }
        }

        let mut store = ZeroLenStore { inner: VecStore::new() };
        // Cover the `truncate` stub too (the open path doesn't call it for flen == 0).
        store.truncate(0).unwrap();
        let db = open_with_store(PathBuf::from(":memory:"), store, OpenOptions::default()).unwrap();
        // If we reached here, we executed the `flen == 0` initialization tuple branch.
        assert_eq!(db.segment_start, (FILE_HEADER_SIZE + 2 * SUPERBLOCK_SIZE) as u64);
    }

    #[test]
    fn open_with_store_returns_error_when_store_len_fails() {
        #[derive(Debug)]
        struct LenErr;

        impl Store for LenErr {
            fn len(&self) -> Result<u64, DbError> {
                Err(DbError::NotImplemented)
            }
            fn read_exact_at(&mut self, _offset: u64, _buf: &mut [u8]) -> Result<(), DbError> {
                Ok(())
            }
            fn write_all_at(&mut self, _offset: u64, _buf: &[u8]) -> Result<(), DbError> {
                Ok(())
            }
            fn sync(&mut self) -> Result<(), DbError> {
                Ok(())
            }
            fn truncate(&mut self, _len: u64) -> Result<(), DbError> {
                Ok(())
            }
        }

        let err = match open_with_store(PathBuf::from(":memory:"), LenErr, OpenOptions::default()) {
            Err(e) => e,
            Ok(_) => panic!("expected open to fail when store.len errors"),
        };
        assert!(matches!(err, DbError::NotImplemented));
    }
