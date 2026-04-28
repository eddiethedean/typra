//! Open and bootstrap: decode header, superblocks, manifest, and initial segment scan.

use std::collections::HashMap;
use std::path::PathBuf;

use crate::config::{OpenOptions, RecoveryMode};
use crate::error::{DbError, FormatError};
use crate::file_format::{decode_header, FileHeader, FILE_HEADER_SIZE};
use crate::manifest::decode_manifest_v0;
use crate::publish::append_manifest_and_publish;
use crate::segments::header::SegmentType;
use crate::segments::reader::read_segment_header_at;
use crate::segments::reader::read_segment_payload;
use crate::storage::Store;
use crate::superblock::{decode_superblock, Superblock, SUPERBLOCK_SIZE};

use super::recover;
use super::replay;
use super::Database;

pub(crate) fn init_superblocks(store: &mut impl Store, segment_start: u64) -> Result<(), DbError> {
    store.write_all_at(segment_start - 1, &[0u8])?;

    let sb_a = Superblock {
        generation: 1,
        ..Superblock::empty()
    };
    let sb_b = Superblock::empty();
    store.write_all_at(FILE_HEADER_SIZE as u64, &sb_a.encode())?;
    store.write_all_at((FILE_HEADER_SIZE + SUPERBLOCK_SIZE) as u64, &sb_b.encode())?;
    Ok(())
}

pub(crate) fn read_and_select_superblock(store: &mut impl Store) -> Result<Superblock, DbError> {
    let mut a = [0u8; SUPERBLOCK_SIZE];
    let mut b = [0u8; SUPERBLOCK_SIZE];
    store.read_exact_at(FILE_HEADER_SIZE as u64, &mut a)?;
    store.read_exact_at((FILE_HEADER_SIZE + SUPERBLOCK_SIZE) as u64, &mut b)?;

    let sa = decode_superblock(&a).ok();
    let sb = decode_superblock(&b).ok();
    match (sa, sb) {
        (Some(sa), Some(sb)) => Ok(if sa.generation >= sb.generation {
            sa
        } else {
            sb
        }),
        (Some(sa), None) => Ok(sa),
        (None, Some(sb)) => Ok(sb),
        (None, None) => Err(DbError::Format(FormatError::BadSuperblockChecksum)),
    }
}

pub(crate) fn read_manifest(store: &mut impl Store, sb: &Superblock) -> Result<(), DbError> {
    let (_, header) = read_segment_header_at(store, sb.manifest_offset)?;
    if header.segment_type != SegmentType::Manifest {
        return Err(DbError::Format(FormatError::UnsupportedVersion {
            major: 0,
            minor: 0,
        }));
    }
    let mut payload = vec![0u8; header.payload_len as usize];
    store.read_exact_at(
        sb.manifest_offset + crate::segments::header::SEGMENT_HEADER_LEN as u64,
        &mut payload,
    )?;
    let _m = decode_manifest_v0(&payload)?;
    Ok(())
}

fn try_load_checkpoint_state(
    store: &mut impl Store,
    sb: &Superblock,
    segment_start: u64,
) -> Result<
    Option<(
        u64,
        crate::catalog::Catalog,
        super::LatestMap,
        crate::index::IndexState,
    )>,
    DbError,
> {
    if sb.checkpoint_offset == 0 || sb.checkpoint_len == 0 {
        return Ok(None);
    }
    let file_len = store.len()?;
    if sb.checkpoint_offset < segment_start || sb.checkpoint_offset >= file_len {
        return Ok(None);
    }

    let (_, header) = read_segment_header_at(store, sb.checkpoint_offset)?;
    if header.segment_type != SegmentType::Checkpoint {
        return Ok(None);
    }

    // Read checkpoint payload and decode.
    let meta = crate::segments::reader::SegmentMeta {
        offset: sb.checkpoint_offset,
        header,
    };
    let payload = read_segment_payload(store, &meta)?;
    let crc = crate::checksum::crc32c(&payload);
    if crc != header.payload_crc32c {
        return Err(DbError::Format(FormatError::BadSegmentPayloadChecksum));
    }
    let (replay_from, catalog, latest, indexes) =
        crate::checkpoint::state_from_checkpoint_payload(&payload)?;
    Ok(Some((replay_from, catalog, latest, indexes)))
}

pub(crate) fn open_with_store<S: Store>(
    path: PathBuf,
    mut store: S,
    opts: OpenOptions,
) -> Result<Database<S>, DbError> {
    #[cfg(feature = "tracing")]
    tracing::info!(path = %path.display(), "open_with_store_begin");
    let len = store.len()?;
    let segment_start = (FILE_HEADER_SIZE + 2 * SUPERBLOCK_SIZE) as u64;

    let format_minor: u16;

    if len == 0 {
        let header = FileHeader::new_v0_8();
        format_minor = header.format_minor;
        store.write_all_at(0, &header.encode())?;
        init_superblocks(&mut store, segment_start)?;
        let _ = append_manifest_and_publish(&mut store, segment_start)?;
        store.sync()?;
    } else if len < FILE_HEADER_SIZE as u64 {
        return Err(DbError::Format(FormatError::TruncatedHeader {
            got: len as usize,
            expected: FILE_HEADER_SIZE,
        }));
    } else {
        let mut buf = [0u8; FILE_HEADER_SIZE];
        store.read_exact_at(0, &mut buf)?;
        let header = decode_header(&buf)?;

        if header.format_minor == 2 {
            if len == FILE_HEADER_SIZE as u64 {
                let upgraded = FileHeader::new_v0_3();
                store.write_all_at(0, &upgraded.encode())?;
                init_superblocks(&mut store, segment_start)?;
                let _ = append_manifest_and_publish(&mut store, segment_start)?;
                store.sync()?;
                format_minor = crate::file_format::FORMAT_MINOR_V3;
            } else {
                return Err(DbError::Format(FormatError::UnsupportedVersion {
                    major: header.format_major,
                    minor: header.format_minor,
                }));
            }
        } else {
            // `decode_header` already validated we are in the supported range (2..=6), and the
            // `format_minor == 2` upgrade path is handled above. So the remaining case is 3..=6.
            if len < segment_start {
                return Err(DbError::Format(FormatError::TruncatedSuperblock {
                    got: len as usize,
                    expected: segment_start as usize,
                }));
            }
            let selected = read_and_select_superblock(&mut store)?;
            if selected.manifest_offset != 0 {
                if let Err(e) = read_manifest(&mut store, &selected) {
                    match opts.recovery {
                        RecoveryMode::Strict => return Err(e),
                        // Auto-truncation can recover from a torn manifest pointer/payload by
                        // scanning and truncating the log to a safe committed prefix.
                        RecoveryMode::AutoTruncate => {}
                    }
                }
            }
            format_minor = header.format_minor;

            let (truncate_to, reason) =
                recover::truncate_end_for_recovery(&mut store, segment_start, format_minor)?;
            match opts.recovery {
                RecoveryMode::Strict => {
                    if let Some(reason) = reason {
                        let flen = store.len()?;
                        if truncate_to < flen { return Err(DbError::Format(FormatError::UncleanLogTail { safe_end: truncate_to, reason })); }
                    }
                }
                RecoveryMode::AutoTruncate => {
                    let flen = store.len()?;
                    if truncate_to < flen {
                        store.truncate(truncate_to)?;
                        store.sync()?;
                    }
                }
            }
        }
    }

    let flen = store.len()?;
    let (mut catalog, mut latest, mut indexes, replay_from) = if flen == 0 {
        (
            crate::catalog::Catalog::default(),
            HashMap::new(),
            crate::index::IndexState::default(),
            segment_start,
        )
    } else {
        let selected = read_and_select_superblock(&mut store)?;
        match try_load_checkpoint_state(&mut store, &selected, segment_start) {
            Ok(Some((from, cat, lat, idx))) => (cat, lat, idx, from),
            Ok(None) => {
                let (cat, lat, idx) = replay::load_catalog_latest_and_indexes(&mut store, segment_start, format_minor)?;
                (cat, lat, idx, store.len()?)
            }
            Err(e) => match opts.recovery {
                RecoveryMode::Strict => return Err(e),
                RecoveryMode::AutoTruncate => {
                    // If the checkpoint pointer is torn or the payload is corrupt, fall back to
                    // full replay (recovery already truncated the log tail).
                    let (cat, lat, idx) = replay::load_catalog_latest_and_indexes(&mut store, segment_start, format_minor)?;
                    (cat, lat, idx, store.len()?)
                }
            },
        }
    };

    if flen != 0 {
        // Apply any tail segments that were written after the checkpoint. (When no checkpoint is
        // present, replay_from is set so this is a no-op.)
        if replay_from < store.len()? && replay_from >= segment_start {
            replay::replay_tail_into(&mut store, replay_from, format_minor, &mut catalog, &mut latest, &mut indexes)?;
        }
    }

    let db = Database {
        path,
        store,
        catalog,
        segment_start,
        format_minor,
        latest,
        indexes,
        txn_seq: 0,
        txn_staging: None,
    };
    #[cfg(feature = "tracing")]
    tracing::info!(path = %db.path.display(), format_minor = db.format_minor, "open_with_store_ok");
    Ok(db)
}

#[cfg(test)]
mod tests {
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
}
