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
                let (cat, lat, idx) = replay::load_catalog_latest_and_indexes(
                    &mut store,
                    segment_start,
                    format_minor,
                )?;
                (cat, lat, idx, store.len()?)
            }
            Err(e) => match opts.recovery {
                RecoveryMode::Strict => return Err(e),
                RecoveryMode::AutoTruncate => {
                    // If the checkpoint pointer is torn or the payload is corrupt, fall back to
                    // full replay (recovery already truncated the log tail).
                    let (cat, lat, idx) =
                        replay::load_catalog_latest_and_indexes(&mut store, segment_start, format_minor)?;
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
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/unit/src_db_open_tests.rs"
    ));
}
