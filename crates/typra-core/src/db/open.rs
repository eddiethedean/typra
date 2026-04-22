use std::collections::HashMap;
use std::path::PathBuf;

use crate::error::{DbError, FormatError};
use crate::file_format::{decode_header, FileHeader, FILE_HEADER_SIZE};
use crate::manifest::decode_manifest_v0;
use crate::publish::append_manifest_and_publish;
use crate::segments::header::SegmentType;
use crate::segments::reader::{read_segment_header_at, scan_segments};
use crate::storage::Store;
use crate::superblock::{decode_superblock, Superblock, SUPERBLOCK_SIZE};

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

pub(crate) fn open_with_store<S: Store>(
    path: PathBuf,
    mut store: S,
) -> Result<Database<S>, DbError> {
    let len = store.len()?;
    let segment_start = (FILE_HEADER_SIZE + 2 * SUPERBLOCK_SIZE) as u64;

    let format_minor: u16;

    if len == 0 {
        let header = FileHeader::new_v0_5();
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
        } else if header.format_minor == 3 || header.format_minor == 4 || header.format_minor == 5 {
            if len < segment_start {
                return Err(DbError::Format(FormatError::TruncatedSuperblock {
                    got: len as usize,
                    expected: segment_start as usize,
                }));
            }
            let selected = read_and_select_superblock(&mut store)?;
            if selected.manifest_offset != 0 {
                let _ = read_manifest(&mut store, &selected);
            }
            if len > segment_start {
                let _ = scan_segments(&mut store, segment_start)?;
            }
            format_minor = header.format_minor;
        } else {
            return Err(DbError::Format(FormatError::UnsupportedVersion {
                major: header.format_major,
                minor: header.format_minor,
            }));
        }
    }

    let catalog = if len == 0 {
        crate::catalog::Catalog::default()
    } else {
        replay::load_catalog(&mut store, segment_start)?
    };

    let latest = if len == 0 {
        HashMap::new()
    } else {
        replay::load_latest_rows(&mut store, segment_start, &catalog)?
    };

    let db = Database {
        path,
        store,
        catalog,
        segment_start,
        format_minor,
        latest,
    };
    Ok(db)
}
