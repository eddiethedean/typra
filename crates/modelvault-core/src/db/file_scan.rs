//! Read-only on-disk inspection: header, superblock selection, segment scan, catalog decode.

use std::path::Path;

use crate::catalog::{decode_catalog_payload, Catalog};
use crate::error::{DbError, FormatError};
use crate::file_format::{decode_header, FileHeader, FILE_HEADER_SIZE};
use crate::segments::header::SegmentType;
use crate::segments::reader::{read_segment_payload, scan_segments, SegmentMeta};
use crate::storage::{FileStore, Store};
use crate::superblock::{decode_superblock, Superblock, SUPERBLOCK_SIZE};

/// Byte offset where the append-only segment region begins (after header + dual superblocks).
pub const SEGMENT_REGION_START: u64 = (FILE_HEADER_SIZE + 2 * SUPERBLOCK_SIZE) as u64;

/// Controls whether segment framing / catalog decode runs during [`scan_database_file`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DatabaseScanMode {
    /// Like CLI `inspect`: skip segment scan when no valid superblock is selected.
    Inspect,
    /// Like CLI `verify`: always require the segment region and run a full segment scan.
    Verify,
}

/// Result of a read-only file scan (header, superblock, segments, decoded catalog).
#[derive(Debug, Clone)]
pub struct DatabaseFileScan {
    pub header: FileHeader,
    pub superblock: Option<Superblock>,
    pub segments: Vec<SegmentMeta>,
    pub catalog: Catalog,
}

/// Open `path` read-only and scan header, superblocks, and (when applicable) segments.
pub fn scan_database_file(
    path: impl AsRef<Path>,
    mode: DatabaseScanMode,
) -> Result<DatabaseFileScan, DbError> {
    let f = std::fs::OpenOptions::new().read(true).open(path.as_ref())?;
    let mut store = FileStore::new(f);
    scan_database_store(&mut store, mode)
}

/// Scan an already-open read-only [`FileStore`].
pub fn scan_database_store(
    store: &mut FileStore,
    mode: DatabaseScanMode,
) -> Result<DatabaseFileScan, DbError> {
    let (header, sb_a, sb_b) = read_header_and_superblocks(store)?;
    let superblock = select_superblock(&sb_a, &sb_b);

    let should_scan = match mode {
        DatabaseScanMode::Inspect => superblock.is_some(),
        DatabaseScanMode::Verify => true,
    };

    let (segments, catalog) = if should_scan {
        ensure_segment_region(store)?;
        let segments = scan_segments(store, SEGMENT_REGION_START)?;
        let catalog = load_catalog_from_schema_segments(store, &segments)?;
        (segments, catalog)
    } else {
        (Vec::new(), Catalog::default())
    };

    Ok(DatabaseFileScan {
        header,
        superblock,
        segments,
        catalog,
    })
}

fn ensure_segment_region(store: &mut impl Store) -> Result<(), DbError> {
    let len = store.len()?;
    if len < SEGMENT_REGION_START {
        return Err(DbError::Format(FormatError::TruncatedSuperblock {
            got: len as usize,
            expected: SEGMENT_REGION_START as usize,
        }));
    }
    Ok(())
}

/// Read and decode the fixed file header plus both redundant superblock slots.
pub fn read_header_and_superblocks(
    store: &mut impl Store,
) -> Result<(FileHeader, [u8; SUPERBLOCK_SIZE], [u8; SUPERBLOCK_SIZE]), DbError> {
    let len = store.len()?;
    if len < FILE_HEADER_SIZE as u64 {
        return Err(DbError::Format(FormatError::TruncatedHeader {
            got: len as usize,
            expected: FILE_HEADER_SIZE,
        }));
    }

    let mut hdr_buf = [0u8; FILE_HEADER_SIZE];
    store.read_exact_at(0, &mut hdr_buf)?;
    let header = decode_header(&hdr_buf)?;

    let mut a = [0u8; SUPERBLOCK_SIZE];
    let mut b = [0u8; SUPERBLOCK_SIZE];
    store.read_exact_at(FILE_HEADER_SIZE as u64, &mut a)?;
    store.read_exact_at((FILE_HEADER_SIZE + SUPERBLOCK_SIZE) as u64, &mut b)?;

    Ok((header, a, b))
}

/// Pick the newer valid superblock from the dual redundant slots.
pub fn select_superblock(
    a: &[u8; SUPERBLOCK_SIZE],
    b: &[u8; SUPERBLOCK_SIZE],
) -> Option<Superblock> {
    let sa = decode_superblock(a).ok();
    let sb = decode_superblock(b).ok();
    match (sa, sb) {
        (Some(sa), Some(sb)) => Some(if sa.generation >= sb.generation {
            sa
        } else {
            sb
        }),
        (Some(sa), None) => Some(sa),
        (None, Some(sb)) => Some(sb),
        (None, None) => None,
    }
}

fn load_catalog_from_schema_segments(
    store: &mut impl Store,
    metas: &[SegmentMeta],
) -> Result<Catalog, DbError> {
    let mut cat = Catalog::default();
    for meta in metas {
        if meta.header.segment_type != SegmentType::Schema {
            continue;
        }
        let payload = read_segment_payload(store, meta)?;
        let rec = decode_catalog_payload(&payload)?;
        cat.apply_record(rec)?;
    }
    Ok(cat)
}
