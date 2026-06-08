//! Coverage for [`modelvault_core::db::file_scan`] read-only inspection helpers.

use modelvault_core::db::{
    scan_database_file, scan_database_store, select_superblock, DatabaseScanMode,
    SEGMENT_REGION_START,
};
use modelvault_core::error::{DbError, FormatError};
use modelvault_core::file_format::{FileHeader, FILE_HEADER_SIZE};
use modelvault_core::storage::{FileStore, Store};
use modelvault_core::superblock::SUPERBLOCK_SIZE;
use modelvault_core::{Database, RowValue};
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fs::OpenOptions;

#[test]
fn scan_database_file_inspect_skips_segments_when_no_valid_superblock() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("no_sb.modelvault");
    let mut bytes = vec![0u8; SEGMENT_REGION_START as usize];
    bytes[0..FILE_HEADER_SIZE].copy_from_slice(&FileHeader::new_v0_8().encode());
    std::fs::write(&path, bytes).unwrap();

    let scan = scan_database_file(&path, DatabaseScanMode::Inspect).unwrap();
    assert!(scan.superblock.is_none());
    assert!(scan.segments.is_empty());
    assert!(scan.catalog.collections().is_empty());
}

#[test]
fn scan_database_file_verify_requires_segment_region() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("short.modelvault");
    let mut bytes = vec![0u8; FILE_HEADER_SIZE + SUPERBLOCK_SIZE];
    bytes[0..FILE_HEADER_SIZE].copy_from_slice(&FileHeader::new_v0_8().encode());
    std::fs::write(&path, bytes).unwrap();

    let err = scan_database_file(&path, DatabaseScanMode::Verify).unwrap_err();
    assert!(
        matches!(
            err,
            DbError::Format(FormatError::TruncatedSuperblock { .. }) | DbError::Io(_)
        ),
        "unexpected error: {err:?}"
    );
}

#[test]
fn scan_database_file_on_live_database_roundtrips_catalog() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("live.modelvault");
    {
        let mut db = Database::open(&path).unwrap();
        db.register_collection(
            "books",
            vec![modelvault_core::schema::FieldDef {
                path: modelvault_core::schema::FieldPath(vec![Cow::Borrowed("title")]),
                ty: modelvault_core::schema::Type::String,
                constraints: vec![],
            }],
            "title",
        )
        .unwrap();
        db.insert(
            modelvault_core::schema::CollectionId(1),
            BTreeMap::from([("title".to_string(), RowValue::String("Rust".to_string()))]),
        )
        .unwrap();
    }

    let scan = scan_database_file(&path, DatabaseScanMode::Verify).unwrap();
    assert!(scan.superblock.is_some());
    assert!(!scan.segments.is_empty());
    assert_eq!(scan.catalog.collection_names(), vec!["books".to_string()]);
}

#[test]
fn scan_database_store_and_select_superblock_branches() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("sb.modelvault");
    let _db = Database::open(&path).unwrap();

    let f = OpenOptions::new().read(true).open(&path).unwrap();
    let mut store = FileStore::new(f);
    let scan = scan_database_store(&mut store, DatabaseScanMode::Inspect).unwrap();
    assert!(scan.superblock.is_some());

    let len = store.len().unwrap();
    assert!(len >= SEGMENT_REGION_START);

    let mut hdr = [0u8; FILE_HEADER_SIZE];
    let mut a = [0u8; SUPERBLOCK_SIZE];
    let mut b = [0u8; SUPERBLOCK_SIZE];
    store.read_exact_at(0, &mut hdr).unwrap();
    store
        .read_exact_at(FILE_HEADER_SIZE as u64, &mut a)
        .unwrap();
    store
        .read_exact_at((FILE_HEADER_SIZE + SUPERBLOCK_SIZE) as u64, &mut b)
        .unwrap();
    assert!(select_superblock(&a, &b).is_some());
    assert!(select_superblock(&a, &[0u8; SUPERBLOCK_SIZE]).is_some());
    assert!(select_superblock(&[0u8; SUPERBLOCK_SIZE], &b).is_some());
    assert!(select_superblock(&[0u8; SUPERBLOCK_SIZE], &[0u8; SUPERBLOCK_SIZE]).is_none());
}
