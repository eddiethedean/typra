//! On-disk `Database::open` with a v0.5 image should load via the legacy replay path. This
//! matches the `FileStore` + `format_minor < 6` monomorph of `load_catalog_latest_and_indexes` in
//! `db/replay.rs` (llvm-cov tracks each generic instantiation separately from unit tests).

use tempfile::tempdir;
use typra_core::db::Database;
use typra_core::file_format::{FileHeader, FILE_HEADER_SIZE};
use typra_core::storage::{Store, VecStore};
use typra_core::superblock::{Superblock, SUPERBLOCK_SIZE};

fn v5_empty_blessed_image() -> Vec<u8> {
    let mut store = VecStore::new();
    store
        .write_all_at(0, &FileHeader::new_v0_5().encode())
        .unwrap();
    let segment_start = (FILE_HEADER_SIZE + 2 * SUPERBLOCK_SIZE) as u64;
    store.write_all_at(segment_start - 1, &[0u8]).unwrap();
    let sb = Superblock {
        generation: 1,
        ..Superblock::empty()
    };
    store
        .write_all_at(FILE_HEADER_SIZE as u64, &sb.encode())
        .unwrap();
    store
        .write_all_at(
            (FILE_HEADER_SIZE + SUPERBLOCK_SIZE) as u64,
            &Superblock::empty().encode(),
        )
        .unwrap();
    store.into_inner()
}

#[test]
fn open_file_backed_v5_hits_legacy_load_catalog() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("v5.typra");
    std::fs::write(&path, v5_empty_blessed_image()).unwrap();
    let _db = Database::open(&path).unwrap();
}
