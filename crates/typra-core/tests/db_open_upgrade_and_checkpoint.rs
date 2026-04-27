use std::fs::OpenOptions as FsOpenOptions;
use std::path::Path;

use typra_core::file_format::{decode_header, FILE_HEADER_SIZE};
use typra_core::segments::header::{SegmentHeader, SegmentType};
use typra_core::storage::FileStore;
use typra_core::storage::Store;
use typra_core::superblock::{Superblock, SUPERBLOCK_SIZE};
use typra_core::{Database, OpenOptions, RecoveryMode};

fn read_header_minor(path: &Path) -> u16 {
    let bytes = std::fs::read(path).unwrap();
    let header = decode_header(&bytes[..FILE_HEADER_SIZE]).unwrap();
    header.format_minor
}

#[test]
fn open_upgrades_legacy_minor2_header_only_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("legacy_v2.typra");

    // Create a file that is exactly FILE_HEADER_SIZE bytes with format_minor=2.
    let mut header = typra_core::file_format::FileHeader::new_v0_3().encode();
    header[6..8].copy_from_slice(&2u16.to_le_bytes());
    std::fs::write(&path, &header).unwrap();

    // Opening should upgrade it to v0_3 and initialize superblocks/manifest.
    let _db = Database::open(&path).unwrap();
    assert_eq!(read_header_minor(&path), typra_core::file_format::FORMAT_MINOR_V3);
}

#[test]
fn open_rejects_truncated_header() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("trunc.typra");
    std::fs::write(&path, &[0u8; 7]).unwrap();
    assert!(Database::open(&path).is_err());
}

#[test]
fn strict_rejects_bad_checkpoint_crc_but_autotruncate_falls_back_to_full_replay() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bad_checkpoint.typra");

    // Create a valid DB file.
    {
        let mut db = Database::open(&path).unwrap();
        let _ = db.register_collection(
            "t",
            vec![typra_core::schema::FieldDef {
                path: typra_core::schema::FieldPath(vec![std::borrow::Cow::Borrowed("id")]),
                ty: typra_core::schema::Type::Int64,
                constraints: vec![],
            }],
            "id",
        );
    }

    // Append a "checkpoint" segment with an intentionally wrong payload CRC, then point the
    // superblock's checkpoint_offset at it.
    let file = FsOpenOptions::new()
        .read(true)
        .write(true)
        .create(false)
        .open(&path)
        .unwrap();
    let mut store = FileStore::new(file);

    let checkpoint_offset = store.len().unwrap();
    // Write a checkpoint segment header with an intentionally wrong payload CRC.
    let bad = SegmentHeader {
        segment_type: SegmentType::Checkpoint,
        payload_len: 0,
        payload_crc32c: 123, // wrong (crc of empty payload is 0)
    }
    .encode();
    store.write_all_at(checkpoint_offset, &bad).unwrap();
    store.sync().unwrap();

    // Overwrite superblock A with generation 2 and checkpoint pointer.
    let sb = Superblock {
        generation: 2,
        manifest_offset: 0,
        manifest_len: 0,
        checkpoint_offset,
        checkpoint_len: 1,
        ..Superblock::empty()
    };
    store
        .write_all_at(FILE_HEADER_SIZE as u64, &sb.encode())
        .unwrap();
    // Leave superblock B empty so A is selected.
    store
        .write_all_at((FILE_HEADER_SIZE + SUPERBLOCK_SIZE) as u64, &Superblock::empty().encode())
        .unwrap();
    store.sync().unwrap();

    // Strict: error on bad checkpoint payload checksum.
    let strict = OpenOptions {
        recovery: RecoveryMode::Strict,
        ..OpenOptions::default()
    };
    assert!(Database::open_with_options(&path, strict).is_err());

    // AutoTruncate: falls back to full replay and should open successfully.
    let auto = OpenOptions {
        recovery: RecoveryMode::AutoTruncate,
        ..OpenOptions::default()
    };
    let _ = Database::open_with_options(&path, auto).unwrap();
}

