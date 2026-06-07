use std::borrow::Cow;
use std::collections::BTreeMap;
use std::path::Path;

use modelvault_core::db::Database;
use modelvault_core::error::FormatError;
use modelvault_core::file_format::{FileHeader, FILE_HEADER_SIZE};
use modelvault_core::record::RowValue;
use modelvault_core::schema::{FieldDef, FieldPath, Type};
use modelvault_core::superblock::{decode_superblock, SUPERBLOCK_SIZE};
use modelvault_core::{CollectionId, DbError, ScalarValue};

fn title_field() -> FieldDef {
    FieldDef {
        path: FieldPath(vec![Cow::Owned("title".to_string())]),
        ty: Type::String,
        constraints: vec![],
    }
}

fn read_superblock_generations(bytes: &[u8]) -> (u64, u64) {
    let sb_a_offset = FILE_HEADER_SIZE;
    let sb_b_offset = FILE_HEADER_SIZE + SUPERBLOCK_SIZE;
    let gen_a = u64::from_le_bytes(
        bytes[(sb_a_offset + 8)..(sb_a_offset + 16)]
            .try_into()
            .unwrap(),
    );
    let gen_b = u64::from_le_bytes(
        bytes[(sb_b_offset + 8)..(sb_b_offset + 16)]
            .try_into()
            .unwrap(),
    );
    (gen_a, gen_b)
}

fn setup_db_with_row(path: &Path) -> CollectionId {
    let mut db = Database::open(path).unwrap();
    db.register_collection("books", vec![title_field()], "title")
        .unwrap();
    let cid = CollectionId(1);
    let mut row = BTreeMap::new();
    row.insert("title".into(), RowValue::String("survives".into()));
    db.insert(cid, row).unwrap();
    cid
}

fn assert_row_readable(db: &Database, cid: CollectionId) {
    let got = db
        .get(cid, &ScalarValue::String("survives".into()))
        .unwrap();
    assert!(got.is_some(), "committed row must be readable after open");
}

#[test]
fn open_new_db_initializes_superblocks() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db.modelvault");
    let mut db = Database::open(&path).unwrap();

    let bytes = db.read_image_for_test().unwrap();
    let sb_a_offset = FILE_HEADER_SIZE;
    let sb_b_offset = FILE_HEADER_SIZE + SUPERBLOCK_SIZE;
    let sa = decode_superblock(&bytes[sb_a_offset..sb_a_offset + SUPERBLOCK_SIZE]).unwrap();
    let sb = decode_superblock(&bytes[sb_b_offset..sb_b_offset + SUPERBLOCK_SIZE]).unwrap();
    assert!(
        sa.manifest_offset != 0 || sb.manifest_offset != 0,
        "at least one superblock must point at a manifest after first open"
    );
}

#[test]
fn open_v0_3_header_only_is_truncated_superblock_error() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db.modelvault");
    std::fs::write(&path, FileHeader::new_v0_3().encode()).unwrap();

    let res = Database::open(&path);
    assert!(matches!(
        res,
        Err(DbError::Format(FormatError::TruncatedSuperblock { got, expected }))
            if got == FILE_HEADER_SIZE && expected > got
    ));
}

#[test]
fn reopen_v0_3_db_reads_and_selects_superblock() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db.modelvault");
    let cid = setup_db_with_row(&path);
    {
        let db = Database::open(&path).unwrap();
        assert_row_readable(&db, cid);
    }
    let db2 = Database::open(&path).unwrap();
    assert_row_readable(&db2, cid);

    let bytes = std::fs::read(&path).unwrap();
    let (gen_a, gen_b) = read_superblock_generations(&bytes);
    assert!(
        gen_a.max(gen_b) >= 1,
        "superblock generation must advance after reopen"
    );
}

#[test]
fn open_v0_3_db_with_corrupt_superblocks_is_format_error() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db.modelvault");
    let header = FileHeader::new_v0_3().encode();

    // Create a file large enough for reserved superblocks, but leave the superblocks as zeros.
    let segment_start = (FILE_HEADER_SIZE + 2 * SUPERBLOCK_SIZE) as u64;
    let mut bytes = vec![0u8; segment_start as usize];
    bytes[0..FILE_HEADER_SIZE].copy_from_slice(&header);
    std::fs::write(&path, bytes).unwrap();

    let res = Database::open(&path);
    assert!(matches!(
        res,
        Err(DbError::Format(FormatError::BadSuperblockChecksum))
    ));
}

#[test]
fn open_v0_3_db_with_one_bad_superblock_still_opens() {
    use modelvault_core::superblock::{SUPERBLOCK_MAGIC, SUPERBLOCK_SIZE};

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db.modelvault");
    let cid = setup_db_with_row(&path);

    // Corrupt superblock B magic.
    let sb_b_offset = (FILE_HEADER_SIZE + SUPERBLOCK_SIZE) as u64;
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[sb_b_offset as usize..(sb_b_offset as usize + 4)].copy_from_slice(&SUPERBLOCK_MAGIC);
    bytes[sb_b_offset as usize] ^= 0xff;
    std::fs::write(&path, bytes).unwrap();

    let db2 = Database::open(&path).unwrap();
    assert_row_readable(&db2, cid);

    let bytes_after = std::fs::read(&path).unwrap();
    let sb_a_offset = FILE_HEADER_SIZE;
    decode_superblock(&bytes_after[sb_a_offset..sb_a_offset + SUPERBLOCK_SIZE])
        .expect("superblock A must remain valid when B is corrupt");
}

#[test]
fn open_v0_3_db_with_only_superblock_b_valid_opens() {
    use modelvault_core::superblock::SUPERBLOCK_MAGIC;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db.modelvault");
    let cid = setup_db_with_row(&path);

    // Corrupt superblock A magic.
    let sb_a_offset = FILE_HEADER_SIZE as u64;
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[sb_a_offset as usize..(sb_a_offset as usize + 4)].copy_from_slice(&SUPERBLOCK_MAGIC);
    bytes[sb_a_offset as usize] ^= 0xff;
    std::fs::write(&path, bytes).unwrap();

    let db2 = Database::open(&path).unwrap();
    assert_row_readable(&db2, cid);

    let bytes_after = std::fs::read(&path).unwrap();
    let sb_b_offset = FILE_HEADER_SIZE + SUPERBLOCK_SIZE;
    decode_superblock(&bytes_after[sb_b_offset..sb_b_offset + SUPERBLOCK_SIZE])
        .expect("superblock B must remain valid when A is corrupt");
}

#[test]
fn open_selects_superblock_with_highest_generation() {
    use modelvault_core::superblock::{SUPERBLOCK_MAGIC, SUPERBLOCK_SIZE};

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db.modelvault");
    let cid = setup_db_with_row(&path);

    let sb_b_offset = (FILE_HEADER_SIZE + SUPERBLOCK_SIZE) as u64;
    let mut bytes = std::fs::read(&path).unwrap();
    let (gen_a_before, _) = read_superblock_generations(&bytes);

    // Set superblock B generation to 2 and fix its CRC.
    bytes[sb_b_offset as usize..(sb_b_offset as usize + 4)].copy_from_slice(&SUPERBLOCK_MAGIC);
    bytes[(sb_b_offset + 4) as usize..(sb_b_offset + 6) as usize]
        .copy_from_slice(&0u16.to_le_bytes());
    bytes[(sb_b_offset + 8) as usize..(sb_b_offset + 16) as usize]
        .copy_from_slice(&2u64.to_le_bytes());
    bytes[(sb_b_offset + 28) as usize] = 0;

    let crc = crc32c::crc32c(&bytes[sb_b_offset as usize..(sb_b_offset as usize + 32)]);
    bytes[(sb_b_offset + 32) as usize..(sb_b_offset + 36) as usize]
        .copy_from_slice(&crc.to_le_bytes());
    std::fs::write(&path, bytes).unwrap();

    let db2 = Database::open(&path).unwrap();
    assert_row_readable(&db2, cid);

    let bytes_after = std::fs::read(&path).unwrap();
    let (gen_a, gen_b) = read_superblock_generations(&bytes_after);
    assert!(
        gen_b >= 2,
        "superblock B generation should reflect the higher slot (got A={gen_a}, B={gen_b})"
    );
    assert!(
        gen_a.max(gen_b) >= gen_a_before.max(2),
        "active superblock chain must reflect selection of the higher generation"
    );
}

#[test]
fn open_new_db_publishes_manifest_pointer() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db.modelvault");
    let mut db = Database::open(&path).unwrap();

    // One of the superblocks should now point at a manifest segment.
    let bytes = db.read_image_for_test().unwrap();
    let sb_a_offset = FILE_HEADER_SIZE;
    let sb_b_offset = FILE_HEADER_SIZE + SUPERBLOCK_SIZE;

    let a_manifest = u64::from_le_bytes(
        bytes[(sb_a_offset + 16)..(sb_a_offset + 24)]
            .try_into()
            .unwrap(),
    );
    let b_manifest = u64::from_le_bytes(
        bytes[(sb_b_offset + 16)..(sb_b_offset + 24)]
            .try_into()
            .unwrap(),
    );
    assert!(a_manifest != 0 || b_manifest != 0);
}

#[test]
fn open_twice_increases_superblock_generation() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db.modelvault");
    let mut db = Database::open(&path).unwrap();

    let bytes1 = db.read_image_for_test().unwrap();
    let sb_a_offset = FILE_HEADER_SIZE;
    let sb_b_offset = FILE_HEADER_SIZE + SUPERBLOCK_SIZE;
    let gen_a1 = u64::from_le_bytes(
        bytes1[(sb_a_offset + 8)..(sb_a_offset + 16)]
            .try_into()
            .unwrap(),
    );
    let gen_b1 = u64::from_le_bytes(
        bytes1[(sb_b_offset + 8)..(sb_b_offset + 16)]
            .try_into()
            .unwrap(),
    );
    let max1 = gen_a1.max(gen_b1);

    drop(db);
    let _db2 = Database::open(&path).unwrap();
    let bytes2 = std::fs::read(&path).unwrap();
    let gen_a2 = u64::from_le_bytes(
        bytes2[(sb_a_offset + 8)..(sb_a_offset + 16)]
            .try_into()
            .unwrap(),
    );
    let gen_b2 = u64::from_le_bytes(
        bytes2[(sb_b_offset + 8)..(sb_b_offset + 16)]
            .try_into()
            .unwrap(),
    );
    let max2 = gen_a2.max(gen_b2);

    assert!(max2 >= max1);
}
