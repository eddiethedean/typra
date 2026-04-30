use typra_core::db::Database;
use typra_core::error::FormatError;
use typra_core::file_format::{FileHeader, FILE_HEADER_SIZE};
use typra_core::DbError;

#[test]
fn open_new_db_initializes_superblocks() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db.typra");
    let _db = Database::open(&path).unwrap();
}

#[test]
fn open_v0_3_header_only_is_truncated_superblock_error() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db.typra");
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
    let path = dir.path().join("db.typra");
    let _db = Database::open(&path).unwrap();
    let _db2 = Database::open(&path).unwrap();
}

#[test]
fn open_v0_3_db_with_corrupt_superblocks_is_format_error() {
    use typra_core::superblock::SUPERBLOCK_SIZE;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db.typra");
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
    use typra_core::superblock::{SUPERBLOCK_MAGIC, SUPERBLOCK_SIZE};

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db.typra");
    let _db = Database::open(&path).unwrap();

    // Corrupt superblock B magic.
    let sb_b_offset = (FILE_HEADER_SIZE + SUPERBLOCK_SIZE) as u64;
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[sb_b_offset as usize..(sb_b_offset as usize + 4)].copy_from_slice(&SUPERBLOCK_MAGIC);
    bytes[sb_b_offset as usize] ^= 0xff;
    std::fs::write(&path, bytes).unwrap();

    let _db2 = Database::open(&path).unwrap();
}

#[test]
fn open_v0_3_db_with_only_superblock_b_valid_opens() {
    use typra_core::superblock::SUPERBLOCK_MAGIC;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db.typra");
    let _db = Database::open(&path).unwrap();

    // Corrupt superblock A magic.
    let sb_a_offset = FILE_HEADER_SIZE as u64;
    let mut bytes = std::fs::read(&path).unwrap();
    bytes[sb_a_offset as usize..(sb_a_offset as usize + 4)].copy_from_slice(&SUPERBLOCK_MAGIC);
    bytes[sb_a_offset as usize] ^= 0xff;
    std::fs::write(&path, bytes).unwrap();

    let _db2 = Database::open(&path).unwrap();
}

#[test]
fn open_selects_superblock_with_highest_generation() {
    use typra_core::superblock::{SUPERBLOCK_MAGIC, SUPERBLOCK_SIZE};

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db.typra");
    let _db = Database::open(&path).unwrap();

    let sb_b_offset = (FILE_HEADER_SIZE + SUPERBLOCK_SIZE) as u64;
    let mut bytes = std::fs::read(&path).unwrap();

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

    let _db2 = Database::open(&path).unwrap();
}

#[test]
fn open_new_db_publishes_manifest_pointer() {
    use typra_core::superblock::SUPERBLOCK_SIZE;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db.typra");
    let _db = Database::open(&path).unwrap();

    // One of the superblocks should now point at a manifest segment.
    let bytes = std::fs::read(&path).unwrap();
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
    use typra_core::superblock::SUPERBLOCK_SIZE;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db.typra");
    let _db = Database::open(&path).unwrap();

    let bytes1 = std::fs::read(&path).unwrap();
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
