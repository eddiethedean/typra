use typra_core::db::Database;
use typra_core::file_format::FILE_HEADER_SIZE;
use typra_core::superblock::{decode_superblock, Superblock, SUPERBLOCK_SIZE};

#[test]
fn corrupt_manifest_does_not_prevent_open_when_scan_succeeds() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db.typra");
    let _db = Database::open(&path).unwrap();

    // Corrupt the manifest pointer (not the segment stream), so scan fallback can succeed.
    let bytes = std::fs::read(&path).unwrap();
    let sb_a_offset = FILE_HEADER_SIZE as u64;
    let sb_b_offset = (FILE_HEADER_SIZE + SUPERBLOCK_SIZE) as u64;

    let sb_a =
        decode_superblock(&bytes[sb_a_offset as usize..(sb_a_offset as usize + SUPERBLOCK_SIZE)])
            .unwrap();
    let sb_b =
        decode_superblock(&bytes[sb_b_offset as usize..(sb_b_offset as usize + SUPERBLOCK_SIZE)])
            .unwrap();
    let (current, current_is_a) = if sb_a.generation >= sb_b.generation {
        (sb_a, true)
    } else {
        (sb_b, false)
    };

    let corrupted = Superblock {
        manifest_offset: 1, // definitely not a valid segment start
        ..current
    };
    let target_offset = if current_is_a {
        sb_a_offset
    } else {
        sb_b_offset
    };

    let mut f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    use std::io::{Seek, Write};
    f.seek(std::io::SeekFrom::Start(target_offset)).unwrap();
    f.write_all(&corrupted.encode()).unwrap();

    // Should still open (falls back to scan).
    let _db2 = Database::open(&path).unwrap();
}
