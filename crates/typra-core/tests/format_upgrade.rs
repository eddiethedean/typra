use std::io::Write;

use typra_core::db::Database;
use typra_core::file_format::FILE_HEADER_SIZE;

#[test]
fn open_upgrades_header_only_v0_2_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("old.typra");

    // Write a 0.2 header-only file: magic + major=0 + minor=2.
    let mut header = [0u8; FILE_HEADER_SIZE];
    header[0..4].copy_from_slice(b"TDB0");
    header[4..6].copy_from_slice(&0u16.to_le_bytes());
    header[6..8].copy_from_slice(&2u16.to_le_bytes());
    header[8..12].copy_from_slice(&(FILE_HEADER_SIZE as u32).to_le_bytes());

    let mut f = std::fs::File::create(&path).unwrap();
    f.write_all(&header).unwrap();
    drop(f);

    let _db = Database::open(&path).unwrap();
    let len = std::fs::metadata(&path).unwrap().len();
    assert!(len > FILE_HEADER_SIZE as u64);
}

#[test]
fn open_rejects_v0_2_file_with_extra_bytes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("old_with_data.typra");

    let mut header = [0u8; FILE_HEADER_SIZE];
    header[0..4].copy_from_slice(b"TDB0");
    header[4..6].copy_from_slice(&0u16.to_le_bytes());
    header[6..8].copy_from_slice(&2u16.to_le_bytes());
    header[8..12].copy_from_slice(&(FILE_HEADER_SIZE as u32).to_le_bytes());

    std::fs::write(&path, [&header[..], b"x"].concat()).unwrap();
    let res = Database::open(&path);
    assert!(res.is_err());
}
