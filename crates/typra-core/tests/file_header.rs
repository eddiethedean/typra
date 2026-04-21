use std::fs;

use typra_core::Database;
use typra_core::DbError;

#[test]
fn open_writes_header_on_new_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("new.typra");

    let _db = Database::open(&path).expect("open");
    let bytes = fs::read(&path).expect("read");
    assert!(bytes.len() >= 4);
    assert_eq!(&bytes[0..4], b"TDB0");
}

#[test]
fn open_validates_header_on_reopen() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("reopen.typra");

    let _a = Database::open(&path).expect("open a");
    let _b = Database::open(&path).expect("open b");
}

#[test]
fn open_non_typra_file_returns_format_error() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("not_typra.typra");

    fs::write(&path, b"NOPE").expect("write");
    let res = Database::open(&path);
    assert!(matches!(res, Err(DbError::Format(_))));
}

#[test]
fn open_wrong_magic_with_full_header_returns_format_error() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("bad_magic.typra");

    // Make the file long enough to pass the truncated-header check, but with wrong magic.
    let mut bytes = vec![0u8; 32];
    bytes[0..4].copy_from_slice(b"NOPE");
    fs::write(&path, bytes).expect("write");

    let res = Database::open(&path);
    assert!(matches!(res, Err(DbError::Format(_))));
}

#[test]
fn open_unsupported_version_returns_format_error() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("bad_version.typra");

    let mut bytes = vec![0u8; 32];
    bytes[0..4].copy_from_slice(b"TDB0");
    // major=9, minor=9 in little-endian at offsets 4..8
    bytes[4] = 9;
    bytes[6] = 9;
    fs::write(&path, bytes).expect("write");

    let res = Database::open(&path);
    assert!(matches!(res, Err(DbError::Format(_))));
}

#[test]
fn open_truncated_file_returns_format_error() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("truncated.typra");

    // Non-empty but shorter than the header size.
    fs::write(&path, b"T").expect("write");
    let res = Database::open(&path);
    assert!(matches!(res, Err(DbError::Format(_))));
}

#[test]
fn open_does_not_overwrite_existing_header() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("no_overwrite.typra");

    let _db = Database::open(&path).expect("open");
    let mut bytes = fs::read(&path).expect("read");
    assert!(bytes.len() >= 32);

    // Mutate a byte in the header to simulate some future header field changes
    // (while keeping the magic intact so open() continues to read/validate).
    bytes[20] ^= 0b1010_1010;
    fs::write(&path, &bytes).expect("write mutated header");

    let _db2 = Database::open(&path).expect("open again");
    let bytes2 = fs::read(&path).expect("read again");
    assert_eq!(bytes2, bytes);
}

