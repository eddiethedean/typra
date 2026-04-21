use std::fs;

use typra_core::Database;
use typra_core::DbError;

#[test]
fn open_nested_file_under_tempdir() {
    let root = tempfile::tempdir().expect("tempdir");
    let path = root.path().join("nested").join("db.typra");
    fs::create_dir_all(path.parent().unwrap()).expect("mkdir");
    let db = Database::open(&path).expect("open");
    assert_eq!(db.path(), path.as_path());
    assert!(path.exists());
}

#[test]
fn open_existing_file_roundtrip() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("round.typra");
    {
        let _a = Database::open(&path).expect("first open");
    }
    let _b = Database::open(&path).expect("second open");
    assert!(path.exists());
}

#[test]
fn open_directory_returns_io_error() {
    let dir = tempfile::tempdir().expect("tempdir");
    let res = Database::open(dir.path());
    assert!(matches!(res, Err(DbError::Io(_))));
}

#[test]
fn open_without_parent_fails() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("nope").join("missing").join("db.typra");
    let res = Database::open(&path);
    assert!(matches!(res, Err(DbError::Io(_))));
}
