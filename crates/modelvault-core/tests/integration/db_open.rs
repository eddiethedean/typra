use std::fs;

use modelvault_core::Database;

#[test]
fn open_creates_file() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("test.modelvault");
    let db = Database::open(&path).expect("open");
    assert_eq!(db.path(), path);
    assert!(path.exists());
    drop(db);
    fs::remove_file(&path).ok();
}

#[test]
fn open_returns_db_path_as_path() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("test2.modelvault");
    let db = Database::open(&path).expect("open");
    assert_eq!(db.path(), path.as_path());
}
