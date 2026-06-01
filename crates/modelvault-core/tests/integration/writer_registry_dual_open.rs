//! At most one writable [`Database`] per on-disk path per process.

use modelvault_core::Database;
use modelvault_core::DbError;

#[test]
fn second_writable_open_in_same_process_fails() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db.modelvault");
    let _a = Database::open(&path).unwrap();
    match Database::open(&path) {
        Ok(_) => panic!("expected second writable open to fail"),
        Err(e) => assert!(matches!(e, DbError::Io(_))),
    }
}
