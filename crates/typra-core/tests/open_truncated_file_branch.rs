use typra_core::error::FormatError;
use typra_core::Database;
use typra_core::DbError;

#[test]
fn open_rejects_file_shorter_than_header() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("trunc.typra");
    std::fs::write(&path, [0u8; 1]).unwrap();
    let res = Database::open(&path);
    assert!(matches!(
        res,
        Err(DbError::Format(FormatError::TruncatedHeader { .. }))
    ));
}
