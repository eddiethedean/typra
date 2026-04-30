//! `VecStore` I/O edge cases (mirrors `FileStore` tests where applicable).

use typra_core::error::DbError;
use typra_core::storage::{Store, VecStore};

#[test]
fn vec_store_len_write_read_roundtrip() {
    let mut s = VecStore::new();
    s.write_all_at(0, b"hello").unwrap();
    assert_eq!(s.len().unwrap(), 5);
    let mut buf = [0u8; 5];
    s.read_exact_at(0, &mut buf).unwrap();
    assert_eq!(&buf, b"hello");
}

#[test]
fn vec_store_as_slice_matches_content() {
    let mut s = VecStore::new();
    s.write_all_at(0, &[1, 2, 3]).unwrap();
    assert_eq!(s.as_slice(), &[1, 2, 3]);
}

#[test]
fn vec_store_read_past_end_errors() {
    let mut s = VecStore::new();
    s.write_all_at(0, &[1]).unwrap();
    let mut buf = [0u8; 2];
    let e = s.read_exact_at(0, &mut buf).unwrap_err();
    assert!(matches!(e, DbError::Io(_)));
}

#[test]
fn vec_store_from_vec_into_inner() {
    let s = VecStore::from_vec(vec![9, 8]);
    assert_eq!(s.into_inner(), vec![9, 8]);
}

#[test]
fn vec_store_sync_ok() {
    let mut s = VecStore::new();
    s.sync().unwrap();
}
