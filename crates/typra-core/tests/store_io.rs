use std::fs::OpenOptions;

use typra_core::storage::{FileStore, Store};

#[test]
fn file_store_len_read_write_and_sync() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("store.typra");

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&path)
        .expect("open file");

    let mut store = FileStore::new(file);
    assert_eq!(store.len().expect("len"), 0);

    store.write_all_at(0, b"hello").expect("write");
    store.sync().expect("sync");
    assert_eq!(store.len().expect("len after write"), 5);

    let mut buf = [0u8; 5];
    store.read_exact_at(0, &mut buf).expect("read");
    assert_eq!(&buf, b"hello");
}

#[test]
fn file_store_read_past_end_returns_error() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("store_eof.typra");

    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&path)
        .expect("open file");

    let mut store = FileStore::new(file);

    let mut buf = [0u8; 1];
    let err = store
        .read_exact_at(0, &mut buf)
        .expect_err("expected read error");
    // Should map to DbError::Io(_)
    assert!(err.to_string().contains("i/o error"));
}

#[test]
fn file_store_write_to_read_only_returns_error() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("ro.typra");

    std::fs::write(&path, b"abc").expect("seed");

    let file = OpenOptions::new()
        .read(true)
        .write(false)
        .open(&path)
        .expect("open read-only");

    let mut store = FileStore::new(file);
    let err = store.write_all_at(0, b"z").expect_err("expected write error");
    assert!(err.to_string().contains("i/o error"));
}

#[test]
fn file_store_read_from_write_only_returns_error() {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("wo.typra");

    let file = OpenOptions::new()
        .read(false)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&path)
        .expect("open write-only");

    let mut store = FileStore::new(file);
    let mut buf = [0u8; 1];
    let err = store
        .read_exact_at(0, &mut buf)
        .expect_err("expected read error");
    assert!(err.to_string().contains("i/o error"));
}

