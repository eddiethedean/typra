use typra_core::segments::reader::scan_segments;
use typra_core::storage::{FileStore, Store};

#[test]
fn scan_segments_empty_range_is_ok() {
    let f = tempfile::NamedTempFile::new().unwrap();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(f.path())
        .unwrap();
    let mut store = FileStore::new(file);
    assert_eq!(store.len().unwrap(), 0);

    let metas = scan_segments(&mut store, 0).unwrap();
    assert!(metas.is_empty());
}
