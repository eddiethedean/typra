//! Exercise [`typra_core::pager::PagedStore`] under `cargo llvm-cov` (integration tests only).

use typra_core::pager::{PagedStore, DEFAULT_PAGE_SIZE};
use typra_core::storage::{Store, VecStore};

#[test]
fn paged_store_len_sync_cross_page_read_and_multi_page_invalidate() {
    let mut raw = VecStore::new();
    raw.write_all_at(0, &vec![1u8; DEFAULT_PAGE_SIZE as usize])
        .unwrap();
    raw.write_all_at(
        DEFAULT_PAGE_SIZE,
        &vec![2u8; DEFAULT_PAGE_SIZE as usize],
    )
    .unwrap();

    let mut ps = PagedStore::new(raw, DEFAULT_PAGE_SIZE);
    assert_eq!(Store::len(&ps).unwrap(), DEFAULT_PAGE_SIZE * 2);

    let mut cross = vec![0u8; 120];
    ps.read_exact_at(DEFAULT_PAGE_SIZE - 60, &mut cross).unwrap();

    let mut tail = vec![0u8; 64];
    let mut short = VecStore::new();
    short.write_all_at(0, &[9u8; 200]).unwrap();
    let mut ps_short = PagedStore::new(short, DEFAULT_PAGE_SIZE);
    ps_short.read_exact_at(0, &mut tail).unwrap();

    ps.write_all_at(DEFAULT_PAGE_SIZE - 8, &[3u8; 24]).unwrap();
    Store::sync(&mut ps).unwrap();

    let mut verify = [0u8; 8];
    ps.read_exact_at(DEFAULT_PAGE_SIZE - 8, &mut verify).unwrap();
    assert_eq!(verify, [3u8; 8]);

    let raw = ps.into_inner();
    assert_eq!(raw.len().unwrap(), DEFAULT_PAGE_SIZE * 2);
}
