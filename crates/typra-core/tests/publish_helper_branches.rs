use typra_core::file_format::{FileHeader, FILE_HEADER_SIZE};
use typra_core::publish::append_manifest_and_publish;
use typra_core::storage::{FileStore, Store};
use typra_core::superblock::{Superblock, SUPERBLOCK_SIZE};

fn new_store_with_superblocks(sb_a: &[u8], sb_b: &[u8]) -> FileStore {
    let f = tempfile::NamedTempFile::new().unwrap();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(f.path())
        .unwrap();
    let mut store = FileStore::new(file);

    let header = FileHeader::new_v0_3();
    store.write_all_at(0, &header.encode()).unwrap();

    let segment_start = (FILE_HEADER_SIZE + 2 * SUPERBLOCK_SIZE) as u64;
    store.write_all_at(segment_start - 1, &[0u8]).unwrap();

    store.write_all_at(FILE_HEADER_SIZE as u64, sb_a).unwrap();
    store
        .write_all_at((FILE_HEADER_SIZE + SUPERBLOCK_SIZE) as u64, sb_b)
        .unwrap();

    store
}

#[test]
fn publish_selects_b_when_b_generation_higher() {
    let a = Superblock {
        generation: 1,
        ..Superblock::empty()
    }
    .encode();
    let b = Superblock {
        generation: 2,
        ..Superblock::empty()
    }
    .encode();
    let mut store = new_store_with_superblocks(&a, &b);

    let segment_start = (FILE_HEADER_SIZE + 2 * SUPERBLOCK_SIZE) as u64;
    let sb = append_manifest_and_publish(&mut store, segment_start).unwrap();
    assert!(sb.generation >= 3);
}

#[test]
fn publish_handles_only_a_valid() {
    let a = Superblock {
        generation: 7,
        ..Superblock::empty()
    }
    .encode();
    let mut b = Superblock::empty().encode();
    b[0] ^= 0xff; // break magic so decode fails
    let mut store = new_store_with_superblocks(&a, &b);

    let segment_start = (FILE_HEADER_SIZE + 2 * SUPERBLOCK_SIZE) as u64;
    let sb = append_manifest_and_publish(&mut store, segment_start).unwrap();
    assert_eq!(sb.generation, 8);
}

#[test]
fn publish_handles_only_b_valid() {
    let mut a = Superblock::empty().encode();
    a[0] ^= 0xff;
    let b = Superblock {
        generation: 9,
        ..Superblock::empty()
    }
    .encode();
    let mut store = new_store_with_superblocks(&a, &b);

    let segment_start = (FILE_HEADER_SIZE + 2 * SUPERBLOCK_SIZE) as u64;
    let sb = append_manifest_and_publish(&mut store, segment_start).unwrap();
    assert_eq!(sb.generation, 10);
}

#[test]
fn publish_handles_neither_valid() {
    let mut a = Superblock::empty().encode();
    let mut b = Superblock::empty().encode();
    a[0] ^= 0xff;
    b[0] ^= 0xff;
    let mut store = new_store_with_superblocks(&a, &b);

    let segment_start = (FILE_HEADER_SIZE + 2 * SUPERBLOCK_SIZE) as u64;
    let sb = append_manifest_and_publish(&mut store, segment_start).unwrap();
    assert_eq!(sb.generation, 1);
}
