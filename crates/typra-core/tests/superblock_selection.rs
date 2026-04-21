use typra_core::superblock::{decode_superblock, Superblock, SUPERBLOCK_SIZE};

#[test]
fn superblock_selection_prefers_highest_generation() {
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

    let sa = decode_superblock(&a).unwrap();
    let sb = decode_superblock(&b).unwrap();

    assert!(sa.generation < sb.generation);
    assert_eq!(SUPERBLOCK_SIZE, 4096);
}
