use typra_core::segments::header::decode_segment_header;
use typra_core::superblock::decode_superblock;

#[test]
fn decoders_reject_truncated_inputs_without_panicking() {
    let sb = [0u8; 17];
    assert!(decode_superblock(&sb).is_err());

    let seg = [0u8; 7];
    assert!(decode_segment_header(&seg).is_err());
}
