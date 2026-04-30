use typra_core::file_format::{decode_header, FileHeader, FILE_HEADER_SIZE, FILE_MAGIC};
use typra_core::segments::header::{
    decode_segment_header, SegmentHeader, SegmentType, SEGMENT_HEADER_LEN, SEGMENT_MAGIC,
};

#[test]
fn file_header_encode_decode_roundtrip_all_supported_minors() {
    for h in [
        FileHeader::new_v0_3(),
        FileHeader::new_v0_4(),
        FileHeader::new_v0_5(),
        FileHeader::new_v0_8(),
    ] {
        let bytes = h.encode();
        let got = decode_header(&bytes).unwrap();
        assert_eq!(got, h);
    }
    assert_eq!(FILE_HEADER_SIZE, 32);
    assert_eq!(FILE_MAGIC, *b"TDB0");
}

#[test]
fn decode_header_rejects_bad_magic_and_unsupported_minor() {
    let mut bytes = FileHeader::new_v0_8().encode();
    bytes[0] = b'X';
    assert!(decode_header(&bytes).is_err());

    let mut bytes2 = FileHeader::new_v0_8().encode();
    // Set minor to 99.
    bytes2[6..8].copy_from_slice(&99u16.to_le_bytes());
    assert!(decode_header(&bytes2).is_err());
}

#[test]
fn segment_header_roundtrip_all_segment_types() {
    for ty in [
        SegmentType::Schema,
        SegmentType::Record,
        SegmentType::Manifest,
        SegmentType::Checkpoint,
        SegmentType::Index,
        SegmentType::TxnBegin,
        SegmentType::TxnCommit,
        SegmentType::TxnAbort,
    ] {
        let h = SegmentHeader {
            segment_type: ty,
            payload_len: 123,
            payload_crc32c: 456,
        };
        let bytes = h.encode();
        let got = decode_segment_header(&bytes).unwrap();
        assert_eq!(got, h);
    }
    assert_eq!(SEGMENT_HEADER_LEN, 32);
    assert_eq!(SEGMENT_MAGIC, *b"TSG0");
}

#[test]
fn decode_segment_header_rejects_truncated_bad_magic_and_bad_checksum() {
    let short = [0u8; SEGMENT_HEADER_LEN - 1];
    assert!(decode_segment_header(&short).is_err());

    let mut bytes = SegmentHeader {
        segment_type: SegmentType::Schema,
        payload_len: 0,
        payload_crc32c: 0,
    }
    .encode();
    bytes[0] = b'X';
    assert!(decode_segment_header(&bytes).is_err());

    // Corrupt the checksum.
    let mut bytes2 = SegmentHeader {
        segment_type: SegmentType::Schema,
        payload_len: 0,
        payload_crc32c: 0,
    }
    .encode();
    bytes2[31] ^= 0xFF;
    assert!(decode_segment_header(&bytes2).is_err());
}
