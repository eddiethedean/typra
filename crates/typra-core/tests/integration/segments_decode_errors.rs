use typra_core::error::FormatError;
use typra_core::segments::header::{
    decode_segment_header, SegmentHeader, SegmentType, SEGMENT_HEADER_LEN,
};
use typra_core::DbError;

#[test]
fn decode_segment_header_rejects_truncated() {
    let bytes = [0u8; 12];
    let res = decode_segment_header(&bytes);
    assert!(matches!(
        res,
        Err(DbError::Format(FormatError::TruncatedSegmentHeader { .. }))
    ));
}

#[test]
fn decode_segment_header_rejects_unsupported_version() {
    let mut bytes = SegmentHeader {
        segment_type: SegmentType::Schema,
        payload_len: 0,
        payload_crc32c: 0,
    }
    .encode();
    bytes[4] = 9;
    let res = decode_segment_header(&bytes);
    assert!(matches!(
        res,
        Err(DbError::Format(FormatError::UnsupportedVersion { .. }))
    ));
}

#[test]
fn decode_segment_header_rejects_unknown_type() {
    let mut bytes = SegmentHeader {
        segment_type: SegmentType::Schema,
        payload_len: 0,
        payload_crc32c: 0,
    }
    .encode();
    bytes[6..8].copy_from_slice(&999u16.to_le_bytes());
    let crc = crc32c::crc32c(&bytes[0..28]);
    bytes[28..32].copy_from_slice(&crc.to_le_bytes());

    let res = decode_segment_header(&bytes);
    assert!(matches!(
        res,
        Err(DbError::Format(FormatError::UnsupportedVersion { .. }))
    ));
}

#[test]
fn decode_segment_header_rejects_header_len_mismatch() {
    let mut bytes = SegmentHeader {
        segment_type: SegmentType::Schema,
        payload_len: 0,
        payload_crc32c: 0,
    }
    .encode();
    bytes[8..12].copy_from_slice(&0u32.to_le_bytes());
    let crc = crc32c::crc32c(&bytes[0..28]);
    bytes[28..32].copy_from_slice(&crc.to_le_bytes());

    let res = decode_segment_header(&bytes);
    assert!(matches!(
        res,
        Err(DbError::Format(FormatError::UnsupportedVersion { .. }))
    ));
    assert_eq!(SEGMENT_HEADER_LEN, 32);
}

#[test]
fn decode_segment_header_rejects_checksum_kind_mismatch() {
    let mut bytes = SegmentHeader {
        segment_type: SegmentType::Schema,
        payload_len: 0,
        payload_crc32c: 0,
    }
    .encode();
    bytes[24] = 9;
    let crc = crc32c::crc32c(&bytes[0..28]);
    bytes[28..32].copy_from_slice(&crc.to_le_bytes());

    let res = decode_segment_header(&bytes);
    assert!(matches!(
        res,
        Err(DbError::Format(FormatError::UnsupportedVersion { .. }))
    ));
    assert_eq!(SEGMENT_HEADER_LEN, 32);
}
