use modelvault_core::checkpoint::{
    decode_checkpoint_payload, encode_checkpoint_payload_v0, CheckpointV0,
};
use modelvault_core::error::{DbError, FormatError};
use modelvault_core::file_format::{
    check_field_bytes_len, MAX_FIELD_BYTES, MAX_SEGMENT_DECODE_ENTRIES, MAX_SEGMENT_PAYLOAD_BYTES,
};
use modelvault_core::record::{decode_row_value, decode_tagged_string, Cursor};
use modelvault_core::schema::Type;
use modelvault_core::segments::header::{decode_segment_header, SegmentHeader, SegmentType};

#[test]
fn checkpoint_decode_rejects_excessive_catalog_count() {
    let mut buf = encode_checkpoint_payload_v0(&CheckpointV0 {
        replay_from_offset: 0,
        catalog_records: vec![],
        record_payloads: vec![],
        index_entries: vec![],
    });
    let n = (MAX_SEGMENT_DECODE_ENTRIES as u32).saturating_add(1);
    buf[10..14].copy_from_slice(&n.to_le_bytes());
    let err = decode_checkpoint_payload(&buf).unwrap_err();
    assert!(matches!(
        err,
        DbError::Format(FormatError::InvalidCatalogPayload { .. })
    ));
}

#[test]
fn list_decode_rejects_excessive_entry_count() {
    let n = (MAX_SEGMENT_DECODE_ENTRIES as u32).saturating_add(1);
    let mut buf = Vec::new();
    buf.extend_from_slice(&n.to_le_bytes());
    let mut cur = Cursor::new(&buf);
    let err = decode_row_value(&mut cur, &Type::List(Box::new(Type::Int64))).unwrap_err();
    assert!(matches!(
        err,
        DbError::Format(FormatError::InvalidCatalogPayload { .. })
    ));
}

#[test]
fn segment_header_rejects_oversized_payload() {
    let hdr = SegmentHeader {
        segment_type: SegmentType::Record,
        payload_len: MAX_SEGMENT_PAYLOAD_BYTES.saturating_add(1),
        payload_crc32c: 0,
    }
    .encode();
    let err = decode_segment_header(&hdr).unwrap_err();
    assert!(matches!(
        err,
        DbError::Format(FormatError::InvalidCatalogPayload { .. })
    ));
}

#[test]
fn field_bytes_cap_rejects_oversized_length() {
    let err = check_field_bytes_len(MAX_FIELD_BYTES.saturating_add(1)).unwrap_err();
    assert!(matches!(
        err,
        DbError::Format(FormatError::InvalidCatalogPayload { .. })
    ));
}

#[test]
fn tagged_string_decode_rejects_oversized_length() {
    let n = (MAX_FIELD_BYTES as u32).saturating_add(1);
    let mut buf = Vec::new();
    buf.push(4); // string tag
    buf.extend_from_slice(&n.to_le_bytes());
    let mut cur = Cursor::new(&buf);
    let err = decode_tagged_string(&mut cur).unwrap_err();
    assert!(matches!(
        err,
        DbError::Format(FormatError::InvalidCatalogPayload { .. })
    ));
}
