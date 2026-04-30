use typra_core::checkpoint::{
    decode_checkpoint_payload, encode_checkpoint_payload_v0, state_from_checkpoint_payload,
    CheckpointV0, CHECKPOINT_VERSION_V0,
};
use typra_core::error::{DbError, FormatError};

#[test]
fn decode_checkpoint_rejects_wrong_version_and_trailing_bytes() {
    let cp = CheckpointV0 {
        replay_from_offset: 0,
        catalog_records: vec![],
        record_payloads: vec![],
        index_entries: vec![],
    };
    let mut b = encode_checkpoint_payload_v0(&cp);
    // Patch version to 9.
    b[0] = 9;
    b[1] = 0;
    let err = decode_checkpoint_payload(&b).unwrap_err();
    assert!(matches!(
        err,
        DbError::Format(FormatError::UnsupportedVersion { .. })
    ));

    // Trailing bytes after a valid payload.
    let mut b2 = encode_checkpoint_payload_v0(&cp);
    b2.push(0);
    let err = decode_checkpoint_payload(&b2).unwrap_err();
    assert!(matches!(
        err,
        DbError::Format(FormatError::InvalidCatalogPayload { message }) if message.contains("trailing bytes")
    ));
}

#[test]
fn decode_checkpoint_cursor_unexpected_eof_paths() {
    // Too short for u16.
    let err = decode_checkpoint_payload(&[]).unwrap_err();
    assert!(matches!(
        err,
        DbError::Format(FormatError::InvalidCatalogPayload { .. })
    ));

    // Exactly version but missing u64 replay offset.
    let mut b = Vec::new();
    b.extend_from_slice(&CHECKPOINT_VERSION_V0.to_le_bytes());
    let err = decode_checkpoint_payload(&b).unwrap_err();
    assert!(matches!(
        err,
        DbError::Format(FormatError::InvalidCatalogPayload { .. })
    ));

    // Version + replay offset but missing u32 catalog count (hits Cursor::take_u32).
    let mut b2 = Vec::new();
    b2.extend_from_slice(&CHECKPOINT_VERSION_V0.to_le_bytes());
    b2.extend_from_slice(&0u64.to_le_bytes());
    let err = decode_checkpoint_payload(&b2).unwrap_err();
    assert!(matches!(
        err,
        DbError::Format(FormatError::InvalidCatalogPayload { .. })
    ));

    // Reach take_bytes failure: n_catalog=1, entry length=5 but no bytes.
    let mut b3 = Vec::new();
    b3.extend_from_slice(&CHECKPOINT_VERSION_V0.to_le_bytes());
    b3.extend_from_slice(&0u64.to_le_bytes());
    b3.extend_from_slice(&1u32.to_le_bytes()); // n_catalog
    b3.extend_from_slice(&5u32.to_le_bytes()); // entry len
    let err = decode_checkpoint_payload(&b3).unwrap_err();
    assert!(matches!(
        err,
        DbError::Format(FormatError::InvalidCatalogPayload { .. })
    ));
}

#[test]
fn state_from_checkpoint_payload_rejects_short_record_payload() {
    let cp = CheckpointV0 {
        replay_from_offset: 0,
        catalog_records: vec![],
        record_payloads: vec![vec![1, 2, 3]], // <6 triggers truncated
        index_entries: vec![],
    };
    let payload = encode_checkpoint_payload_v0(&cp);
    let err = state_from_checkpoint_payload(&payload).unwrap_err();
    assert!(matches!(
        err,
        DbError::Format(FormatError::TruncatedRecordPayload)
    ));
}
