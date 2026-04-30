use typra_core::error::{DbError, FormatError, SchemaError};
use typra_core::index::{
    decode_index_payload, encode_index_payload, IndexEntry, IndexOp, IndexState,
};
use typra_core::schema::IndexKind;
use typra_core::ScalarValue;

#[test]
fn index_state_unique_insert_delete_branches() {
    let mut st = IndexState::default();
    let e1 = IndexEntry {
        collection_id: 1,
        index_name: "u".into(),
        kind: IndexKind::Unique,
        op: IndexOp::Insert,
        index_key: ScalarValue::Int64(1).canonical_key_bytes(),
        pk_key: ScalarValue::String("a".into()).canonical_key_bytes(),
    };
    st.apply(e1.clone()).unwrap();

    // Idempotent insert with same pk.
    st.apply(e1.clone()).unwrap();

    // Conflicting unique insert.
    let mut e2 = e1.clone();
    e2.pk_key = ScalarValue::String("b".into()).canonical_key_bytes();
    let err = st.apply(e2).unwrap_err();
    assert!(matches!(
        err,
        DbError::Schema(SchemaError::UniqueIndexViolation)
    ));

    // Delete mismatch is ok (no-op).
    let mut del = e1.clone();
    del.op = IndexOp::Delete;
    del.pk_key = ScalarValue::String("zzz".into()).canonical_key_bytes();
    st.apply(del).unwrap();

    // Delete exact match removes.
    let mut del2 = e1.clone();
    del2.op = IndexOp::Delete;
    st.apply(del2).unwrap();
}

#[test]
fn decode_index_payload_rejects_unknown_kind_and_op_and_trailing_bytes() {
    // Start with valid payload.
    let e = IndexEntry {
        collection_id: 1,
        index_name: "x".into(),
        kind: IndexKind::Unique,
        op: IndexOp::Insert,
        index_key: vec![1],
        pk_key: vec![2],
    };
    let mut bytes = encode_index_payload(&[e]);

    // Unknown kind tag: overwrite the kind byte (after ver(2), n(4), collection_id(4)).
    // Layout: ver u16, n u32, cid u32, kind u8, op u8, ...
    let kind_pos = 2 + 4 + 4;
    bytes[kind_pos] = 9;
    let err = decode_index_payload(&bytes).unwrap_err();
    assert!(matches!(
        err,
        DbError::Format(FormatError::InvalidCatalogPayload { .. })
    ));

    // Unknown op tag (v2): reset kind to 1, corrupt op.
    let mut bytes2 = encode_index_payload(&[IndexEntry {
        collection_id: 1,
        index_name: "x".into(),
        kind: IndexKind::Unique,
        op: IndexOp::Insert,
        index_key: vec![1],
        pk_key: vec![2],
    }]);
    let op_pos = kind_pos + 1;
    bytes2[op_pos] = 9;
    let err = decode_index_payload(&bytes2).unwrap_err();
    assert!(matches!(
        err,
        DbError::Format(FormatError::InvalidCatalogPayload { .. })
    ));

    // Trailing bytes.
    let mut bytes3 = encode_index_payload(&[IndexEntry {
        collection_id: 1,
        index_name: "x".into(),
        kind: IndexKind::NonUnique,
        op: IndexOp::Insert,
        index_key: vec![1],
        pk_key: vec![2],
    }]);
    bytes3.push(0);
    let err = decode_index_payload(&bytes3).unwrap_err();
    assert!(matches!(
        err,
        DbError::Format(FormatError::InvalidCatalogPayload { .. })
    ));
}

#[test]
fn index_state_non_unique_delete_removes_empty_set() {
    let mut st = IndexState::default();
    let mut e = IndexEntry {
        collection_id: 1,
        index_name: "n".into(),
        kind: IndexKind::NonUnique,
        op: IndexOp::Insert,
        index_key: ScalarValue::Int64(1).canonical_key_bytes(),
        pk_key: ScalarValue::String("a".into()).canonical_key_bytes(),
    };
    st.apply(e.clone()).unwrap();

    e.op = IndexOp::Delete;
    st.apply(e).unwrap();

    assert!(st
        .non_unique_lookup(1, "n", &ScalarValue::Int64(1).canonical_key_bytes())
        .is_none());
}

#[test]
fn index_state_unique_delete_missing_key_is_ok() {
    let mut st = IndexState::default();
    let del = IndexEntry {
        collection_id: 1,
        index_name: "u".into(),
        kind: IndexKind::Unique,
        op: IndexOp::Delete,
        index_key: vec![1],
        pk_key: vec![2],
    };
    st.apply(del).unwrap();
}

#[test]
fn index_state_non_unique_delete_missing_key_is_ok() {
    let mut st = IndexState::default();
    let del = IndexEntry {
        collection_id: 1,
        index_name: "n".into(),
        kind: IndexKind::NonUnique,
        op: IndexOp::Delete,
        index_key: vec![1],
        pk_key: vec![2],
    };
    st.apply(del).unwrap();
}

#[test]
fn encode_decode_index_payload_v2_delete_op_roundtrips() {
    let entries = vec![IndexEntry {
        collection_id: 7,
        index_name: "x".into(),
        kind: IndexKind::Unique,
        op: IndexOp::Delete,
        index_key: vec![9, 9],
        pk_key: vec![1, 2, 3],
    }];
    let bytes = encode_index_payload(&entries);
    let got = decode_index_payload(&bytes).unwrap();
    assert_eq!(got, entries);
}

#[test]
fn decode_index_payload_rejects_unexpected_eof_in_tags_and_lengths() {
    // Empty buffer hits take_u16 unexpected eof.
    let err = decode_index_payload(&[]).unwrap_err();
    assert!(matches!(
        err,
        DbError::Format(FormatError::InvalidCatalogPayload { .. })
    ));

    // Version only, missing u32 count hits take_u32 unexpected eof.
    let mut b = Vec::new();
    b.extend_from_slice(&typra_core::index::INDEX_PAYLOAD_VERSION_V2.to_le_bytes());
    let err = decode_index_payload(&b).unwrap_err();
    assert!(matches!(
        err,
        DbError::Format(FormatError::InvalidCatalogPayload { .. })
    ));

    // Full header + one entry, but missing kind/op bytes hits take_u8 unexpected eof.
    let mut b2 = Vec::new();
    b2.extend_from_slice(&typra_core::index::INDEX_PAYLOAD_VERSION_V2.to_le_bytes());
    b2.extend_from_slice(&1u32.to_le_bytes()); // n
    b2.extend_from_slice(&1u32.to_le_bytes()); // collection_id
    let err = decode_index_payload(&b2).unwrap_err();
    assert!(matches!(
        err,
        DbError::Format(FormatError::InvalidCatalogPayload { .. })
    ));

    // Provide kind/op, then set index_name length > remaining to hit take_bytes unexpected eof.
    let mut b3 = Vec::new();
    b3.extend_from_slice(&typra_core::index::INDEX_PAYLOAD_VERSION_V2.to_le_bytes());
    b3.extend_from_slice(&1u32.to_le_bytes()); // n
    b3.extend_from_slice(&1u32.to_le_bytes()); // collection_id
    b3.push(1); // kind unique
    b3.push(1); // op insert
    b3.extend_from_slice(&10u32.to_le_bytes()); // name len
    let err = decode_index_payload(&b3).unwrap_err();
    assert!(matches!(
        err,
        DbError::Format(FormatError::InvalidCatalogPayload { .. })
    ));
}
