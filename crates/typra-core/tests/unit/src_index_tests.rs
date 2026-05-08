use crate::index::{IndexEntry, IndexOp, IndexState};
use crate::schema::IndexKind;
use crate::ScalarValue;

#[test]
fn non_unique_index_delete_removes_empty_set_entry() {
    let mut idx = IndexState::default();
    idx.apply(IndexEntry {
        collection_id: 1,
        index_name: "x".to_string(),
        kind: IndexKind::NonUnique,
        op: IndexOp::Insert,
        index_key: ScalarValue::Int64(7).canonical_key_bytes(),
        pk_key: b"pk".to_vec(),
    })
    .unwrap();

    idx.apply(IndexEntry {
        collection_id: 1,
        index_name: "x".to_string(),
        kind: IndexKind::NonUnique,
        op: IndexOp::Delete,
        index_key: ScalarValue::Int64(7).canonical_key_bytes(),
        pk_key: b"pk".to_vec(),
    })
    .unwrap();

    assert!(idx
        .non_unique_lookup(1, "x", &ScalarValue::Int64(7).canonical_key_bytes())
        .is_none());
}

