use modelvault_core::index::{IndexEntry, IndexOp, IndexState};
use modelvault_core::schema::IndexKind;
use modelvault_core::ScalarValue;

#[test]
fn non_unique_index_delete_removes_empty_set_entry() {
    let mut idx = IndexState::default();
    let e = IndexEntry {
        collection_id: 1,
        index_name: "x".to_string(),
        kind: IndexKind::NonUnique,
        op: IndexOp::Insert,
        index_key: ScalarValue::Int64(7).canonical_key_bytes(),
        pk_key: b"pk".to_vec(),
    };
    idx.apply(e).unwrap();

    idx.apply(IndexEntry {
        op: IndexOp::Delete,
        ..IndexEntry {
            collection_id: 1,
            index_name: "x".to_string(),
            kind: IndexKind::NonUnique,
            op: IndexOp::Insert,
            index_key: ScalarValue::Int64(7).canonical_key_bytes(),
            pk_key: b"pk".to_vec(),
        }
    })
    .unwrap();

    assert!(idx
        .non_unique_lookup(1, "x", &ScalarValue::Int64(7).canonical_key_bytes())
        .is_none());
}

