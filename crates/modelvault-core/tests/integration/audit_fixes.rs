//! Regression tests for security/correctness audit fixes.

use modelvault_core::index::IndexState;
use modelvault_core::schema::IndexKind;
use modelvault_core::ScalarValue;

#[test]
fn uint64_index_range_lookup_respects_unsigned_order() {
    let hi = u64::MAX;
    let lo = hi - 1;
    let mut indexes = IndexState::default();
    let collection_id = 1u32;
    let index_name = "u_idx";
    indexes
        .apply(modelvault_core::index::IndexEntry {
            collection_id,
            index_name: index_name.to_string(),
            kind: IndexKind::NonUnique,
            op: modelvault_core::index::IndexOp::Insert,
            index_key: ScalarValue::Uint64(lo).canonical_key_bytes(),
            pk_key: b"pk_lo".to_vec(),
        })
        .unwrap();
    indexes
        .apply(modelvault_core::index::IndexEntry {
            collection_id,
            index_name: index_name.to_string(),
            kind: IndexKind::NonUnique,
            op: modelvault_core::index::IndexOp::Insert,
            index_key: ScalarValue::Uint64(hi).canonical_key_bytes(),
            pk_key: b"pk_hi".to_vec(),
        })
        .unwrap();

    let pks = indexes.non_unique_range_lookup(
        collection_id,
        index_name,
        Some(&ScalarValue::Uint64(lo)),
        true,
        Some(&ScalarValue::Uint64(hi)),
        true,
    );
    assert_eq!(pks.len(), 2);

    let mid_only = indexes.non_unique_range_lookup(
        collection_id,
        index_name,
        Some(&ScalarValue::Uint64(hi)),
        true,
        Some(&ScalarValue::Uint64(hi)),
        true,
    );
    assert_eq!(mid_only.len(), 1);
    assert_eq!(mid_only[0], b"pk_hi");
}

#[test]
fn aggregation_sum_overflow_returns_error() {
    use modelvault_core::query::spillable_group_count_sum_i64;
    use modelvault_core::schema::FieldPath;
    use std::borrow::Cow;
    use std::collections::BTreeMap;

    let group_by = FieldPath::new([Cow::Borrowed("g")]).unwrap();
    let sum_field = FieldPath::new([Cow::Borrowed("v")]).unwrap();

    let mut row = BTreeMap::new();
    row.insert(
        "g".to_string(),
        modelvault_core::record::RowValue::from_scalar(ScalarValue::Int64(1)),
    );
    row.insert(
        "v".to_string(),
        modelvault_core::record::RowValue::from_scalar(ScalarValue::Int64(i64::MAX)),
    );
    let rows = vec![row.clone(), row];

    use modelvault_core::storage::VecStore;

    let err = spillable_group_count_sum_i64::<_, VecStore>(
        rows.into_iter().map(Ok),
        &group_by,
        &sum_field,
        1024,
        None,
    )
    .unwrap_err();
    let msg = format!("{err}");
    assert!(
        msg.contains("overflow"),
        "expected overflow error, got {msg}"
    );
}
