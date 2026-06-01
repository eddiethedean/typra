    use super::*;
    use crate::spill::TempSpillFile;
    use crate::storage::VecStore;
    use std::collections::BTreeMap;

    fn fp(s: &'static str) -> FieldPath {
        FieldPath::new([std::borrow::Cow::Borrowed(s)]).unwrap()
    }

    fn row(k: i64, v: i64) -> BTreeMap<String, RowValue> {
        let mut m = BTreeMap::new();
        m.insert("g".to_string(), RowValue::Int64(k));
        m.insert("v".to_string(), RowValue::Int64(v));
        m
    }

    fn row_missing(key: &'static str) -> BTreeMap<String, RowValue> {
        let mut m = BTreeMap::new();
        m.insert(key.to_string(), RowValue::Int64(1));
        m
    }

    #[test]
    fn decode_partition_entries_truncated_errors() {
        let err = decode_partition_entries(&[]).unwrap_err();
        assert!(matches!(err, DbError::Query(_)));

        // Claims 1 entry but missing bytes.
        let mut buf = Vec::new();
        buf.extend_from_slice(&1u32.to_le_bytes());
        buf.extend_from_slice(&0i64.to_le_bytes());
        let err = decode_partition_entries(&buf).unwrap_err();
        assert!(matches!(err, DbError::Query(_)));
    }

    #[test]
    fn group_count_sum_rejects_zero_budget_and_requires_spill_store_when_over_budget() {
        let rows = std::iter::once(Ok(row(1, 1)));
        let err =
            spillable_group_count_sum_i64::<_, VecStore>(rows, &fp("g"), &fp("v"), 0, None)
                .unwrap_err();
        assert!(matches!(err, DbError::Query(_)));

        // Over budget with no spill store.
        let rows = vec![Ok(row(1, 1)), Ok(row(2, 1))].into_iter();
        let err =
            spillable_group_count_sum_i64::<_, VecStore>(rows, &fp("g"), &fp("v"), 1, None)
                .unwrap_err();
        assert!(matches!(err, DbError::Query(_)));
    }

    #[test]
    fn group_count_sum_spills_and_merges_partitions() {
        let group_by = fp("g");
        let sum_field = fp("v");

        // Force spilling: budget 1, but 3 distinct groups.
        let rows = vec![
            Ok(row(1, 10)),
            Ok(row_missing("g")), // ignored (missing sum/group)
            Ok(row_missing("v")),
            Ok(row(2, 1)),
            Ok(row(3, 2)),
            Ok(row(1, 5)),
        ]
        .into_iter();

        let base = VecStore::new();
        let mut spill = TempSpillFile::new(base).unwrap();
        let out = spillable_group_count_sum_i64(rows, &group_by, &sum_field, 1, Some(&mut spill))
            .unwrap();

        assert_eq!(out, vec![(1, 2, 15), (2, 1, 1), (3, 1, 2)]);
    }
