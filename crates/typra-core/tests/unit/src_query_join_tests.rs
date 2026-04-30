    use super::*;
    use crate::spill::TempSpillFile;
    use crate::storage::VecStore;
    use std::collections::BTreeMap;

    fn fp(s: &'static str) -> FieldPath {
        FieldPath::new([std::borrow::Cow::Borrowed(s)]).unwrap()
    }

    fn row(k: i64) -> BTreeMap<String, RowValue> {
        let mut m = BTreeMap::new();
        m.insert("k".to_string(), RowValue::Int64(k));
        m
    }

    fn row_missing() -> BTreeMap<String, RowValue> {
        BTreeMap::new()
    }

    #[test]
    fn decode_entries_truncated_errors() {
        let err = decode_entries(&[]).unwrap_err();
        assert!(matches!(err, DbError::Query(_)));

        let mut buf = Vec::new();
        buf.extend_from_slice(&1u32.to_le_bytes());
        buf.extend_from_slice(&0i64.to_le_bytes());
        let err = decode_entries(&buf).unwrap_err();
        assert!(matches!(err, DbError::Query(_)));
    }

    #[test]
    fn join_rejects_zero_budget_and_requires_spill_store_when_over_budget() {
        let err = spillable_hash_join_match_count_i64::<_, _, VecStore>(
            std::iter::once(Ok(row(1))),
            std::iter::once(Ok(row(1))),
            &fp("k"),
            &fp("k"),
            0,
            None,
        )
        .unwrap_err();
        assert!(matches!(err, DbError::Query(_)));

        // Over budget with no spill store: 2 distinct keys with budget 1.
        let left = vec![Ok(row(1)), Ok(row(2))].into_iter();
        let right = vec![Ok(row(1))].into_iter();
        let err = spillable_hash_join_match_count_i64::<_, _, VecStore>(
            left,
            right,
            &fp("k"),
            &fp("k"),
            1,
            None,
        )
        .unwrap_err();
        assert!(matches!(err, DbError::Query(_)));
    }

    #[test]
    fn join_no_spill_path_counts_matches() {
        let left = vec![Ok(row_missing()), Ok(row(1)), Ok(row(1)), Ok(row(2))].into_iter();
        let right = vec![Ok(row_missing()), Ok(row(1)), Ok(row(3)), Ok(row(1))].into_iter();
        let total = spillable_hash_join_match_count_i64::<_, _, VecStore>(
            left,
            right,
            &fp("k"),
            &fp("k"),
            10,
            None,
        )
        .unwrap();
        // left(1)=2, right(1)=2 => 4 matches; key 2 has 0 matches.
        assert_eq!(total, 4);
    }

    #[test]
    fn join_spills_and_merges_partitions() {
        let left = vec![Ok(row(1)), Ok(row(2)), Ok(row(1)), Ok(row(3))].into_iter();
        let right = vec![Ok(row(1)), Ok(row(1)), Ok(row(2))].into_iter();

        let base = VecStore::new();
        let mut spill = TempSpillFile::new(base).unwrap();
        let total = spillable_hash_join_match_count_i64(
            left,
            right,
            &fp("k"),
            &fp("k"),
            1,
            Some(&mut spill),
        )
        .unwrap();

        // left counts: 1->2,2->1,3->1 ; right: 1->2,2->1 => 2*2 + 1*1 = 5
        assert_eq!(total, 5);
    }
