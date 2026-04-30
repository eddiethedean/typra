    use std::borrow::Cow;
    use std::collections::BTreeMap;

    use crate::db::scalar_at_path;
    use crate::record::RowValue;
    use crate::schema::FieldPath;
    use crate::ScalarValue;

    fn fp(parts: &[&'static str]) -> FieldPath {
        FieldPath(parts.iter().copied().map(Cow::Borrowed).collect())
    }

    #[test]
    fn scalar_at_path_empty_path_is_none() {
        let row: BTreeMap<String, RowValue> = BTreeMap::new();
        assert!(scalar_at_path(&row, &FieldPath(vec![])).is_none());
    }

    #[test]
    fn scalar_at_path_none_parent_is_none() {
        let row = BTreeMap::from([("a".into(), RowValue::None)]);
        assert!(scalar_at_path(&row, &fp(&["a", "b"])).is_none());
    }

    #[test]
    fn scalar_at_path_non_object_parent_is_none() {
        let row = BTreeMap::from([("a".into(), RowValue::Int64(1))]);
        assert!(scalar_at_path(&row, &fp(&["a", "b"])).is_none());
    }

    #[test]
    fn scalar_at_path_finds_nested_scalar() {
        let row = BTreeMap::from([(
            "a".into(),
            RowValue::Object(BTreeMap::from([("b".into(), RowValue::Int64(7))])),
        )]);
        assert_eq!(
            scalar_at_path(&row, &fp(&["a", "b"])),
            Some(ScalarValue::Int64(7))
        );
    }
