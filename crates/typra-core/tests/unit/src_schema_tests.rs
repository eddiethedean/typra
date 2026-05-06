    use super::*;
    use std::borrow::Cow;

    fn fp(parts: &[&'static str]) -> FieldPath {
        FieldPath::new(parts.iter().copied().map(Cow::Borrowed)).unwrap()
    }

    #[test]
    fn validate_field_defs_rejects_empty_duplicate_and_parent_child_conflict() {
        // Empty path via direct construction (bypasses FieldPath::new).
        let bad = FieldDef::new(FieldPath(vec![]), Type::Int64);
        assert!(matches!(
            validate_field_defs(&[bad]),
            Err(DbError::Schema(SchemaError::InvalidFieldPath))
        ));

        // Duplicate.
        let a1 = FieldDef::new(fp(&["a"]), Type::Int64);
        let a2 = FieldDef::new(fp(&["a"]), Type::Int64);
        assert!(matches!(
            validate_field_defs(&[a1, a2]),
            Err(DbError::Schema(SchemaError::InvalidFieldPath))
        ));

        // Parent/child conflict.
        let p = FieldDef::new(fp(&["a"]), Type::Int64);
        let c = FieldDef::new(fp(&["a", "b"]), Type::Int64);
        assert!(matches!(
            validate_field_defs(&[p, c]),
            Err(DbError::Schema(SchemaError::InvalidFieldPath))
        ));
    }

    #[test]
    fn classify_schema_update_hits_breaking_and_migration_paths() {
        // Constraints changed => Breaking.
        let mut old = FieldDef::new(fp(&["x"]), Type::Int64);
        old.constraints = vec![Constraint::MinI64(0)];
        let old_fields = vec![old.clone()];
        let old_indexes: Vec<IndexDef> = vec![];

        let mut new = FieldDef::new(fp(&["x"]), Type::Int64);
        new.constraints = vec![Constraint::MinI64(1)];
        let ch = classify_schema_update(&old_fields, &old_indexes, &[new], &[]).unwrap();
        assert!(matches!(ch, SchemaChange::Breaking { .. }));

        // Type changed => Breaking.
        let ch = classify_schema_update(
            &[FieldDef::new(fp(&["x"]), Type::Int64)],
            &[],
            &[FieldDef::new(fp(&["x"]), Type::Uint64)],
            &[],
        )
        .unwrap();
        assert!(matches!(ch, SchemaChange::Breaking { .. }));

        // New required field (non-optional) => NeedsMigration.
        let ch = classify_schema_update(
            &[FieldDef::new(fp(&["x"]), Type::Int64)],
            &[],
            &[
                FieldDef::new(fp(&["x"]), Type::Int64),
                FieldDef::new(fp(&["y"]), Type::Int64),
            ],
            &[],
        )
        .unwrap();
        assert!(matches!(ch, SchemaChange::NeedsMigration { .. }));

        // Index changed => Breaking.
        let old_idx = IndexDef {
            name: "i".to_string(),
            path: fp(&["x"]),
            kind: IndexKind::Unique,
        };
        let new_idx = IndexDef {
            name: "i".to_string(),
            path: fp(&["x"]),
            kind: IndexKind::NonUnique,
        };
        let ch = classify_schema_update(
            &[FieldDef::new(fp(&["x"]), Type::Int64)],
            &[old_idx],
            &[FieldDef::new(fp(&["x"]), Type::Int64)],
            &[new_idx],
        )
        .unwrap();
        assert!(matches!(ch, SchemaChange::Breaking { .. }));
    }

    #[test]
    fn classify_schema_update_field_removed_is_breaking() {
        let ch = classify_schema_update(
            &[
                FieldDef::new(fp(&["x"]), Type::Int64),
                FieldDef::new(fp(&["y"]), Type::Int64),
            ],
            &[],
            &[FieldDef::new(fp(&["x"]), Type::Int64)],
            &[],
        )
        .unwrap();
        match ch {
            SchemaChange::Breaking { reason } => assert!(reason.contains("field removed")),
            o => panic!("expected Breaking, got {o:?}"),
        }
    }

    #[test]
    fn classify_schema_update_new_optional_field_is_safe() {
        let ch = classify_schema_update(
            &[FieldDef::new(fp(&["x"]), Type::Int64)],
            &[],
            &[
                FieldDef::new(fp(&["x"]), Type::Int64),
                FieldDef::new(fp(&["y"]), Type::Optional(Box::new(Type::Int64))),
            ],
            &[],
        )
        .unwrap();
        assert!(matches!(ch, SchemaChange::Safe));
    }

    #[test]
    fn classify_schema_update_dropping_index_is_safe() {
        let old_idx = IndexDef {
            name: "i".to_string(),
            path: fp(&["x"]),
            kind: IndexKind::NonUnique,
        };
        let ch = classify_schema_update(
            &[FieldDef::new(fp(&["x"]), Type::Int64)],
            &[old_idx],
            &[FieldDef::new(fp(&["x"]), Type::Int64)],
            &[],
        )
        .unwrap();
        assert!(matches!(ch, SchemaChange::Safe));
    }

    #[test]
    fn classify_schema_update_adding_nonunique_index_is_safe() {
        let new_idx = IndexDef {
            name: "n".to_string(),
            path: fp(&["x"]),
            kind: IndexKind::NonUnique,
        };
        let ch = classify_schema_update(
            &[FieldDef::new(fp(&["x"]), Type::Int64)],
            &[],
            &[FieldDef::new(fp(&["x"]), Type::Int64)],
            &[new_idx],
        )
        .unwrap();
        assert!(matches!(ch, SchemaChange::Safe));
    }

    #[test]
    fn classify_schema_update_adding_unique_index_needs_migration() {
        let new_idx = IndexDef {
            name: "u".to_string(),
            path: fp(&["x"]),
            kind: IndexKind::Unique,
        };
        let ch = classify_schema_update(
            &[FieldDef::new(fp(&["x"]), Type::Int64)],
            &[],
            &[FieldDef::new(fp(&["x"]), Type::Int64)],
            &[new_idx],
        )
        .unwrap();
        assert!(matches!(ch, SchemaChange::NeedsMigration { .. }));
    }

    #[test]
    fn classify_schema_update_enum_superset_is_safe() {
        let old = FieldDef::new(
            fp(&["e"]),
            Type::Enum(vec!["a".to_string(), "b".to_string()]),
        );
        let new = FieldDef::new(
            fp(&["e"]),
            Type::Enum(vec![
                "a".to_string(),
                "b".to_string(),
                "c".to_string(),
            ]),
        );
        let ch = classify_schema_update(&[old], &[], &[new], &[]).unwrap();
        assert!(matches!(ch, SchemaChange::Safe));
    }

    #[test]
    fn classify_schema_update_enum_narrowing_is_breaking() {
        let old = FieldDef::new(
            fp(&["e"]),
            Type::Enum(vec!["a".to_string(), "b".to_string()]),
        );
        let new = FieldDef::new(fp(&["e"]), Type::Enum(vec!["a".to_string()]));
        let ch = classify_schema_update(&[old], &[], &[new], &[]).unwrap();
        assert!(matches!(ch, SchemaChange::Breaking { .. }));
    }

    #[test]
    fn classify_schema_update_list_element_change_is_breaking() {
        let ch = classify_schema_update(
            &[FieldDef::new(fp(&["x"]), Type::List(Box::new(Type::Int64)))],
            &[],
            &[FieldDef::new(fp(&["x"]), Type::List(Box::new(Type::String)))],
            &[],
        )
        .unwrap();
        assert!(matches!(ch, SchemaChange::Breaking { .. }));
    }

    #[test]
    fn classify_schema_update_optional_wrapping_is_breaking() {
        let ch = classify_schema_update(
            &[FieldDef::new(fp(&["x"]), Type::Int64)],
            &[],
            &[FieldDef::new(
                fp(&["x"]),
                Type::Optional(Box::new(Type::Int64)),
            )],
            &[],
        )
        .unwrap();
        assert!(matches!(ch, SchemaChange::Breaking { .. }));
    }
