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
