    use super::Database;
    use crate::db::open;
    use crate::db::write;
    use crate::error::FormatError;
    use crate::error::{SchemaError, ValidationError};
    use crate::file_format::{FileHeader, FILE_HEADER_SIZE};
    use crate::index::{encode_index_payload, IndexEntry};
    use crate::schema::{CollectionId, FieldDef, Type};
    use crate::segments::header::{SegmentHeader, SegmentType};
    use crate::segments::writer::SegmentWriter;
    use crate::storage::{FileStore, Store};
    use crate::superblock::{Superblock, SUPERBLOCK_SIZE};
    use crate::DbError;
    use std::borrow::Cow;
    use std::collections::BTreeMap;

    fn new_store() -> FileStore {
        let f = tempfile::NamedTempFile::new().unwrap();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(f.path())
            .unwrap();
        FileStore::new(file)
    }

    fn path_field(name: &str) -> FieldDef {
        FieldDef {
            path: crate::schema::FieldPath(vec![Cow::Owned(name.to_string())]),
            ty: Type::String,
            constraints: vec![],
        }
    }

    #[test]
    fn transaction_api_nested_begin_and_commit_without_begin_are_ok() {
        let mut db = crate::db::Database::<crate::storage::VecStore>::open_in_memory().unwrap();

        // Committing without a transaction is a no-op.
        db.commit_transaction().unwrap();

        db.begin_transaction().unwrap();
        let e = db.begin_transaction().unwrap_err();
        assert!(matches!(
            e,
            DbError::Transaction(crate::error::TransactionError::NestedTransaction)
        ));
        db.rollback_transaction();
        // rollback without begin is fine
        db.rollback_transaction();
    }

    #[test]
    fn transaction_closure_rolls_back_on_error_and_commits_on_success() {
        let mut db = crate::db::Database::<crate::storage::VecStore>::open_in_memory().unwrap();

        // Error path rolls back.
        let err = db
            .transaction(|_| {
                Err::<(), DbError>(DbError::Format(FormatError::InvalidCatalogPayload {
                message: "boom".into(),
            }))
            })
            .unwrap_err();
        assert!(matches!(err, DbError::Format(_)));
        assert!(db.txn_staging.is_none());

        // Success path commits.
        db.transaction(|_| Ok::<_, DbError>(())).unwrap();
        assert!(db.txn_staging.is_none());
    }

    #[test]
    fn transaction_closure_errors_when_called_inside_active_transaction() {
        let mut db = crate::db::Database::<crate::storage::VecStore>::open_in_memory().unwrap();
        db.begin_transaction().unwrap();
        let e = db.transaction(|_| Ok::<(), DbError>(())).unwrap_err();
        assert!(matches!(
            e,
            DbError::Transaction(crate::error::TransactionError::NestedTransaction)
        ));
        db.rollback_transaction();
    }

    #[test]
    fn commit_txn_staging_writes_pending_segments_and_updates_shadow_state() {
        let mut db = crate::db::Database::<crate::storage::VecStore>::open_in_memory().unwrap();
        db.begin_transaction().unwrap();

        // This should stage a schema segment (pending non-empty).
        let fields = vec![FieldDef::new(
            crate::schema::FieldPath(vec![Cow::Borrowed("id")]),
            Type::String,
        )];
        let (cid, _v1) = db.register_collection("t", fields, "id").unwrap();

        db.commit_transaction().unwrap();
        assert!(db.catalog().get(cid).is_some());
        assert!(db.txn_staging.is_none());
    }

    #[test]
    fn index_state_returns_secondary_index_snapshot() {
        let db = crate::db::Database::<crate::storage::VecStore>::open_in_memory().unwrap();
        let _ = db.index_state();
    }

    #[test]
    fn compact_snapshot_bytes_bumps_schema_versions() {
        let mut db = crate::db::Database::<crate::storage::VecStore>::open_in_memory().unwrap();
        let fields = vec![FieldDef::new(
            crate::schema::FieldPath(vec![Cow::Borrowed("id")]),
            Type::String,
        )];
        let (cid, _v1) = db.register_collection("t", fields.clone(), "id").unwrap();

        // Force schema version > 1 so compaction's bump loop runs.
        db.register_schema_version_with_indexes_force(cid, fields.clone(), vec![])
            .unwrap();
        db.register_schema_version_with_indexes_force(cid, fields.clone(), vec![])
            .unwrap();

        db.insert(
            cid,
            BTreeMap::from([("id".to_string(), crate::RowValue::String("k".to_string()))]),
        )
        .unwrap();

        let bytes = db.compact_snapshot_bytes().unwrap();
        let compacted = crate::db::Database::<crate::storage::VecStore>::from_snapshot_bytes(bytes)
            .unwrap();
        let col = compacted.catalog().get(cid).unwrap();
        assert!(col.current_version.0 >= 3);
    }

    #[test]
    fn delete_missing_is_ok_and_delete_existing_removes_row() {
        let mut db = crate::db::Database::<crate::storage::VecStore>::open_in_memory().unwrap();
        let fields = vec![
            FieldDef::new(crate::schema::FieldPath(vec![Cow::Borrowed("id")]), Type::Int64),
            FieldDef::new(crate::schema::FieldPath(vec![Cow::Borrowed("x")]), Type::Int64),
        ];
        let (cid, _) = db.register_collection("t", fields, "id").unwrap();

        // Missing key is a no-op.
        db.delete(cid, &crate::ScalarValue::Int64(1)).unwrap();

        db.insert(
            cid,
            BTreeMap::from([
                ("id".to_string(), crate::RowValue::Int64(1)),
                ("x".to_string(), crate::RowValue::Int64(10)),
            ]),
        )
        .unwrap();
        db.delete(cid, &crate::ScalarValue::Int64(1)).unwrap();
        assert!(db.get(cid, &crate::ScalarValue::Int64(1)).unwrap().is_none());
    }

    #[test]
    fn row_subset_and_merge_paths_exercise_nested_helpers() {
        let row = BTreeMap::from([(
            "a".to_string(),
            crate::RowValue::Object(BTreeMap::from([
                ("b".to_string(), crate::RowValue::Int64(1)),
                ("c".to_string(), crate::RowValue::None),
            ])),
        )]);
        let wanted = vec![
            FieldDef::new(
                crate::schema::FieldPath(vec![Cow::Borrowed("a"), Cow::Borrowed("b")]),
                Type::Int64,
            ),
            FieldDef::new(
                crate::schema::FieldPath(vec![Cow::Borrowed("a"), Cow::Borrowed("c")]),
                Type::Optional(Box::new(Type::Int64)),
            ),
        ];
        let out = crate::db::row_subset_by_field_defs(&row, &wanted);
        assert!(out.contains_key("a"));
    }

    #[test]
    fn validate_subset_model_error_paths() {
        #[derive(Clone)]
        struct M;
        impl crate::schema::DbModel for M {
            fn collection_name() -> &'static str {
                "t"
            }
            fn primary_field() -> &'static str {
                "wrong_pk"
            }
            fn fields() -> Vec<FieldDef> {
                vec![FieldDef::new(
                    crate::schema::FieldPath(vec![Cow::Borrowed("id")]),
                    Type::Int64,
                )]
            }
            fn indexes() -> Vec<crate::schema::IndexDef> {
                vec![]
            }
        }

        let mut db = crate::db::Database::<crate::storage::VecStore>::open_in_memory().unwrap();
        let fields =
            vec![FieldDef::new(crate::schema::FieldPath(vec![Cow::Borrowed("id")]), Type::Int64)];
        let (_cid, _) = db.register_collection("t", fields, "id").unwrap();

        let e = match db.collection::<M>() {
            Ok(_) => panic!("expected error"),
            Err(e) => e,
        };
        assert!(matches!(e, crate::DbError::Schema(_)));
    }

    #[test]
    fn row_value_as_object_map_covers_non_object_arm() {
        assert!(crate::RowValue::Int64(0).as_object_map().is_none());
        let m = BTreeMap::new();
        assert!(crate::RowValue::Object(m).as_object_map().is_some());
    }

    #[test]
    fn collection_returns_unknown_collection_name_when_model_not_registered() {
        #[derive(Clone)]
        struct Missing;
        impl crate::schema::DbModel for Missing {
            fn collection_name() -> &'static str {
                "no_such_table_for_collection_api_test"
            }
            fn primary_field() -> &'static str {
                "id"
            }
            fn fields() -> Vec<FieldDef> {
                vec![FieldDef::new(
                    crate::schema::FieldPath(vec![Cow::Borrowed("id")]),
                    Type::Int64,
                )]
            }
            fn indexes() -> Vec<crate::schema::IndexDef> {
                vec![]
            }
        }

        let db = crate::db::Database::<crate::storage::VecStore>::open_in_memory().unwrap();
        let e = match db.collection::<Missing>() {
            Ok(_) => panic!("expected UnknownCollectionName"),
            Err(e) => e,
        };
        assert!(matches!(
            e,
            DbError::Schema(SchemaError::UnknownCollectionName { .. })
        ));
    }

    #[test]
    fn plan_insert_row_multi_segment_and_index_missing_path_are_covered() {
        let mut db = crate::db::Database::<crate::storage::VecStore>::open_in_memory().unwrap();
        let fields = vec![
            FieldDef::new(crate::schema::FieldPath(vec![Cow::Borrowed("id")]), Type::Int64),
            FieldDef::new(
                crate::schema::FieldPath(vec![Cow::Borrowed("obj"), Cow::Borrowed("n"), Cow::Borrowed("x")]),
                Type::Int64,
            ),
        ];
        let indexes = vec![
            crate::schema::IndexDef {
                name: "idx_missing".to_string(),
                path: crate::schema::FieldPath(vec![Cow::Borrowed("missing")]),
                kind: crate::schema::IndexKind::NonUnique,
            },
            crate::schema::IndexDef {
                name: "idx_obj".to_string(),
                path: crate::schema::FieldPath(vec![Cow::Borrowed("obj"), Cow::Borrowed("n"), Cow::Borrowed("x")]),
                kind: crate::schema::IndexKind::NonUnique,
            },
        ];
        let (cid, _) = db
            .register_collection_with_indexes("t", fields, indexes, "id")
            .unwrap();

        let mut row = BTreeMap::new();
        row.insert("id".to_string(), crate::RowValue::Int64(1));
        row.insert(
            "obj".to_string(),
            crate::RowValue::Object(BTreeMap::from([(
                "n".to_string(),
                crate::RowValue::Object(BTreeMap::from([("x".to_string(), crate::RowValue::Int64(7))])),
            )])),
        );

        let (payload, (_pk, full), idx_entries, _pk_scalar) =
            super::plan_insert_row(db.catalog(), cid, row).unwrap();
        assert!(!payload.is_empty());
        assert!(full.contains_key("obj"));
        assert_eq!(idx_entries.len(), 1);
        assert_eq!(idx_entries[0].index_name, "idx_obj");
    }

    #[test]
    fn plan_insert_row_unknown_collection_errors() {
        let db = crate::db::Database::<crate::storage::VecStore>::open_in_memory().unwrap();
        let row = BTreeMap::from([("id".to_string(), crate::RowValue::Int64(1))]);

        let err = super::plan_insert_row(db.catalog(), CollectionId(999), row).unwrap_err();
        assert!(matches!(err, DbError::Schema(SchemaError::UnknownCollection { id: 999 })));
    }

    #[test]
    fn plan_insert_row_no_primary_key_errors() {
        let mut catalog = crate::catalog::Catalog::default();
        catalog
            .apply_record(crate::catalog::CatalogRecordWire::CreateCollection {
                collection_id: 1,
                name: "t".into(),
                schema_version: 1,
                fields: vec![path_field("id")],
                indexes: vec![],
                primary_field: None,
            })
            .unwrap();

        let row = BTreeMap::from([("id".to_string(), crate::RowValue::String("k".into()))]);
        let err = super::plan_insert_row(&catalog, CollectionId(1), row).unwrap_err();
        assert!(matches!(
            err,
            DbError::Schema(SchemaError::NoPrimaryKey { collection_id: 1 })
        ));
    }

    #[test]
    fn plan_insert_row_rejects_non_primitive_primary_key_type() {
        let mut catalog = crate::catalog::Catalog::default();
        catalog
            .apply_record(crate::catalog::CatalogRecordWire::CreateCollection {
                collection_id: 1,
                name: "t".into(),
                schema_version: 1,
                fields: vec![FieldDef::new(
                    crate::schema::FieldPath(vec![Cow::Borrowed("id")]),
                    Type::Object(vec![]),
                )],
                indexes: vec![],
                primary_field: Some("id".into()),
            })
            .unwrap();

        let row = BTreeMap::from([(
            "id".to_string(),
            crate::RowValue::Object(BTreeMap::new()),
        )]);
        let err = super::plan_insert_row(&catalog, CollectionId(1), row).unwrap_err();
        assert!(matches!(
            err,
            DbError::Validation(ValidationError { .. })
        ));
    }

    #[test]
    fn plan_insert_row_row_missing_primary_key_errors() {
        let mut catalog = crate::catalog::Catalog::default();
        catalog
            .apply_record(crate::catalog::CatalogRecordWire::CreateCollection {
                collection_id: 1,
                name: "t".into(),
                schema_version: 1,
                fields: vec![FieldDef::new(
                    crate::schema::FieldPath(vec![Cow::Borrowed("id")]),
                    Type::Int64,
                )],
                indexes: vec![],
                primary_field: Some("id".into()),
            })
            .unwrap();

        let row = BTreeMap::new();
        let err = super::plan_insert_row(&catalog, CollectionId(1), row).unwrap_err();
        assert!(matches!(
            err,
            DbError::Schema(SchemaError::RowMissingPrimary { .. })
        ));
    }

    #[test]
    fn plan_insert_row_multi_segment_unknown_field_errors() {
        let mut db = crate::db::Database::<crate::storage::VecStore>::open_in_memory().unwrap();
        let fields = vec![
            FieldDef::new(crate::schema::FieldPath(vec![Cow::Borrowed("id")]), Type::Int64),
            FieldDef::new(
                crate::schema::FieldPath(vec![Cow::Borrowed("obj"), Cow::Borrowed("n"), Cow::Borrowed("x")]),
                Type::Int64,
            ),
        ];
        let (cid, _) = db.register_collection("t", fields, "id").unwrap();

        // Unknown leaf at obj.n.y (schema only defines obj.n.x).
        let row = BTreeMap::from([
            ("id".to_string(), crate::RowValue::Int64(1)),
            (
                "obj".to_string(),
                crate::RowValue::Object(BTreeMap::from([(
                    "n".to_string(),
                    crate::RowValue::Object(BTreeMap::from([("y".to_string(), crate::RowValue::Int64(7))])),
                )])),
            ),
        ]);
        let err = super::plan_insert_row(db.catalog(), cid, row).unwrap_err();
        assert!(matches!(err, DbError::Schema(SchemaError::RowUnknownField { .. })));
    }

    #[test]
    fn plan_insert_row_legacy_unknown_top_level_field_is_validation_error() {
        // Legacy single-segment schema uses validate_top_level_row.
        let mut db = crate::db::Database::<crate::storage::VecStore>::open_in_memory().unwrap();
        let fields = vec![
            FieldDef::new(crate::schema::FieldPath(vec![Cow::Borrowed("id")]), Type::Int64),
            FieldDef::new(crate::schema::FieldPath(vec![Cow::Borrowed("x")]), Type::Int64),
        ];
        let (cid, _) = db.register_collection("t", fields, "id").unwrap();

        let row = BTreeMap::from([
            ("id".to_string(), crate::RowValue::Int64(1)),
            ("x".to_string(), crate::RowValue::Int64(2)),
            ("unknown".to_string(), crate::RowValue::Int64(3)),
        ]);
        let err = super::plan_insert_row(db.catalog(), cid, row).unwrap_err();
        assert!(matches!(err, DbError::Validation(ValidationError { .. })));
    }

    #[test]
    fn plan_insert_row_returns_missing_field_when_intermediate_is_not_object() {
        let mut db = crate::db::Database::<crate::storage::VecStore>::open_in_memory().unwrap();
        let fields = vec![
            FieldDef::new(crate::schema::FieldPath(vec![Cow::Borrowed("id")]), Type::Int64),
            FieldDef::new(
                crate::schema::FieldPath(vec![Cow::Borrowed("obj"), Cow::Borrowed("n"), Cow::Borrowed("x")]),
                Type::Int64,
            ),
        ];
        let (cid, _) = db.register_collection("t", fields, "id").unwrap();

        // `obj` exists but is not an object, so nested lookup returns None.
        let row = BTreeMap::from([
            ("id".to_string(), crate::RowValue::Int64(1)),
            ("obj".to_string(), crate::RowValue::Int64(123)),
        ]);

        let e = super::plan_insert_row(db.catalog(), cid, row).unwrap_err();
        assert!(matches!(e, crate::DbError::Schema(_)));
    }

    #[test]
    fn plan_insert_row_returns_missing_field_when_nested_object_key_is_absent() {
        let mut db = crate::db::Database::<crate::storage::VecStore>::open_in_memory().unwrap();
        let fields = vec![
            FieldDef::new(crate::schema::FieldPath(vec![Cow::Borrowed("id")]), Type::Int64),
            FieldDef::new(
                crate::schema::FieldPath(vec![Cow::Borrowed("obj"), Cow::Borrowed("n"), Cow::Borrowed("x")]),
                Type::Int64,
            ),
        ];
        let (cid, _) = db.register_collection("t", fields, "id").unwrap();

        // `obj` is an object, but missing the `n` key.
        let row = BTreeMap::from([
            ("id".to_string(), crate::RowValue::Int64(1)),
            ("obj".to_string(), crate::RowValue::Object(BTreeMap::new())),
        ]);

        let e = super::plan_insert_row(db.catalog(), cid, row).unwrap_err();
        assert!(matches!(
            e,
            crate::DbError::Schema(SchemaError::RowMissingField { .. })
        ));
    }

    #[test]
    fn plan_schema_version_with_indexes_covers_safe_and_breaking_arms() {
        let mut db = crate::db::Database::<crate::storage::VecStore>::open_in_memory().unwrap();
        let fields = vec![
            FieldDef::new(crate::schema::FieldPath(vec![Cow::Borrowed("id")]), Type::Int64),
            FieldDef::new(crate::schema::FieldPath(vec![Cow::Borrowed("x")]), Type::Int64),
        ];
        let (cid, _) = db.register_collection("t", fields.clone(), "id").unwrap();

        // Safe: identical schema.
        let p = db.plan_schema_version_with_indexes(cid, fields.clone(), vec![]).unwrap();
        assert!(matches!(p.change, crate::schema::SchemaChange::Safe));

        // Breaking: drop the primary key.
        let p = db
            .plan_schema_version_with_indexes(
                cid,
                vec![FieldDef::new(
                    crate::schema::FieldPath(vec![Cow::Borrowed("x")]),
                    Type::Int64,
                )],
                vec![],
            )
            .unwrap();
        assert!(matches!(p.change, crate::schema::SchemaChange::Breaking { .. }));
    }

    #[test]
    fn plan_schema_version_unknown_collection_errors() {
        let db = crate::db::Database::<crate::storage::VecStore>::open_in_memory().unwrap();
        let e = db
            .plan_schema_version_with_indexes(CollectionId(999_999), vec![], vec![])
            .unwrap_err();
        assert!(matches!(
            e,
            DbError::Schema(SchemaError::UnknownCollection { id }) if id == 999_999
        ));
    }

    #[test]
    fn register_collection_duplicate_name_in_transaction_errors_on_staging_apply() {
        let mut db = crate::db::Database::<crate::storage::VecStore>::open_in_memory().unwrap();
        let fields = vec![FieldDef::new(
            crate::schema::FieldPath(vec![Cow::Borrowed("id")]),
            Type::Int64,
        )];
        db.begin_transaction().unwrap();
        db.register_collection("t", fields.clone(), "id").unwrap();
        let e = db.register_collection("t", fields, "id").unwrap_err();
        assert!(matches!(
            e,
            DbError::Schema(SchemaError::DuplicateCollectionName { .. })
        ));
        db.rollback_transaction();
    }

    #[test]
    fn backfill_unknown_collection_errors() {
        let mut db = crate::db::Database::<crate::storage::VecStore>::open_in_memory().unwrap();
        let e = db
            .backfill_top_level_field_with_value(
                CollectionId(99_999),
                "extra",
                crate::RowValue::Int64(1),
            )
            .unwrap_err();
        assert!(matches!(
            e,
            DbError::Schema(SchemaError::UnknownCollection { id }) if id == 99_999
        ));
    }

    #[test]
    fn rebuild_indexes_unknown_collection_errors() {
        let mut db = crate::db::Database::<crate::storage::VecStore>::open_in_memory().unwrap();
        let e = db.rebuild_indexes_for_collection(CollectionId(88_888)).unwrap_err();
        assert!(matches!(
            e,
            DbError::Schema(SchemaError::UnknownCollection { id }) if id == 88_888
        ));
    }

    #[test]
    fn backfill_top_level_field_skips_other_collections() {
        let mut db = crate::db::Database::<crate::storage::VecStore>::open_in_memory().unwrap();
        let fields = vec![
            FieldDef::new(crate::schema::FieldPath(vec![Cow::Borrowed("id")]), Type::Int64),
            FieldDef::new(crate::schema::FieldPath(vec![Cow::Borrowed("x")]), Type::Int64),
            FieldDef::new(
                crate::schema::FieldPath(vec![Cow::Borrowed("y")]),
                Type::Optional(Box::new(Type::Int64)),
            ),
        ];
        let (a, _) = db.register_collection("a", fields.clone(), "id").unwrap();
        let (b, _) = db.register_collection("b", fields, "id").unwrap();

        db.insert(
            a,
            BTreeMap::from([
                ("id".to_string(), crate::RowValue::Int64(1)),
                ("x".to_string(), crate::RowValue::Int64(10)),
            ]),
        )
        .unwrap();
        db.insert(
            b,
            BTreeMap::from([
                ("id".to_string(), crate::RowValue::Int64(1)),
                ("x".to_string(), crate::RowValue::Int64(20)),
            ]),
        )
        .unwrap();

        db.backfill_top_level_field_with_value(a, "y", crate::RowValue::Int64(7))
            .unwrap();

        // Backfill should not affect the other collection.
        let b_row = db.get(b, &crate::ScalarValue::Int64(1)).unwrap().unwrap();
        let y = b_row.get("y").cloned();
        assert!(matches!(y, None | Some(crate::RowValue::None)));
    }

    #[test]
    fn rebuild_indexes_for_collection_skips_other_collections_and_filters_rows() {
        let mut db = crate::db::Database::<crate::storage::VecStore>::open_in_memory().unwrap();
        let fields = vec![
            FieldDef::new(crate::schema::FieldPath(vec![Cow::Borrowed("id")]), Type::Int64),
            FieldDef::new(
                crate::schema::FieldPath(vec![Cow::Borrowed("x")]),
                Type::Optional(Box::new(Type::Int64)),
            ),
        ];
        let idx = crate::schema::IndexDef {
            name: "x".to_string(),
            path: crate::schema::FieldPath(vec![Cow::Borrowed("x")]),
            kind: crate::schema::IndexKind::NonUnique,
        };
        let (a, _) = db
            .register_collection_with_indexes("a", fields.clone(), vec![idx], "id")
            .unwrap();
        let (b, _) = db.register_collection("b", fields, "id").unwrap();

        // Normal row in `a` but missing x; scalar_at_path should be None and skipped.
        db.insert(a, BTreeMap::from([("id".to_string(), crate::RowValue::Int64(1))]))
            .unwrap();
        // Another collection should be ignored by rebuild loop.
        db.insert(b, BTreeMap::from([("id".to_string(), crate::RowValue::Int64(1))]))
            .unwrap();

        // Inject a corrupt row missing PK and a row with PK type mismatch into `a` to hit filters.
        db.latest.insert((a.0, b"no_pk".to_vec()), BTreeMap::from([("x".to_string(), crate::RowValue::Int64(1))]));
        db.latest.insert(
            (a.0, b"bad_ty".to_vec()),
            BTreeMap::from([("id".to_string(), crate::RowValue::String("not_int".to_string()))]),
        );

        db.rebuild_indexes_for_collection(a).unwrap();
    }

    #[test]
    fn validate_subset_model_type_mismatch_errors() {
        #[derive(Clone)]
        struct M;
        impl crate::schema::DbModel for M {
            fn collection_name() -> &'static str {
                "t"
            }
            fn primary_field() -> &'static str {
                "id"
            }
            fn fields() -> Vec<FieldDef> {
                vec![FieldDef::new(crate::schema::FieldPath(vec![Cow::Borrowed("id")]), Type::String)]
            }
            fn indexes() -> Vec<crate::schema::IndexDef> {
                vec![]
            }
        }

        let mut db = crate::db::Database::<crate::storage::VecStore>::open_in_memory().unwrap();
        let fields = vec![FieldDef::new(crate::schema::FieldPath(vec![Cow::Borrowed("id")]), Type::Int64)];
        let (_cid, _) = db.register_collection("t", fields, "id").unwrap();

        let e = match db.collection::<M>() {
            Ok(_) => panic!("expected error"),
            Err(e) => e,
        };
        assert!(matches!(e, crate::DbError::Format(_)));
    }

    #[test]
    fn row_subset_and_scalar_at_path_edge_branches_are_exercised() {
        // row_subset_by_field_defs: empty path -> skipped.
        let row = BTreeMap::from([(
            "a".to_string(),
            crate::RowValue::Object(BTreeMap::from([("b".to_string(), crate::RowValue::Int64(1))])),
        )]);
        let wanted = vec![
            FieldDef::new(crate::schema::FieldPath(vec![]), Type::Int64),
            FieldDef::new(crate::schema::FieldPath(vec![Cow::Borrowed("a"), Cow::Borrowed("b")]), Type::Int64),
            FieldDef::new(crate::schema::FieldPath(vec![Cow::Borrowed("a"), Cow::Borrowed("c")]), Type::Int64),
        ];
        let out = crate::db::row_subset_by_field_defs(&row, &wanted);
        assert!(out.contains_key("a"));

        // scalar_at_path: RowValue::None and non-object parent both return None.
        let row2 = BTreeMap::from([("a".to_string(), crate::RowValue::None)]);
        let p = crate::schema::FieldPath(vec![Cow::Borrowed("a"), Cow::Borrowed("b")]);
        assert!(crate::db::scalar_at_path(&row2, &p).is_none());
        let row3 = BTreeMap::from([("a".to_string(), crate::RowValue::Int64(1))]);
        assert!(crate::db::scalar_at_path(&row3, &p).is_none());
    }

    #[test]
    fn merge_row_value_trees_occupied_branch_recurses() {
        let mut into = crate::RowValue::Object(BTreeMap::from([(
            "a".to_string(),
            crate::RowValue::Object(BTreeMap::from([("b".to_string(), crate::RowValue::Int64(1))])),
        )]));
        let from = crate::RowValue::Object(BTreeMap::from([(
            "a".to_string(),
            crate::RowValue::Object(BTreeMap::from([("b".to_string(), crate::RowValue::Int64(2))])),
        )]));
        super::merge_row_value_trees(&mut into, from);
        let crate::RowValue::Object(m) = into else { panic!("expected object"); };
        assert!(m.contains_key("a"));
    }

    #[test]
    fn validate_subset_model_covers_no_primary_key_and_empty_path_unknown_field() {
        #[derive(Clone)]
        struct M;
        impl crate::schema::DbModel for M {
            fn collection_name() -> &'static str {
                "t"
            }
            fn primary_field() -> &'static str {
                "id"
            }
            fn fields() -> Vec<FieldDef> {
                vec![FieldDef::new(crate::schema::FieldPath(vec![]), Type::Int64)]
            }
            fn indexes() -> Vec<crate::schema::IndexDef> {
                vec![]
            }
        }

        let col_no_pk = crate::catalog::CollectionInfo {
            id: crate::schema::CollectionId(1),
            name: "t".to_string(),
            current_version: crate::schema::SchemaVersion(1),
            fields: vec![],
            indexes: vec![],
            primary_field: None,
        };
        let e = super::validate_subset_model::<M>(&col_no_pk).unwrap_err();
        assert!(matches!(e, crate::DbError::Schema(_)));

        let col_with_pk = crate::catalog::CollectionInfo {
            primary_field: Some("id".to_string()),
            ..col_no_pk
        };
        let e = super::validate_subset_model::<M>(&col_with_pk).unwrap_err();
        assert!(matches!(e, crate::DbError::Schema(_)));
    }

    #[test]
    fn row_value_at_path_and_nested_object_path_edge_cases_are_covered() {
        assert!(super::row_value_at_path_segments(&BTreeMap::new(), &[]).is_none());

        let segs = vec![Cow::Borrowed("a"), Cow::Borrowed("b")];
        let nested = super::row_value_nested_object_path(&segs, crate::RowValue::Int64(1));
        let crate::RowValue::Object(m) = nested else { panic!("expected object"); };
        assert!(m.contains_key("a"));
    }

    #[test]
    fn index_deletes_for_existing_row_skips_missing_index_paths() {
        let pk = crate::ScalarValue::Int64(1);
        let row = BTreeMap::from([("x".to_string(), crate::RowValue::Int64(7))]);
        let indexes = vec![
            crate::schema::IndexDef {
                name: "x".to_string(),
                path: crate::schema::FieldPath(vec![Cow::Borrowed("x")]),
                kind: crate::schema::IndexKind::NonUnique,
            },
            crate::schema::IndexDef {
                name: "y".to_string(),
                path: crate::schema::FieldPath(vec![Cow::Borrowed("y")]),
                kind: crate::schema::IndexKind::NonUnique,
            },
        ];
        let out = super::index_deletes_for_existing_row(crate::schema::CollectionId(1), &pk, &indexes, &row);
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].index_name, "x");
    }

    #[test]
    fn delete_in_transaction_exercises_staging_branch() {
        let mut db = crate::db::Database::<crate::storage::VecStore>::open_in_memory().unwrap();
        let fields = vec![
            FieldDef::new(crate::schema::FieldPath(vec![Cow::Borrowed("id")]), Type::Int64),
            FieldDef::new(crate::schema::FieldPath(vec![Cow::Borrowed("x")]), Type::Int64),
        ];
        let indexes = vec![crate::schema::IndexDef {
            name: "x_idx".to_string(),
            path: crate::schema::FieldPath(vec![Cow::Borrowed("x")]),
            kind: crate::schema::IndexKind::NonUnique,
        }];
        let (cid, _) = db
            .register_collection_with_indexes("t", fields, indexes, "id")
            .unwrap();
        db.insert(
            cid,
            BTreeMap::from([
                ("id".to_string(), crate::RowValue::Int64(1)),
                ("x".to_string(), crate::RowValue::Int64(7)),
            ]),
        )
        .unwrap();

        db.begin_transaction().unwrap();
        db.delete(cid, &crate::ScalarValue::Int64(1)).unwrap();
        db.commit_transaction().unwrap();
        assert!(db.get(cid, &crate::ScalarValue::Int64(1)).unwrap().is_none());
    }

    #[test]
    fn delete_pk_type_mismatch_errors() {
        let mut db = crate::db::Database::<crate::storage::VecStore>::open_in_memory().unwrap();
        let fields = vec![FieldDef::new(crate::schema::FieldPath(vec![Cow::Borrowed("id")]), Type::Int64)];
        let (cid, _) = db.register_collection("t", fields, "id").unwrap();

        let e = db.delete(cid, &crate::ScalarValue::String("nope".to_string())).unwrap_err();
        assert!(matches!(e, crate::DbError::Format(_)));
    }

    #[test]
    fn delete_autocommit_with_index_entries_exercises_index_batch_and_apply_loop() {
        let mut db = crate::db::Database::<crate::storage::VecStore>::open_in_memory().unwrap();
        let fields = vec![
            FieldDef::new(crate::schema::FieldPath(vec![Cow::Borrowed("id")]), Type::Int64),
            FieldDef::new(crate::schema::FieldPath(vec![Cow::Borrowed("x")]), Type::Int64),
        ];
        let indexes = vec![crate::schema::IndexDef {
            name: "x_idx".to_string(),
            path: crate::schema::FieldPath(vec![Cow::Borrowed("x")]),
            kind: crate::schema::IndexKind::NonUnique,
        }];
        let (cid, _) = db
            .register_collection_with_indexes("t", fields, indexes, "id")
            .unwrap();
        db.insert(
            cid,
            BTreeMap::from([
                ("id".to_string(), crate::RowValue::Int64(1)),
                ("x".to_string(), crate::RowValue::Int64(7)),
            ]),
        )
        .unwrap();

        // No active transaction -> autocommit path builds index segment batch and applies entries.
        db.delete(cid, &crate::ScalarValue::Int64(1)).unwrap();
        assert!(db.get(cid, &crate::ScalarValue::Int64(1)).unwrap().is_none());
    }

    #[test]
    fn delete_multisegment_schema_uses_v3_delete_payload_path() {
        let mut db = crate::db::Database::<crate::storage::VecStore>::open_in_memory().unwrap();
        let fields = vec![
            FieldDef::new(crate::schema::FieldPath(vec![Cow::Borrowed("id")]), Type::Int64),
            FieldDef::new(
                crate::schema::FieldPath(vec![Cow::Borrowed("obj"), Cow::Borrowed("x")]),
                Type::Int64,
            ),
        ];
        let (cid, _) = db.register_collection("t", fields, "id").unwrap();
        db.insert(
            cid,
            BTreeMap::from([
                ("id".to_string(), crate::RowValue::Int64(1)),
                (
                    "obj".to_string(),
                    crate::RowValue::Object(BTreeMap::from([("x".to_string(), crate::RowValue::Int64(1))])),
                ),
            ]),
        )
        .unwrap();
        db.delete(cid, &crate::ScalarValue::Int64(1)).unwrap();
    }

    struct FailFsOps {
        fail_on_rename_from_suffix: &'static str,
        rename_calls: std::sync::atomic::AtomicUsize,
        last_tmp: std::sync::Mutex<Option<std::path::PathBuf>>,
    }

    impl FailFsOps {
        fn new(fail_on_rename_from_suffix: &'static str) -> Self {
            Self {
                fail_on_rename_from_suffix,
                rename_calls: std::sync::atomic::AtomicUsize::new(0),
                last_tmp: std::sync::Mutex::new(None),
            }
        }
    }

    impl crate::db::fs_ops::FsOps for FailFsOps {
        fn remove_file(&self, path: &std::path::Path) -> std::io::Result<()> {
            std::fs::remove_file(path)
        }

        fn rename(&self, from: &std::path::Path, to: &std::path::Path) -> std::io::Result<()> {
            self.rename_calls
                .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            if from
                .to_string_lossy()
                .ends_with(self.fail_on_rename_from_suffix)
            {
                return Err(std::io::Error::new(std::io::ErrorKind::PermissionDenied, "boom"));
            }
            std::fs::rename(from, to)
        }

        fn copy(&self, from: &std::path::Path, to: &std::path::Path) -> std::io::Result<u64> {
            *self.last_tmp.lock().unwrap() = Some(to.to_path_buf());
            std::fs::copy(from, to)
        }

        fn open_read(&self, path: &std::path::Path) -> std::io::Result<std::fs::File> {
            std::fs::OpenOptions::new().read(true).open(path)
        }

        fn open_dir(&self, path: &std::path::Path) -> std::io::Result<std::fs::File> {
            std::fs::File::open(path)
        }

        fn open_read_write_create_truncate(
            &self,
            path: &std::path::Path,
        ) -> std::io::Result<std::fs::File> {
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create(true)
                .truncate(true)
                .open(path)
        }

        fn open_read_write_create_new(&self, path: &std::path::Path) -> std::io::Result<std::fs::File> {
            std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(path)
        }
    }

    #[test]
    fn compact_in_place_with_fsops_rename_tmp_to_live_failure_restores_backup() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("inplace.typra");

        let mut db = super::Database::open(&path).unwrap();
        let mut x = path_field("x");
        x.ty = Type::Int64;
        let fields = vec![path_field("id"), x];
        db.register_collection("t", fields, "id").unwrap();
        let cid = crate::schema::CollectionId(1);
        db.insert(
            cid,
            BTreeMap::from([
                ("id".to_string(), crate::RowValue::String("k".to_string())),
                ("x".to_string(), crate::RowValue::Int64(1)),
            ]),
        )
        .unwrap();

        // Fail on tmp -> live rename (tmp suffix is stable).
        let fs = FailFsOps::new(".tmp");
        let e = db.compact_in_place_with_fsops(&fs).unwrap_err();
        assert!(matches!(e, crate::DbError::Io(_)));

        // Live DB should still be readable (backup restored).
        let reopened = super::Database::open(&path).unwrap();
        let got = reopened
            .get(cid, &crate::ScalarValue::String("k".to_string()))
            .unwrap()
            .unwrap();
        assert_eq!(got.get("x"), Some(&crate::RowValue::Int64(1)));
    }

    #[test]
    fn restore_snapshot_to_path_with_fsops_rename_tmp_to_dest_failure_restores_dest_and_removes_tmp() {
        let dir = tempfile::tempdir().unwrap();
        let snapshot_src = dir.path().join("snapshot_src.typra");
        let snapshot = dir.path().join("snapshot.typra");
        let dest = dir.path().join("dest.typra");

        // Create snapshot source and export it.
        {
            let mut db = super::Database::open(&snapshot_src).unwrap();
            let (cid, _) = db
                .register_collection("t", vec![path_field("id")], "id")
                .unwrap();
            db.insert(
                cid,
                BTreeMap::from([("id".to_string(), crate::RowValue::String("a".to_string()))]),
            )
            .unwrap();
            db.export_snapshot_to_path(&snapshot).unwrap();
        }

        // Create an initial dest file so the backup path is exercised.
        {
            let mut db = super::Database::open(&dest).unwrap();
            let (cid, _) = db
                .register_collection("t", vec![path_field("id")], "id")
                .unwrap();
            db.insert(
                cid,
                BTreeMap::from([("id".to_string(), crate::RowValue::String("orig".to_string()))]),
            )
            .unwrap();
        }

        let fs = FailFsOps::new(".tmp");
        let e = super::Database::restore_snapshot_to_path_with_fsops(&fs, &snapshot, &dest)
            .unwrap_err();
        assert!(matches!(e, crate::DbError::Io(_)));

        // Temp file should have been removed (best-effort cleanup).
        if let Some(tmp) = fs.last_tmp.lock().unwrap().clone() {
            assert!(
                !tmp.exists(),
                "expected tmp file to be removed on failure: {tmp:?}"
            );
        }

        // Destination should still contain the original row (backup restored).
        let reopened = super::Database::open(&dest).unwrap();
        let cid = reopened.collection_id_named("t").unwrap();
        let got = reopened
            .get(cid, &crate::ScalarValue::String("orig".to_string()))
            .unwrap()
            .unwrap();
        assert_eq!(
            got.get("id"),
            Some(&crate::RowValue::String("orig".to_string()))
        );
    }


    #[test]
    fn read_and_select_superblock_errors_when_both_invalid() {
        let mut store = new_store();
        store
            .write_all_at(0, &FileHeader::new_v0_3().encode())
            .unwrap();
        let segment_start = (FILE_HEADER_SIZE + 2 * SUPERBLOCK_SIZE) as u64;
        store.write_all_at(segment_start - 1, &[0u8]).unwrap();

        let mut a = Superblock::empty().encode();
        let mut b = Superblock::empty().encode();
        a[0] ^= 0xff;
        b[0] ^= 0xff;
        store.write_all_at(FILE_HEADER_SIZE as u64, &a).unwrap();
        store
            .write_all_at((FILE_HEADER_SIZE + SUPERBLOCK_SIZE) as u64, &b)
            .unwrap();

        let res = open::read_and_select_superblock(&mut store);
        assert!(matches!(
            res,
            Err(DbError::Format(FormatError::BadSuperblockChecksum))
        ));
    }

    #[test]
    fn read_manifest_rejects_wrong_segment_type() {
        let mut store = new_store();
        store
            .write_all_at(0, &FileHeader::new_v0_3().encode())
            .unwrap();
        let segment_start = (FILE_HEADER_SIZE + 2 * SUPERBLOCK_SIZE) as u64;
        store.write_all_at(segment_start - 1, &[0u8]).unwrap();

        let sb_a = Superblock {
            generation: 1,
            ..Superblock::empty()
        };
        store
            .write_all_at(FILE_HEADER_SIZE as u64, &sb_a.encode())
            .unwrap();
        store
            .write_all_at(
                (FILE_HEADER_SIZE + SUPERBLOCK_SIZE) as u64,
                &Superblock::empty().encode(),
            )
            .unwrap();

        let mut w = SegmentWriter::new(&mut store, segment_start);
        let off = w
            .append(
                SegmentHeader {
                    segment_type: SegmentType::Schema,
                    payload_len: 0,
                    payload_crc32c: 0,
                },
                b"hi",
            )
            .unwrap();

        let sb = Superblock {
            manifest_offset: off,
            manifest_len: 2,
            ..sb_a
        };
        let res = open::read_manifest(&mut store, &sb);
        assert!(matches!(
            res,
            Err(DbError::Format(FormatError::UnsupportedVersion { .. }))
        ));
    }

    #[test]
    fn read_and_select_superblock_prefers_a_when_generation_higher() {
        let mut store = new_store();
        store
            .write_all_at(0, &FileHeader::new_v0_3().encode())
            .unwrap();
        let segment_start = (FILE_HEADER_SIZE + 2 * SUPERBLOCK_SIZE) as u64;
        store.write_all_at(segment_start - 1, &[0u8]).unwrap();

        let sb_a = Superblock {
            generation: 10,
            ..Superblock::empty()
        };
        let sb_b = Superblock {
            generation: 9,
            ..Superblock::empty()
        };
        store
            .write_all_at(FILE_HEADER_SIZE as u64, &sb_a.encode())
            .unwrap();
        store
            .write_all_at((FILE_HEADER_SIZE + SUPERBLOCK_SIZE) as u64, &sb_b.encode())
            .unwrap();

        let selected = open::read_and_select_superblock(&mut store).unwrap();
        assert_eq!(selected.generation, sb_a.generation);
    }

    #[test]
    fn register_and_reopen_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.typra");
        {
            let mut db = Database::open(&path).unwrap();
            assert!(db.catalog().is_empty());
            let (id, v) = db
                .register_collection("books", vec![path_field("title")], "title")
                .unwrap();
            assert_eq!(id.0, 1);
            assert_eq!(v.0, 1);
        }
        let db = Database::open(&path).unwrap();
        assert_eq!(db.collection_names(), vec!["books".to_string()]);
        let c = db.catalog().get(crate::schema::CollectionId(1)).unwrap();
        assert_eq!(c.name, "books");
        assert_eq!(c.fields.len(), 1);
    }

    #[test]
    fn index_segment_replay_builds_index_state_on_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.typra");
        {
            let mut db = Database::open(&path).unwrap();
            let (id, _v) = db
                .register_collection("books", vec![path_field("title")], "title")
                .unwrap();
            let payload = encode_index_payload(&[IndexEntry {
                collection_id: id.0,
                index_name: "title_idx".to_string(),
                kind: crate::schema::IndexKind::NonUnique,
                op: crate::index::IndexOp::Insert,
                index_key: b"Hello".to_vec(),
                pk_key: b"Hello".to_vec(),
            }]);
            write::commit_write_txn_v6(
                &mut db.store,
                db.segment_start,
                &mut db.format_minor,
                2,
                &[(
                    crate::segments::header::SegmentType::Index,
                    payload.as_slice(),
                )],
            )
            .unwrap();
        }
        let db = Database::open(&path).unwrap();
        let got = db
            .index_state()
            .non_unique_lookup(1, "title_idx", b"Hello")
            .unwrap();
        assert_eq!(got, vec![b"Hello".to_vec()]);
    }

    #[test]
    fn query_uses_non_unique_index_for_equality_filter() {
        use crate::query::{Predicate, Query};
        use crate::schema::{FieldPath, IndexDef, IndexKind};
        use crate::{RowValue, ScalarValue};
        use std::collections::BTreeMap;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.typra");
        let mut db = Database::open(&path).unwrap();
        let mut year_def = path_field("year");
        year_def.ty = Type::Int64;
        let fields = vec![path_field("title"), year_def];
        let indexes = vec![IndexDef {
            name: "title_idx".to_string(),
            path: FieldPath(vec![std::borrow::Cow::Owned("title".to_string())]),
            kind: IndexKind::NonUnique,
        }];
        let (cid, _) = db
            .register_collection_with_indexes("books", fields, indexes, "title")
            .unwrap();
        db.insert(cid, {
            let mut m = BTreeMap::new();
            m.insert("title".to_string(), RowValue::String("Hello".to_string()));
            m.insert("year".to_string(), RowValue::Int64(2020));
            m
        })
        .unwrap();
        db.insert(cid, {
            let mut m = BTreeMap::new();
            m.insert("title".to_string(), RowValue::String("World".to_string()));
            m.insert("year".to_string(), RowValue::Int64(2021));
            m
        })
        .unwrap();

        let q = Query {
            collection: cid,
            predicate: Some(Predicate::Eq {
                path: FieldPath(vec![std::borrow::Cow::Owned("title".to_string())]),
                value: ScalarValue::String("Hello".to_string()),
            }),
            limit: None,
            order_by: None,
        };
        let explain = db.explain_query(&q).unwrap();
        assert!(explain.contains("IndexLookup"));
        let rows = db.query(&q).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("year"), Some(&RowValue::Int64(2020)));
    }

    #[test]
    fn subset_model_projection_returns_only_declared_fields() {
        use crate::schema::{DbModel, FieldDef, FieldPath, Type};
        use crate::RowValue;
        use std::borrow::Cow;
        use std::collections::BTreeMap;

        #[allow(dead_code)]
        struct BookFull {
            title: String,
            year: i64,
        }

        #[allow(dead_code)]
        struct BookTitleOnly {
            title: String,
        }

        impl DbModel for BookFull {
            fn collection_name() -> &'static str {
                "books"
            }
            fn fields() -> Vec<FieldDef> {
                vec![
                    FieldDef {
                        path: FieldPath(vec![Cow::Borrowed("title")]),
                        ty: Type::String,
                        constraints: vec![],
                    },
                    FieldDef {
                        path: FieldPath(vec![Cow::Borrowed("year")]),
                        ty: Type::Int64,
                        constraints: vec![],
                    },
                ]
            }
            fn primary_field() -> &'static str {
                "title"
            }
        }

        impl DbModel for BookTitleOnly {
            fn collection_name() -> &'static str {
                "books"
            }
            fn fields() -> Vec<FieldDef> {
                vec![FieldDef {
                    path: FieldPath(vec![Cow::Borrowed("title")]),
                    ty: Type::String,
                    constraints: vec![],
                }]
            }
            fn primary_field() -> &'static str {
                "title"
            }
        }

        let mut db = Database::open_in_memory().unwrap();
        let (cid, _) = db.register_model::<BookFull>().unwrap();
        db.insert(cid, {
            let mut m = BTreeMap::new();
            m.insert("title".to_string(), RowValue::String("Hello".to_string()));
            m.insert("year".to_string(), RowValue::Int64(2020));
            m
        })
        .unwrap();

        let books = db.collection::<BookTitleOnly>().unwrap();
        let rows = books.all().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0],
            BTreeMap::from([("title".to_string(), RowValue::String("Hello".to_string()))])
        );
    }

    #[test]
    fn query_iter_matches_execute_query_for_indexed_equality() {
        use crate::query::{Predicate, Query};
        use crate::schema::{FieldPath, IndexDef, IndexKind};
        use crate::{RowValue, ScalarValue};

        let mut db = Database::open_in_memory().unwrap();
        let mut year_def = path_field("year");
        year_def.ty = Type::Int64;
        let fields = vec![path_field("title"), year_def];
        let indexes = vec![IndexDef {
            name: "title_idx".to_string(),
            path: FieldPath(vec![std::borrow::Cow::Owned("title".to_string())]),
            kind: IndexKind::NonUnique,
        }];
        let (cid, _) = db
            .register_collection_with_indexes("books", fields, indexes, "title")
            .unwrap();
        db.insert(cid, {
            let mut m = BTreeMap::new();
            m.insert("title".to_string(), RowValue::String("Hello".to_string()));
            m.insert("year".to_string(), RowValue::Int64(2020));
            m
        })
        .unwrap();

        let q = Query {
            collection: cid,
            predicate: Some(Predicate::Eq {
                path: FieldPath(vec![std::borrow::Cow::Owned("title".to_string())]),
                value: ScalarValue::String("Hello".to_string()),
            }),
            limit: None,
            order_by: None,
        };
        let mut from_iter: Vec<_> = db.query_iter(&q).unwrap().map(|r| r.unwrap()).collect();
        let mut from_vec = db.query(&q).unwrap();
        from_iter.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
        from_vec.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
        assert_eq!(from_iter, from_vec);
    }

    #[test]
    fn query_iter_order_by_uses_external_sort_spill_for_large_inputs() {
        use crate::query::{OrderBy, OrderDirection, Query};
        use crate::{RowValue, ScalarValue};

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.typra");
        let mut db = Database::open(&path).unwrap();

        let mut year_def = path_field("year");
        year_def.ty = Type::Int64;
        let fields = vec![path_field("title"), year_def];
        let (cid, _) = db.register_collection("books", fields, "title").unwrap();
        for i in 0..6000i64 {
            db.insert(cid, {
                let mut m = BTreeMap::new();
                m.insert("title".to_string(), RowValue::String(format!("t{i}")));
                m.insert("year".to_string(), RowValue::Int64(i));
                m
            })
            .unwrap();
        }

        let q = Query {
            collection: cid,
            predicate: None,
            order_by: Some(OrderBy {
                path: crate::schema::FieldPath(vec![std::borrow::Cow::Borrowed("year")]),
                direction: OrderDirection::Desc,
            }),
            limit: Some(50),
        };

        let from_vec = db.query(&q).unwrap();
        let from_iter: Vec<_> = db.query_iter(&q).unwrap().map(|r| r.unwrap()).collect();
        assert_eq!(from_iter, from_vec);

        assert_eq!(from_iter[0].get("year"), Some(&RowValue::Int64(5999)));
        assert_eq!(
            from_iter.last().unwrap().get("year"),
            Some(&RowValue::Int64(5950))
        );

        let got = db
            .get(cid, &ScalarValue::String("t123".to_string()))
            .unwrap()
            .unwrap();
        assert_eq!(got.get("year"), Some(&RowValue::Int64(123)));
    }

    #[test]
    fn subset_projection_merges_nested_paths_under_shared_object() {
        use crate::schema::{DbModel, FieldDef, FieldPath, Type};
        use crate::RowValue;
        use std::borrow::Cow;
        struct Sub;
        impl DbModel for Sub {
            fn collection_name() -> &'static str {
                "x"
            }
            fn fields() -> Vec<FieldDef> {
                vec![
                    FieldDef {
                        path: FieldPath(vec![Cow::Borrowed("a"), Cow::Borrowed("b")]),
                        ty: Type::String,
                        constraints: vec![],
                    },
                    FieldDef {
                        path: FieldPath(vec![Cow::Borrowed("a"), Cow::Borrowed("c")]),
                        ty: Type::Int64,
                        constraints: vec![],
                    },
                ]
            }
            fn primary_field() -> &'static str {
                "id"
            }
        }

        let mut row = BTreeMap::new();
        row.insert("id".to_string(), RowValue::String("pk".to_string()));
        let inner = BTreeMap::from([
            ("b".to_string(), RowValue::String("B".to_string())),
            ("c".to_string(), RowValue::Int64(42)),
        ]);
        row.insert("a".to_string(), RowValue::Object(inner));

        let out = super::project_row::<Sub>(row);
        let a = out.get("a").unwrap();
        let RowValue::Object(m) = a else {
            panic!("expected object");
        };
        assert_eq!(m.get("b"), Some(&RowValue::String("B".to_string())));
        assert_eq!(m.get("c"), Some(&RowValue::Int64(42)));
        assert_eq!(out.len(), 1);
    }

    #[test]
    fn checkpoint_roundtrip_replays_only_tail() {
        use crate::schema::{IndexDef, IndexKind};
        use crate::{RowValue, ScalarValue};

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.typra");

        // Create, write state, checkpoint, then append more.
        {
            let mut db = Database::open(&path).unwrap();
            let fields = vec![path_field("title"), path_field("author")];
            let indexes = vec![IndexDef {
                name: "author_idx".to_string(),
                path: crate::schema::FieldPath(vec![std::borrow::Cow::Owned("author".to_string())]),
                kind: IndexKind::NonUnique,
            }];
            let (cid, _) = db
                .register_collection_with_indexes("books", fields, indexes, "title")
                .unwrap();
            db.insert(cid, {
                let mut m = BTreeMap::new();
                m.insert("title".to_string(), RowValue::String("Hello".to_string()));
                m.insert("author".to_string(), RowValue::String("Alice".to_string()));
                m
            })
            .unwrap();
            db.checkpoint().unwrap();
            db.insert(cid, {
                let mut m = BTreeMap::new();
                m.insert("title".to_string(), RowValue::String("World".to_string()));
                m.insert("author".to_string(), RowValue::String("Bob".to_string()));
                m
            })
            .unwrap();
        }

        // Reopen; should load from checkpoint then replay tail insert.
        let db = Database::open(&path).unwrap();
        let cid = db.collection_id_named("books").unwrap();
        let got = db
            .get(cid, &ScalarValue::String("Hello".to_string()))
            .unwrap()
            .unwrap();
        assert_eq!(
            got.get("author"),
            Some(&RowValue::String("Alice".to_string()))
        );
        let got2 = db
            .get(cid, &ScalarValue::String("World".to_string()))
            .unwrap()
            .unwrap();
        assert_eq!(
            got2.get("author"),
            Some(&RowValue::String("Bob".to_string()))
        );
    }

    #[test]
    fn corrupt_checkpoint_falls_back_in_auto_truncate_but_errors_in_strict() {
        use crate::config::{OpenOptions, RecoveryMode};
        use crate::segments::header::SEGMENT_HEADER_LEN;
        use crate::superblock::decode_superblock;
        use crate::RowValue;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.typra");

        // Create DB and checkpoint.
        {
            let mut db = Database::open(&path).unwrap();
            let (cid, _) = db
                .register_collection("books", vec![path_field("title")], "title")
                .unwrap();
            db.insert(cid, {
                let mut m = BTreeMap::new();
                m.insert("title".to_string(), RowValue::String("Hello".to_string()));
                m
            })
            .unwrap();
            db.checkpoint().unwrap();
        }

        // Corrupt one byte inside the checkpoint payload.
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .unwrap();
        let mut store = crate::storage::FileStore::new(file);
        let mut sb_buf = [0u8; SUPERBLOCK_SIZE];
        store
            .read_exact_at(FILE_HEADER_SIZE as u64, &mut sb_buf)
            .unwrap();
        let sb = decode_superblock(&sb_buf).unwrap();
        assert!(sb.checkpoint_offset != 0);
        let corrupt_at = sb.checkpoint_offset + SEGMENT_HEADER_LEN as u64 + 5;
        store.write_all_at(corrupt_at, &[0xff]).unwrap();
        store.sync().unwrap();

        // Strict should error.
        let strict = Database::open_with_options(
            &path,
            OpenOptions {
                recovery: RecoveryMode::Strict,
                ..OpenOptions::default()
            },
        );
        assert!(strict.is_err());

        // AutoTruncate should fall back to replay and still open.
        let auto = Database::open_with_options(
            &path,
            OpenOptions {
                recovery: RecoveryMode::AutoTruncate,
                ..OpenOptions::default()
            },
        )
        .unwrap();
        assert_eq!(auto.collection_names(), vec!["books".to_string()]);
    }

    #[test]
    fn temp_segments_are_ignored_on_reopen() {
        use crate::segments::header::{SegmentHeader, SegmentType};
        use crate::segments::writer::SegmentWriter;
        use crate::{RowValue, ScalarValue};

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.typra");

        // Create DB, write state, then append an ephemeral Temp segment.
        {
            let mut db = Database::open(&path).unwrap();
            let (cid, _) = db
                .register_collection("books", vec![path_field("title")], "title")
                .unwrap();
            db.insert(cid, {
                let mut m = BTreeMap::new();
                m.insert("title".to_string(), RowValue::String("Hello".to_string()));
                m
            })
            .unwrap();

            let off = db.store.len().unwrap();
            let mut w = SegmentWriter::new(&mut db.store, off);
            w.append(
                SegmentHeader {
                    segment_type: SegmentType::Temp,
                    payload_len: 0,
                    payload_crc32c: 0,
                },
                b"spill",
            )
            .unwrap();
            db.store.sync().unwrap();
        }

        // Reopen should succeed and ignore the Temp segment.
        let db = Database::open(&path).unwrap();
        let cid = db.collection_id_named("books").unwrap();
        let got = db
            .get(cid, &ScalarValue::String("Hello".to_string()))
            .unwrap()
            .unwrap();
        assert_eq!(
            got.get("title"),
            Some(&RowValue::String("Hello".to_string()))
        );
    }

    #[test]
    fn commit_transaction_propagates_store_write_failure_when_budget_exhausted_at_commit() {
        use std::cell::Cell;
        use std::io;
        use std::path::PathBuf;
        use std::rc::Rc;

        use crate::config::OpenOptions;
        use crate::schema::{FieldDef, FieldPath};
        use crate::storage::{Store, VecStore};

        struct CountWrites {
            n: Rc<Cell<usize>>,
            inner: VecStore,
        }

        impl Store for CountWrites {
            fn len(&self) -> Result<u64, DbError> {
                self.inner.len()
            }

            fn read_exact_at(&mut self, offset: u64, buf: &mut [u8]) -> Result<(), DbError> {
                self.inner.read_exact_at(offset, buf)
            }

            fn write_all_at(&mut self, offset: u64, buf: &[u8]) -> Result<(), DbError> {
                self.n.set(self.n.get() + 1);
                self.inner.write_all_at(offset, buf)
            }

            fn sync(&mut self) -> Result<(), DbError> {
                self.inner.sync()
            }

            fn truncate(&mut self, len: u64) -> Result<(), DbError> {
                self.inner.truncate(len)
            }
        }

        struct BudgetWrites {
            remaining: Cell<usize>,
            inner: VecStore,
        }

        impl Store for BudgetWrites {
            fn len(&self) -> Result<u64, DbError> {
                self.inner.len()
            }

            fn read_exact_at(&mut self, offset: u64, buf: &mut [u8]) -> Result<(), DbError> {
                self.inner.read_exact_at(offset, buf)
            }

            fn write_all_at(&mut self, offset: u64, buf: &[u8]) -> Result<(), DbError> {
                let r = self.remaining.get();
                if r == 0 {
                    return Err(DbError::Io(io::Error::other(
                        "write budget exhausted (commit)",
                    )));
                }
                self.remaining.set(r - 1);
                self.inner.write_all_at(offset, buf)
            }

            fn sync(&mut self) -> Result<(), DbError> {
                self.inner.sync()
            }

            fn truncate(&mut self, len: u64) -> Result<(), DbError> {
                self.inner.truncate(len)
            }
        }

        let write_count = Rc::new(Cell::new(0));
        let store = CountWrites {
            n: write_count.clone(),
            inner: VecStore::new(),
        };
        {
            let _db = Database::open_with_store(
                PathBuf::from(":memory:"),
                store,
                OpenOptions::default(),
            )
            .unwrap();
        }
        let w_open = write_count.get();
        assert!(w_open > 0, "expected bootstrap open to perform writes");

        let store2 = BudgetWrites {
            remaining: Cell::new(w_open),
            inner: VecStore::new(),
        };
        let mut db = Database::open_with_store(
            PathBuf::from(":memory:"),
            store2,
            OpenOptions::default(),
        )
        .unwrap();

        db.begin_transaction().unwrap();
        let fields = vec![FieldDef::new(
            FieldPath(vec![Cow::Borrowed("id")]),
            Type::String,
        )];
        db.register_collection("c", fields, "id").unwrap();
        let err = db.commit_transaction().unwrap_err();
        assert!(
            matches!(err, DbError::Io(_)),
            "expected Io from exhausted write budget, got {err:?}"
        );
    }
