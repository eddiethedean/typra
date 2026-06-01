    use std::borrow::Cow;
    use std::collections::BTreeMap;

    use crate::catalog::{Catalog, CatalogRecordWire};
    use crate::checkpoint::checkpoint_from_state;
    use crate::checkpoint::{encode_checkpoint_payload_v0, state_from_checkpoint_payload, CheckpointV0};
    use crate::db::LatestMap;
    use crate::error::{DbError, SchemaError};
    use crate::index::IndexState;
    use crate::record::{encode_record_payload_v2, RowValue};
    use crate::schema::{CollectionId, FieldDef, FieldPath, IndexDef, IndexKind, SchemaVersion, Type};
    use crate::ScalarValue;

    fn fp(parts: &[&'static str]) -> FieldPath {
        FieldPath(parts.iter().copied().map(Cow::Borrowed).collect())
    }

    #[test]
    fn checkpoint_from_state_includes_new_schema_versions_and_record_payloads() {
        let mut catalog = Catalog::default();
        let fields_v1 = vec![
            FieldDef::new(fp(&["id"]), Type::String),
            FieldDef::new(fp(&["year"]), Type::Int64),
        ];
        let indexes = vec![IndexDef {
            name: "year_idx".into(),
            path: fp(&["year"]),
            kind: IndexKind::NonUnique,
        }];
        catalog
            .apply_record(CatalogRecordWire::CreateCollection {
                collection_id: 1,
                name: "books".into(),
                schema_version: 1,
                fields: fields_v1.clone(),
                indexes: indexes.clone(),
                primary_field: Some("id".into()),
            })
            .unwrap();
        // Bump to schema version 2 to hit the NewSchemaVersion encoding loop.
        catalog
            .apply_record(CatalogRecordWire::NewSchemaVersion {
                collection_id: 1,
                schema_version: 2,
                fields: fields_v1.clone(),
                indexes: indexes.clone(),
            })
            .unwrap();

        let mut latest: LatestMap = std::collections::HashMap::new();
        let pk = ScalarValue::String("k".into()).canonical_key_bytes();
        latest.insert(
            (1, pk),
            BTreeMap::from([
                ("id".into(), RowValue::String("k".into())),
                ("year".into(), RowValue::Int64(2020)),
            ]),
        );

        let indexes_state = IndexState::default();
        let cp = checkpoint_from_state(&catalog, &latest, &indexes_state).unwrap();

        // CreateCollection + NewSchemaVersion should both appear.
        assert!(cp
            .catalog_records
            .iter()
            .any(|r| matches!(r, CatalogRecordWire::CreateCollection { collection_id: 1, .. })));
        assert!(cp.catalog_records.iter().any(|r| matches!(
            r,
            CatalogRecordWire::NewSchemaVersion {
                collection_id: 1,
                schema_version: 2,
                ..
            }
        )));

        assert_eq!(cp.record_payloads.len(), 1);
    }

    #[test]
    fn checkpoint_from_state_surfaces_record_encoding_errors() {
        // Hit the `encode_record_payload_v2(...)?` error path (covers the `?` line).
        let mut catalog = Catalog::default();
        let fields = vec![
            FieldDef::new(fp(&["id"]), Type::String),
            FieldDef::new(fp(&["year"]), Type::Int64),
        ];
        catalog
            .apply_record(CatalogRecordWire::CreateCollection {
                collection_id: 1,
                name: "books".into(),
                schema_version: 1,
                fields: fields.clone(),
                indexes: vec![],
                primary_field: Some("id".into()),
            })
            .unwrap();

        let mut latest: LatestMap = std::collections::HashMap::new();
        let pk = ScalarValue::String("k".into()).canonical_key_bytes();
        // Wrong type for `year` (expects Int64) so record payload encoding fails.
        latest.insert(
            (1, pk),
            BTreeMap::from([
                ("id".into(), RowValue::String("k".into())),
                ("year".into(), RowValue::String("nope".into())),
            ]),
        );

        let indexes_state = IndexState::default();
        let err = checkpoint_from_state(&catalog, &latest, &indexes_state).unwrap_err();
        assert!(matches!(err, DbError::Format(_)));
    }

    #[test]
    fn checkpoint_from_state_errors_when_collection_has_no_primary_field() {
        let mut catalog = Catalog::default();
        catalog
            .apply_record(CatalogRecordWire::CreateCollection {
                collection_id: 1,
                name: "books".into(),
                schema_version: 1,
                fields: vec![FieldDef::new(fp(&["id"]), Type::String)],
                indexes: vec![],
                primary_field: None,
            })
            .unwrap();

        let mut latest: LatestMap = std::collections::HashMap::new();
        latest.insert(
            (1, b"k".to_vec()),
            BTreeMap::from([("id".into(), RowValue::String("k".into()))]),
        );
        let indexes_state = IndexState::default();
        let err = checkpoint_from_state(&catalog, &latest, &indexes_state).unwrap_err();
        assert!(matches!(
            err,
            DbError::Schema(SchemaError::NoPrimaryKey { collection_id: 1 })
        ));
    }

    #[test]
    fn checkpoint_from_state_errors_when_primary_field_not_found_in_schema() {
        // Test-only: inject inconsistent catalog state so checkpoint logic exercises the
        // PrimaryFieldNotFound error surface.
        let mut catalog = Catalog::default();
        catalog.test_insert_collection_info(crate::catalog::CollectionInfo {
            id: CollectionId(1),
            name: "books".into(),
            current_version: SchemaVersion(1),
            fields: vec![FieldDef::new(fp(&["year"]), Type::Int64)], // missing `id`
            indexes: vec![],
            primary_field: Some("id".into()),
        });

        let mut latest: LatestMap = std::collections::HashMap::new();
        latest.insert(
            (1, b"k".to_vec()),
            BTreeMap::from([("id".into(), RowValue::String("k".into()))]),
        );
        let err = checkpoint_from_state(&catalog, &latest, &IndexState::default()).unwrap_err();
        assert!(matches!(
            err,
            DbError::Schema(SchemaError::PrimaryFieldNotFound { .. })
        ));
    }

    #[test]
    fn checkpoint_from_state_errors_when_row_missing_primary_key_cell() {
        let mut catalog = Catalog::default();
        catalog
            .apply_record(CatalogRecordWire::CreateCollection {
                collection_id: 1,
                name: "books".into(),
                schema_version: 1,
                fields: vec![
                    FieldDef::new(fp(&["id"]), Type::String),
                    FieldDef::new(fp(&["year"]), Type::Int64),
                ],
                indexes: vec![],
                primary_field: Some("id".into()),
            })
            .unwrap();

        // Row is missing `id` cell.
        let mut latest: LatestMap = std::collections::HashMap::new();
        latest.insert((1, b"k".to_vec()), BTreeMap::from([("year".into(), RowValue::Int64(1))]));

        let err = checkpoint_from_state(&catalog, &latest, &IndexState::default()).unwrap_err();
        assert!(matches!(
            err,
            DbError::Schema(SchemaError::RowMissingPrimary { .. })
        ));
    }

    #[test]
    fn state_from_checkpoint_payload_errors_when_record_references_unknown_collection() {
        let record = encode_record_payload_v2(
            99,
            1,
            &ScalarValue::String("k".into()),
            &Type::String,
            &[],
        )
        .unwrap();
        let cp = CheckpointV0 {
            replay_from_offset: 0,
            catalog_records: vec![],
            record_payloads: vec![record],
            index_entries: vec![],
        };
        let bytes = encode_checkpoint_payload_v0(&cp);
        let err = state_from_checkpoint_payload(&bytes).unwrap_err();
        assert!(matches!(
            err,
            DbError::Schema(SchemaError::UnknownCollection { id: 99 })
        ));
    }

    #[test]
    fn state_from_checkpoint_payload_errors_when_collection_has_no_primary_key() {
        let wire = CatalogRecordWire::CreateCollection {
            collection_id: 1,
            name: "t".into(),
            schema_version: 1,
            fields: vec![FieldDef::new(fp(&["id"]), Type::String)],
            indexes: vec![],
            primary_field: None,
        };
        let record = encode_record_payload_v2(
            1,
            1,
            &ScalarValue::String("k".into()),
            &Type::String,
            &[],
        )
        .unwrap();
        let cp = CheckpointV0 {
            replay_from_offset: 0,
            catalog_records: vec![wire],
            record_payloads: vec![record],
            index_entries: vec![],
        };
        let bytes = encode_checkpoint_payload_v0(&cp);
        let err = state_from_checkpoint_payload(&bytes).unwrap_err();
        assert!(matches!(
            err,
            DbError::Schema(SchemaError::NoPrimaryKey { collection_id: 1 })
        ));
    }

    #[test]
    fn apply_checkpoint_record_payload_errors_when_primary_field_missing_in_fields() {
        // Test-only: inject inconsistent catalog state.
        let mut catalog = Catalog::default();
        catalog.test_insert_collection_info(crate::catalog::CollectionInfo {
            id: CollectionId(1),
            name: "t".into(),
            current_version: SchemaVersion(1),
            fields: vec![FieldDef::new(fp(&["year"]), Type::Int64)], // missing `id`
            indexes: vec![],
            primary_field: Some("id".into()),
        });

        let record = encode_record_payload_v2(
            1,
            1,
            &ScalarValue::String("k".into()),
            &Type::String,
            &[],
        )
        .unwrap();
        let mut latest = LatestMap::default();
        let err = super::apply_checkpoint_record_payload(&record, &catalog, &mut latest).unwrap_err();
        assert!(matches!(
            err,
            DbError::Schema(SchemaError::PrimaryFieldNotFound { .. })
        ));
    }
