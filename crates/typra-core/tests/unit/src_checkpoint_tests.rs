    use std::borrow::Cow;
    use std::collections::BTreeMap;

    use crate::catalog::{Catalog, CatalogRecordWire};
    use crate::checkpoint::checkpoint_from_state;
    use crate::db::LatestMap;
    use crate::index::IndexState;
    use crate::record::RowValue;
    use crate::schema::{FieldDef, FieldPath, IndexDef, IndexKind, Type};
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
        assert!(matches!(err, crate::error::DbError::Format(_)));
    }
