    use super::*;
    use crate::catalog::codec::encode_catalog_payload;
    use crate::catalog::MAX_COLLECTION_NAME_BYTES;
    use crate::schema::{FieldPath, Type};
    use std::borrow::Cow;

    fn path(parts: &[&str]) -> crate::schema::FieldPath {
        FieldPath(parts.iter().map(|s| Cow::Owned(s.to_string())).collect())
    }

    #[test]
    fn apply_create_then_version() {
        let mut c = Catalog::default();
        let w = CatalogRecordWire::CreateCollection {
            collection_id: 1,
            name: "a".to_string(),
            schema_version: 1,
            fields: vec![],
            indexes: vec![],
            primary_field: None,
        };
        c.apply_record(w).unwrap();
        assert_eq!(c.next_collection_id(), CollectionId(2));
        let w2 = CatalogRecordWire::NewSchemaVersion {
            collection_id: 1,
            schema_version: 2,
            fields: vec![FieldDef {
                path: path(&["x"]),
                ty: Type::Int64,
                constraints: vec![],
            }],
            indexes: vec![],
        };
        c.apply_record(w2).unwrap();
        assert_eq!(
            c.get(CollectionId(1)).unwrap().current_version,
            SchemaVersion(2)
        );
    }

    #[test]
    fn duplicate_name_rejected() {
        let mut c = Catalog::default();
        c.apply_record(CatalogRecordWire::CreateCollection {
            collection_id: 1,
            name: "a".to_string(),
            schema_version: 1,
            fields: vec![],
            indexes: vec![],
            primary_field: None,
        })
        .unwrap();
        let err = c.apply_record(CatalogRecordWire::CreateCollection {
            collection_id: 2,
            name: "a".to_string(),
            schema_version: 1,
            fields: vec![],
            indexes: vec![],
            primary_field: None,
        });
        assert!(matches!(
            err,
            Err(DbError::Schema(SchemaError::DuplicateCollectionName { .. }))
        ));
    }

    #[test]
    fn new_schema_version_cannot_skip_numbers() {
        let mut c = Catalog::default();
        c.apply_record(CatalogRecordWire::CreateCollection {
            collection_id: 1,
            name: "a".to_string(),
            schema_version: 1,
            fields: vec![],
            indexes: vec![],
            primary_field: None,
        })
        .unwrap();
        let err = c.apply_record(CatalogRecordWire::NewSchemaVersion {
            collection_id: 1,
            schema_version: 3,
            fields: vec![],
            indexes: vec![],
        });
        assert!(matches!(
            err,
            Err(DbError::Schema(SchemaError::InvalidSchemaVersion {
                expected: 2,
                got: 3
            }))
        ));
    }

    #[test]
    fn encode_then_apply_matches() {
        let w = CatalogRecordWire::CreateCollection {
            collection_id: 1,
            name: "b".to_string(),
            schema_version: 1,
            fields: vec![],
            indexes: vec![],
            primary_field: None,
        };
        let bytes = encode_catalog_payload(&w);
        let mut c = Catalog::default();
        c.apply_record(crate::catalog::codec::decode_catalog_payload(&bytes).unwrap())
            .unwrap();
        assert_eq!(c.collection_names(), vec!["b".to_string()]);
    }

    #[test]
    fn len_and_collections_helpers_track_entries() {
        let mut c = Catalog::default();
        assert_eq!(c.len(), 0);
        assert!(c.collections().is_empty());
        c.apply_record(CatalogRecordWire::CreateCollection {
            collection_id: 1,
            name: "z".to_string(),
            schema_version: 1,
            fields: vec![],
            indexes: vec![],
            primary_field: None,
        })
        .unwrap();
        assert_eq!(c.len(), 1);
        let list = c.collections();
        assert_eq!(list.len(), 1);
        assert_eq!(list[0].id, CollectionId(1));
    }

    #[test]
    fn apply_create_rejects_empty_name() {
        let mut c = Catalog::default();
        let err = c.apply_record(CatalogRecordWire::CreateCollection {
            collection_id: 1,
            name: "".to_string(),
            schema_version: 1,
            fields: vec![],
            indexes: vec![],
            primary_field: None,
        });
        assert!(matches!(
            err,
            Err(DbError::Schema(SchemaError::InvalidCollectionName))
        ));
    }

    #[test]
    fn apply_create_rejects_name_longer_than_max() {
        let mut c = Catalog::default();
        let name = "x".repeat(MAX_COLLECTION_NAME_BYTES + 1);
        let err = c.apply_record(CatalogRecordWire::CreateCollection {
            collection_id: 1,
            name,
            schema_version: 1,
            fields: vec![],
            indexes: vec![],
            primary_field: None,
        });
        assert!(matches!(
            err,
            Err(DbError::Schema(SchemaError::InvalidCollectionName))
        ));
    }

    #[test]
    fn apply_create_rejects_initial_schema_version_not_one() {
        let mut c = Catalog::default();
        let err = c.apply_record(CatalogRecordWire::CreateCollection {
            collection_id: 1,
            name: "a".to_string(),
            schema_version: 2,
            fields: vec![],
            indexes: vec![],
            primary_field: None,
        });
        assert!(matches!(
            err,
            Err(DbError::Schema(SchemaError::InvalidSchemaVersion {
                expected: 1,
                got: 2
            }))
        ));
    }

    #[test]
    fn apply_create_rejects_non_sequential_collection_id() {
        let mut c = Catalog::default();
        let err = c.apply_record(CatalogRecordWire::CreateCollection {
            collection_id: 7,
            name: "a".to_string(),
            schema_version: 1,
            fields: vec![],
            indexes: vec![],
            primary_field: None,
        });
        assert!(matches!(
            err,
            Err(DbError::Schema(SchemaError::UnexpectedCollectionId {
                expected: 1,
                got: 7
            }))
        ));
    }

    #[test]
    fn apply_create_rejects_primary_field_not_in_schema() {
        let mut c = Catalog::default();
        let err = c.apply_record(CatalogRecordWire::CreateCollection {
            collection_id: 1,
            name: "a".to_string(),
            schema_version: 1,
            fields: vec![FieldDef {
                path: path(&["x"]),
                ty: Type::Int64,
                constraints: vec![],
            }],
            indexes: vec![],
            primary_field: Some("missing".to_string()),
        });
        assert!(matches!(
            err,
            Err(DbError::Schema(SchemaError::PrimaryFieldNotFound { .. }))
        ));
    }

    #[test]
    fn apply_new_version_rejects_when_primary_dropped_from_fields() {
        let mut c = Catalog::default();
        c.apply_record(CatalogRecordWire::CreateCollection {
            collection_id: 1,
            name: "a".to_string(),
            schema_version: 1,
            fields: vec![FieldDef {
                path: path(&["id"]),
                ty: Type::Int64,
                constraints: vec![],
            }],
            indexes: vec![],
            primary_field: Some("id".to_string()),
        })
        .unwrap();
        let err = c.apply_record(CatalogRecordWire::NewSchemaVersion {
            collection_id: 1,
            schema_version: 2,
            fields: vec![],
            indexes: vec![],
        });
        assert!(matches!(
            err,
            Err(DbError::Schema(
                SchemaError::PrimaryFieldMissingInSchema { .. }
            ))
        ));
    }
