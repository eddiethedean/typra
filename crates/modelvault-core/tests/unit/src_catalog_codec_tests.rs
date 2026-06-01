    use super::*;
    use crate::schema::FieldPath;

    fn path(parts: &[&str]) -> FieldPath {
        FieldPath(parts.iter().map(|s| Cow::Owned(s.to_string())).collect())
    }

    #[test]
    fn roundtrip_create_collection() {
        let rec = CatalogRecordWire::CreateCollection {
            collection_id: 1,
            name: "books".to_string(),
            schema_version: 1,
            fields: vec![FieldDef {
                path: path(&["title"]),
                ty: Type::String,
                constraints: Vec::new(),
            }],
            indexes: vec![],
            primary_field: Some("title".to_string()),
        };
        let bytes = encode_catalog_payload(&rec);
        let got = decode_catalog_payload(&bytes).unwrap();
        assert_eq!(got, rec);
    }

    #[test]
    fn roundtrip_new_version() {
        let rec = CatalogRecordWire::NewSchemaVersion {
            collection_id: 1,
            schema_version: 2,
            fields: vec![],
            indexes: vec![],
        };
        let bytes = encode_catalog_payload(&rec);
        let got = decode_catalog_payload(&bytes).unwrap();
        assert_eq!(got, rec);
    }

    #[test]
    fn nested_type_depth_limit() {
        let mut t = Type::Bool;
        for _ in 0..40 {
            t = Type::Optional(Box::new(t));
        }
        let mut out = Vec::new();
        encode_type(&mut out, &t, 0);
        let mut cur = Cursor::new(&out);
        let err = decode_type(&mut cur, 0);
        assert!(err.is_err());
    }

    #[test]
    fn decode_catalog_payload_rejects_unknown_version_kind_and_trailing_bytes() {
        // Unknown payload version.
        let mut b = Vec::new();
        b.extend_from_slice(&999u16.to_le_bytes());
        b.extend_from_slice(&0u16.to_le_bytes());
        assert!(decode_catalog_payload(&b).is_err());

        // Unknown entry kind (but valid version).
        let mut b2 = Vec::new();
        b2.extend_from_slice(&CATALOG_PAYLOAD_VERSION_V4.to_le_bytes());
        b2.extend_from_slice(&999u16.to_le_bytes());
        let err = decode_catalog_payload(&b2).unwrap_err();
        assert!(matches!(err, DbError::Format(_)));

        // Trailing bytes after a valid new schema version.
        let rec = CatalogRecordWire::NewSchemaVersion {
            collection_id: 1,
            schema_version: 2,
            fields: vec![],
            indexes: vec![],
        };
        let mut b3 = encode_catalog_payload(&rec);
        b3.push(0);
        let err = decode_catalog_payload(&b3).unwrap_err();
        assert!(matches!(err, DbError::Format(_)));
    }

    #[test]
    fn decode_name_and_field_path_error_branches() {
        // decode_name: n=0
        let z = 0u32.to_le_bytes();
        let mut cur = Cursor::new(&z);
        let err = decode_name(&mut cur).unwrap_err();
        assert!(matches!(err, DbError::Format(_)));

        // decode_field_path: n=0 -> schema invalid field path
        let z2 = 0u32.to_le_bytes();
        let mut cur2 = Cursor::new(&z2);
        let err = decode_field_path(&mut cur2).unwrap_err();
        assert!(matches!(err, DbError::Schema(_)));

        // decode_field_path: one segment with len=0 -> schema invalid field path
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1u32.to_le_bytes()); // n
        bytes.extend_from_slice(&0u32.to_le_bytes()); // len
        let mut cur3 = Cursor::new(&bytes);
        let err = decode_field_path(&mut cur3).unwrap_err();
        assert!(matches!(err, DbError::Schema(_)));
    }

    #[test]
    fn decode_constraints_and_type_unknown_tags() {
        // decode_constraints unknown tag.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1u32.to_le_bytes()); // n=1
        bytes.push(0xFF);
        let mut cur = Cursor::new(&bytes);
        let err = decode_constraints(&mut cur).unwrap_err();
        assert!(matches!(err, DbError::Format(_)));

        // decode_type unknown type tag.
        let mut cur2 = Cursor::new(&[0xFF]);
        let err = decode_type(&mut cur2, 0).unwrap_err();
        assert!(matches!(err, DbError::Format(_)));
    }

    #[test]
    fn cursor_unexpected_eof_branches() {
        let mut cur = Cursor::new(&[]);
        assert!(cur.take_u8().is_err());
        assert!(cur.take_u16().is_err());
        assert!(cur.take_u32().is_err());
        assert!(cur.take_u64().is_err());
        assert!(cur.take_bytes(1).is_err());
    }

    #[test]
    fn constraints_and_indexes_roundtrip_hits_all_codec_arms() {
        let rec = CatalogRecordWire::CreateCollection {
            collection_id: 123,
            name: "c".to_string(),
            schema_version: 1,
            fields: vec![FieldDef {
                path: path(&["f"]),
                ty: Type::String,
                constraints: vec![
                    Constraint::MinI64(-7),
                    Constraint::MaxI64(9),
                    Constraint::MinU64(1),
                    Constraint::MaxU64(2),
                    Constraint::MinF64(1.25),
                    Constraint::MaxF64(9.5),
                    Constraint::MinLength(3),
                    Constraint::MaxLength(10),
                    Constraint::Regex("^[a-z]+$".to_string()),
                    Constraint::Email,
                    Constraint::Url,
                    Constraint::NonEmpty,
                ],
            }],
            indexes: vec![
                IndexDef {
                    name: "u".to_string(),
                    path: path(&["f"]),
                    kind: IndexKind::Unique,
                },
                IndexDef {
                    name: "n".to_string(),
                    path: path(&["f"]),
                    kind: IndexKind::NonUnique,
                },
            ],
            primary_field: None,
        };

        let bytes = encode_catalog_payload(&rec);
        let got = decode_catalog_payload(&bytes).unwrap();
        assert_eq!(got, rec);
    }

    #[test]
    fn decode_fields_and_new_schema_version_old_versions_take_empty_vectors() {
        // decode_fields() with catalog version < v3 uses empty constraints vec.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1u32.to_le_bytes()); // n fields
        encode_field_path(&mut bytes, &path(&["a"]));
        encode_type(&mut bytes, &Type::Bool, 0);
        let mut cur = Cursor::new(&bytes);
        let fields = decode_fields(&mut cur, CATALOG_PAYLOAD_VERSION_V2).unwrap();
        assert_eq!(fields.len(), 1);
        assert!(fields[0].constraints.is_empty());

        // NewSchemaVersion with catalog ver < v4 uses empty indexes vec.
        let mut payload = Vec::new();
        payload.extend_from_slice(&CATALOG_PAYLOAD_VERSION_V3.to_le_bytes());
        payload.extend_from_slice(&ENTRY_KIND_NEW_SCHEMA_VERSION.to_le_bytes());
        payload.extend_from_slice(&1u32.to_le_bytes()); // collection_id
        payload.extend_from_slice(&2u32.to_le_bytes()); // schema_version
        payload.extend_from_slice(&0u32.to_le_bytes()); // n fields = 0
        let rec = decode_catalog_payload(&payload).unwrap();
        assert!(matches!(
            rec,
            CatalogRecordWire::NewSchemaVersion { indexes, .. } if indexes.is_empty()
        ));
    }

    #[test]
    fn decode_indexes_rejects_empty_name_and_too_long_name() {
        // Empty index name.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&1u32.to_le_bytes()); // n=1
        bytes.push(1); // kind unique
        encode_field_path(&mut bytes, &path(&["a"]));
        bytes.extend_from_slice(&0u32.to_le_bytes()); // name_len=0
        let mut cur = Cursor::new(&bytes);
        let err = decode_indexes(&mut cur).unwrap_err();
        assert!(matches!(err, DbError::Format(_)));

        // Too-long index name.
        let mut bytes2 = Vec::new();
        bytes2.extend_from_slice(&1u32.to_le_bytes()); // n=1
        bytes2.push(1); // kind unique
        encode_field_path(&mut bytes2, &path(&["a"]));
        bytes2.extend_from_slice(&(MAX_COLLECTION_NAME_BYTES as u32 + 1).to_le_bytes());
        let mut cur2 = Cursor::new(&bytes2);
        let err = decode_indexes(&mut cur2).unwrap_err();
        assert!(matches!(err, DbError::Format(_)));
    }
