    use std::borrow::Cow;

    use super::{load_catalog_latest_and_indexes, replay_tail_into};
    use crate::catalog::{encode_catalog_payload, Catalog, CatalogRecordWire};
    use crate::error::{DbError, FormatError, SchemaError};
    use crate::file_format::{FileHeader, FORMAT_MINOR, FILE_HEADER_SIZE};
    use crate::index::{encode_index_payload, IndexEntry, IndexOp, IndexState};
    use crate::record::{encode_record_payload_v2, encode_record_payload_v2_op, RowValue, ScalarValue};
    use crate::schema::{FieldDef, FieldPath, IndexDef, IndexKind, Type};
    use crate::segments::header::{SegmentHeader, SegmentType};
    use crate::segments::writer::SegmentWriter;
    use crate::storage::{Store, VecStore};
    use crate::superblock::SUPERBLOCK_SIZE;
    use crate::txn::encode_txn_payload_v0;

    fn segment_start() -> u64 {
        (FILE_HEADER_SIZE + 2 * SUPERBLOCK_SIZE) as u64
    }

    fn seed_store_v0_5(store: &mut VecStore) {
        let h = FileHeader::new_v0_5();
        store.write_all_at(0, &h.encode()).unwrap();
        crate::db::open::init_superblocks(store, segment_start()).unwrap();
        let _ = crate::publish::append_manifest_and_publish(store, segment_start()).unwrap();
    }

    fn seed_store_v0_8(store: &mut VecStore) {
        let h = FileHeader::new_v0_8();
        store.write_all_at(0, &h.encode()).unwrap();
        crate::db::open::init_superblocks(store, segment_start()).unwrap();
        let _ = crate::publish::append_manifest_and_publish(store, segment_start()).unwrap();
    }

    fn append_segment(store: &mut VecStore, segment_type: SegmentType, payload: &[u8]) {
        let file_len = store.len().unwrap();
        let mut w = SegmentWriter::new(store, file_len);
        w.append(
            SegmentHeader {
                segment_type,
                payload_len: 0,
                payload_crc32c: 0,
            },
            payload,
        )
        .unwrap();
    }

    #[test]
    fn legacy_load_and_replay_paths_work() {
        let mut store = VecStore::new();
        seed_store_v0_5(&mut store);

        // Create one collection schema (id pk, year non-pk) and one record.
        let fields = vec![
            FieldDef::new(FieldPath::new([Cow::Borrowed("id")]).unwrap(), Type::String),
            FieldDef::new(FieldPath::new([Cow::Borrowed("year")]).unwrap(), Type::Int64),
        ];
        let wire = CatalogRecordWire::CreateCollection {
            collection_id: 1,
            name: "books".into(),
            schema_version: 1,
            fields: fields.clone(),
            indexes: vec![IndexDef {
                name: "year_idx".into(),
                path: FieldPath::new([Cow::Borrowed("year")]).unwrap(),
                kind: IndexKind::NonUnique,
            }],
            primary_field: Some("id".into()),
        };
        append_segment(&mut store, SegmentType::Schema, &encode_catalog_payload(&wire));

        let payload = encode_record_payload_v2(
            1,
            1,
            &ScalarValue::String("k".into()),
            &Type::String,
            &[(
                fields[1].clone(),
                RowValue::Int64(2020),
            )],
        )
        .unwrap();
        append_segment(&mut store, SegmentType::Record, &payload);

        let idx_entry = IndexEntry {
            collection_id: 1,
            index_name: "year_idx".into(),
            kind: IndexKind::NonUnique,
            op: IndexOp::Insert,
            index_key: ScalarValue::Int64(2020).canonical_key_bytes(),
            pk_key: ScalarValue::String("k".into()).canonical_key_bytes(),
        };
        append_segment(&mut store, SegmentType::Index, &encode_index_payload(&[idx_entry]));

        // Legacy load path: FORMAT_MINOR (5) is < v6.
        let (catalog, latest, indexes) =
            load_catalog_latest_and_indexes(&mut store, segment_start(), FORMAT_MINOR).unwrap();
        assert!(catalog.get(crate::schema::CollectionId(1)).is_some());
        assert!(!latest.is_empty());
        assert!(indexes
            .non_unique_lookup(1, "year_idx", &ScalarValue::Int64(2020).canonical_key_bytes())
            .is_some());

        // Legacy replay_tail_into path: start at segment_start.
        let mut catalog2 = Catalog::default();
        let mut latest2 = super::LatestMap::new();
        let mut indexes2 = IndexState::default();
        replay_tail_into(
            &mut store,
            segment_start(),
            FORMAT_MINOR,
            &mut catalog2,
            &mut latest2,
            &mut indexes2,
        )
        .unwrap();
        assert!(catalog2.get(crate::schema::CollectionId(1)).is_some());
        assert_eq!(latest2.len(), 1);
    }

    #[test]
    fn v6_replay_tail_errors_for_txn_framing_issues() {
        let mut store = VecStore::new();
        seed_store_v0_8(&mut store);

        // Nested begin.
        append_segment(&mut store, SegmentType::TxnBegin, &encode_txn_payload_v0(1));
        append_segment(&mut store, SegmentType::TxnBegin, &encode_txn_payload_v0(2));
        let mut cat = Catalog::default();
        let mut latest = super::LatestMap::new();
        let mut idx = IndexState::default();
        let err = replay_tail_into(&mut store, segment_start(), crate::file_format::FORMAT_MINOR_V6, &mut cat, &mut latest, &mut idx)
            .unwrap_err();
        assert!(matches!(err, DbError::Format(FormatError::InvalidTxnPayload { .. })));

        // Commit outside transaction.
        let mut store2 = VecStore::new();
        seed_store_v0_8(&mut store2);
        append_segment(&mut store2, SegmentType::TxnCommit, &encode_txn_payload_v0(1));
        let mut cat2 = Catalog::default();
        let mut latest2 = super::LatestMap::new();
        let mut idx2 = IndexState::default();
        let err = replay_tail_into(&mut store2, segment_start(), crate::file_format::FORMAT_MINOR_V6, &mut cat2, &mut latest2, &mut idx2)
            .unwrap_err();
        assert!(matches!(err, DbError::Format(FormatError::InvalidTxnPayload { .. })));

        // Commit mismatch.
        let mut store3 = VecStore::new();
        seed_store_v0_8(&mut store3);
        append_segment(&mut store3, SegmentType::TxnBegin, &encode_txn_payload_v0(1));
        append_segment(&mut store3, SegmentType::TxnCommit, &encode_txn_payload_v0(2));
        let mut cat3 = Catalog::default();
        let mut latest3 = super::LatestMap::new();
        let mut idx3 = IndexState::default();
        let err = replay_tail_into(&mut store3, segment_start(), crate::file_format::FORMAT_MINOR_V6, &mut cat3, &mut latest3, &mut idx3)
            .unwrap_err();
        assert!(matches!(err, DbError::Format(FormatError::InvalidTxnPayload { .. })));

        // Unframed data segment.
        let mut store4 = VecStore::new();
        seed_store_v0_8(&mut store4);
        append_segment(&mut store4, SegmentType::Schema, &[0u8; 0]);
        let mut cat4 = Catalog::default();
        let mut latest4 = super::LatestMap::new();
        let mut idx4 = IndexState::default();
        let err = replay_tail_into(&mut store4, segment_start(), crate::file_format::FORMAT_MINOR_V6, &mut cat4, &mut latest4, &mut idx4)
            .unwrap_err();
        assert!(matches!(err, DbError::Format(FormatError::InvalidTxnPayload { .. })));

        // Unframed index.
        let mut store4b = VecStore::new();
        seed_store_v0_8(&mut store4b);
        append_segment(&mut store4b, SegmentType::Index, &[]);
        let err = replay_tail_into(
            &mut store4b,
            segment_start(),
            crate::file_format::FORMAT_MINOR_V6,
            &mut Catalog::default(),
            &mut super::LatestMap::new(),
            &mut IndexState::default(),
        )
        .unwrap_err();
        assert!(matches!(err, DbError::Format(FormatError::InvalidTxnPayload { .. })));

        // Unframed record.
        let mut store4c = VecStore::new();
        seed_store_v0_8(&mut store4c);
        append_segment(&mut store4c, SegmentType::Record, &[]);
        let err = replay_tail_into(
            &mut store4c,
            segment_start(),
            crate::file_format::FORMAT_MINOR_V6,
            &mut Catalog::default(),
            &mut super::LatestMap::new(),
            &mut IndexState::default(),
        )
        .unwrap_err();
        assert!(matches!(err, DbError::Format(FormatError::InvalidTxnPayload { .. })));

        // Unclosed transaction.
        let mut store5 = VecStore::new();
        seed_store_v0_8(&mut store5);
        append_segment(&mut store5, SegmentType::TxnBegin, &encode_txn_payload_v0(1));
        let mut cat5 = Catalog::default();
        let mut latest5 = super::LatestMap::new();
        let mut idx5 = IndexState::default();
        let err = replay_tail_into(&mut store5, segment_start(), crate::file_format::FORMAT_MINOR_V6, &mut cat5, &mut latest5, &mut idx5)
            .unwrap_err();
        assert!(matches!(err, DbError::Format(FormatError::InvalidTxnPayload { .. })));
    }

    #[test]
    fn v6_load_and_replay_happy_path_and_abort_path() {
        let mut store = VecStore::new();
        seed_store_v0_8(&mut store);

        // Build one collection schema (id pk, year non-pk), one record, one index entry.
        let fields = vec![
            FieldDef::new(FieldPath::new([Cow::Borrowed("id")]).unwrap(), Type::String),
            FieldDef::new(FieldPath::new([Cow::Borrowed("year")]).unwrap(), Type::Int64),
        ];
        let wire = CatalogRecordWire::CreateCollection {
            collection_id: 1,
            name: "books".into(),
            schema_version: 1,
            fields: fields.clone(),
            indexes: vec![IndexDef {
                name: "year_idx".into(),
                path: FieldPath::new([Cow::Borrowed("year")]).unwrap(),
                kind: IndexKind::NonUnique,
            }],
            primary_field: Some("id".into()),
        };
        let schema_payload = encode_catalog_payload(&wire);
        let record_payload = encode_record_payload_v2(
            1,
            1,
            &ScalarValue::String("k".into()),
            &Type::String,
            &[(fields[1].clone(), RowValue::Int64(2020))],
        )
        .unwrap();
        let idx_entry = IndexEntry {
            collection_id: 1,
            index_name: "year_idx".into(),
            kind: IndexKind::NonUnique,
            op: IndexOp::Insert,
            index_key: ScalarValue::Int64(2020).canonical_key_bytes(),
            pk_key: ScalarValue::String("k".into()).canonical_key_bytes(),
        };
        let index_payload = encode_index_payload(&[idx_entry]);

        // First transaction: stage schema then abort (should be discarded).
        append_segment(&mut store, SegmentType::TxnBegin, &encode_txn_payload_v0(1));
        append_segment(&mut store, SegmentType::Schema, &schema_payload);
        append_segment(&mut store, SegmentType::TxnAbort, &encode_txn_payload_v0(1));

        // Second transaction: stage full schema+index+record then commit.
        append_segment(&mut store, SegmentType::TxnBegin, &encode_txn_payload_v0(2));
        append_segment(&mut store, SegmentType::Schema, &schema_payload);
        append_segment(&mut store, SegmentType::Index, &index_payload);
        append_segment(&mut store, SegmentType::Record, &record_payload);
        append_segment(&mut store, SegmentType::TxnCommit, &encode_txn_payload_v0(2));

        // v6 load path should see only committed txn (the aborted schema is ignored).
        let (catalog, latest, indexes) = load_catalog_latest_and_indexes(
            &mut store,
            segment_start(),
            crate::file_format::FORMAT_MINOR_V6,
        )
        .unwrap();
        assert!(catalog.get(crate::schema::CollectionId(1)).is_some());
        assert_eq!(latest.len(), 1);
        assert!(indexes
            .non_unique_lookup(1, "year_idx", &ScalarValue::Int64(2020).canonical_key_bytes())
            .is_some());

        // v6 replay_tail_into path also works and exercises the abort branch.
        let mut catalog2 = Catalog::default();
        let mut latest2 = super::LatestMap::new();
        let mut indexes2 = IndexState::default();
        replay_tail_into(
            &mut store,
            segment_start(),
            crate::file_format::FORMAT_MINOR_V6,
            &mut catalog2,
            &mut latest2,
            &mut indexes2,
        )
        .unwrap();
        assert!(catalog2.get(crate::schema::CollectionId(1)).is_some());
        assert_eq!(latest2.len(), 1);
    }

    #[test]
    fn v6_load_catalog_latest_and_indexes_errors_match_replay_errors() {
        // Nested begin.
        let mut store = VecStore::new();
        seed_store_v0_8(&mut store);
        append_segment(&mut store, SegmentType::TxnBegin, &encode_txn_payload_v0(1));
        append_segment(&mut store, SegmentType::TxnBegin, &encode_txn_payload_v0(2));
        let err = load_catalog_latest_and_indexes(
            &mut store,
            segment_start(),
            crate::file_format::FORMAT_MINOR_V6,
        )
        .unwrap_err();
        assert!(matches!(err, DbError::Format(FormatError::InvalidTxnPayload { .. })));

        // Commit outside transaction.
        let mut store4 = VecStore::new();
        seed_store_v0_8(&mut store4);
        append_segment(&mut store4, SegmentType::TxnCommit, &encode_txn_payload_v0(1));
        let err = load_catalog_latest_and_indexes(
            &mut store4,
            segment_start(),
            crate::file_format::FORMAT_MINOR_V6,
        )
        .unwrap_err();
        assert!(matches!(err, DbError::Format(FormatError::InvalidTxnPayload { .. })));

        // Commit mismatch.
        let mut store5 = VecStore::new();
        seed_store_v0_8(&mut store5);
        append_segment(&mut store5, SegmentType::TxnBegin, &encode_txn_payload_v0(1));
        append_segment(&mut store5, SegmentType::TxnCommit, &encode_txn_payload_v0(2));
        let err = load_catalog_latest_and_indexes(
            &mut store5,
            segment_start(),
            crate::file_format::FORMAT_MINOR_V6,
        )
        .unwrap_err();
        assert!(matches!(err, DbError::Format(FormatError::InvalidTxnPayload { .. })));

        // Unframed data segment.
        let mut store2 = VecStore::new();
        seed_store_v0_8(&mut store2);
        append_segment(&mut store2, SegmentType::Schema, &[]);
        let err = load_catalog_latest_and_indexes(
            &mut store2,
            segment_start(),
            crate::file_format::FORMAT_MINOR_V6,
        )
        .unwrap_err();
        assert!(matches!(err, DbError::Format(FormatError::InvalidTxnPayload { .. })));

        // Unframed index.
        let mut store2b = VecStore::new();
        seed_store_v0_8(&mut store2b);
        append_segment(&mut store2b, SegmentType::Index, &[]);
        let err = load_catalog_latest_and_indexes(
            &mut store2b,
            segment_start(),
            crate::file_format::FORMAT_MINOR_V6,
        )
        .unwrap_err();
        assert!(matches!(err, DbError::Format(FormatError::InvalidTxnPayload { .. })));

        // Unframed record.
        let mut store2c = VecStore::new();
        seed_store_v0_8(&mut store2c);
        append_segment(&mut store2c, SegmentType::Record, &[]);
        let err = load_catalog_latest_and_indexes(
            &mut store2c,
            segment_start(),
            crate::file_format::FORMAT_MINOR_V6,
        )
        .unwrap_err();
        assert!(matches!(err, DbError::Format(FormatError::InvalidTxnPayload { .. })));

        // Unclosed transaction.
        let mut store3 = VecStore::new();
        seed_store_v0_8(&mut store3);
        append_segment(&mut store3, SegmentType::TxnBegin, &encode_txn_payload_v0(1));
        let err = load_catalog_latest_and_indexes(
            &mut store3,
            segment_start(),
            crate::file_format::FORMAT_MINOR_V6,
        )
        .unwrap_err();
        assert!(matches!(err, DbError::Format(FormatError::InvalidTxnPayload { .. })));
    }

    #[test]
    fn apply_record_segment_skips_collections_without_primary_key_and_handles_delete_and_schema_mismatch() {
        // Collection with no primary key: record segments are skipped.
        let mut catalog = Catalog::default();
        let wire = CatalogRecordWire::CreateCollection {
            collection_id: 1,
            name: "no_pk".into(),
            schema_version: 1,
            fields: vec![FieldDef::new(FieldPath::new([Cow::Borrowed("id")]).unwrap(), Type::String)],
            indexes: vec![],
            primary_field: None,
        };
        catalog.apply_record(wire).unwrap();

        let mut latest = super::LatestMap::new();
        let mut payload = vec![0u8; 6];
        payload[2..6].copy_from_slice(&1u32.to_le_bytes());
        super::apply_record_segment(&payload, &catalog, &mut latest).unwrap();
        assert!(latest.is_empty());

        // Normal collection with pk: schema mismatch errors.
        let mut catalog2 = Catalog::default();
        let fields = vec![
            FieldDef::new(FieldPath::new([Cow::Borrowed("id")]).unwrap(), Type::String),
            FieldDef::new(FieldPath::new([Cow::Borrowed("x")]).unwrap(), Type::Int64),
        ];
        let wire2 = CatalogRecordWire::CreateCollection {
            collection_id: 1,
            name: "t".into(),
            schema_version: 1,
            fields: fields.clone(),
            indexes: vec![],
            primary_field: Some("id".into()),
        };
        catalog2.apply_record(wire2).unwrap();

        let bad = encode_record_payload_v2(
            1,
            999,
            &ScalarValue::String("k".into()),
            &Type::String,
            &[(fields[1].clone(), RowValue::Int64(1))],
        )
        .unwrap();
        let err =
            super::apply_record_segment(&bad, &catalog2, &mut super::LatestMap::new()).unwrap_err();
        assert!(matches!(err, DbError::Schema(SchemaError::InvalidSchemaVersion { .. })));

        // Delete removes from latest.
        let mut latest2 = super::LatestMap::new();
        let ins = encode_record_payload_v2(
            1,
            1,
            &ScalarValue::String("k".into()),
            &Type::String,
            &[(fields[1].clone(), RowValue::Int64(1))],
        )
        .unwrap();
        super::apply_record_segment(&ins, &catalog2, &mut latest2).unwrap();
        assert_eq!(latest2.len(), 1);

        let del = encode_record_payload_v2_op(
            1,
            1,
            crate::record::OP_DELETE,
            &ScalarValue::String("k".into()),
            &Type::String,
            &[],
        )
        .unwrap();
        super::apply_record_segment(&del, &catalog2, &mut latest2).unwrap();
        assert!(latest2.is_empty());
    }
