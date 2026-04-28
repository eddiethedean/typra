//! Replay schema and record segments into [`crate::catalog::Catalog`] and in-memory latest-row maps.
//!
//! Legacy catalog entries may omit `primary_field`; record segments for those collections are
//! skipped during replay (insert was never supported without a primary key).
//!
//! Format minor **6+** uses [`SegmentType::TxnBegin`] / [`SegmentType::TxnCommit`] framing; older
//! minors use the legacy three-pass replay.

use std::collections::{BTreeMap, HashMap};

use crate::catalog::{decode_catalog_payload, Catalog};
use crate::error::{DbError, FormatError, SchemaError};
use crate::file_format::FORMAT_MINOR_V6;
use crate::index::{decode_index_payload, IndexState};
use crate::record::{decode_record_payload, RowValue};
use crate::schema::CollectionId;
use crate::segments::header::SegmentType;
use crate::segments::reader::{read_segment_payload, scan_segments};
use crate::storage::Store;
use crate::txn::decode_txn_payload_v0;

use super::LatestMap;

enum StagedSegment {
    Schema(Vec<u8>),
    Index(Vec<u8>),
    Record(Vec<u8>),
}

pub(crate) fn load_catalog_latest_and_indexes<S: Store>(
    store: &mut S,
    segment_start: u64,
    format_minor: u16,
) -> Result<(Catalog, LatestMap, IndexState), DbError> {
    if format_minor < FORMAT_MINOR_V6 {
        load_catalog_latest_and_indexes_legacy(store, segment_start)
    } else {
        load_catalog_latest_and_indexes_v6(store, segment_start)
    }
}

pub(crate) fn replay_tail_into<S: Store>(
    store: &mut S,
    start: u64,
    format_minor: u16,
    catalog: &mut Catalog,
    latest: &mut LatestMap,
    indexes: &mut IndexState,
) -> Result<(), DbError> {
    if format_minor < FORMAT_MINOR_V6 {
        replay_tail_legacy(store, start, catalog, latest, indexes)
    } else {
        replay_tail_v6(store, start, catalog, latest, indexes)
    }
}

fn replay_tail_legacy<S: Store>(
    store: &mut S,
    start: u64,
    catalog: &mut Catalog,
    latest: &mut LatestMap,
    indexes: &mut IndexState,
) -> Result<(), DbError> {
    let metas = scan_segments(store, start)?;
    for meta in &metas {
        if meta.header.segment_type != SegmentType::Schema {
            continue;
        }
        let payload = read_segment_payload(store, meta)?;
        let record = decode_catalog_payload(&payload)?;
        catalog.apply_record(record)?;
    }
    for meta in &metas {
        if meta.header.segment_type != SegmentType::Index {
            continue;
        }
        let payload = read_segment_payload(store, meta)?;
        let entries = decode_index_payload(&payload)?;
        for e in entries {
            indexes.apply(e)?;
        }
    }
    for meta in &metas {
        if meta.header.segment_type != SegmentType::Record {
            continue;
        }
        let payload = read_segment_payload(store, meta)?;
        apply_record_segment(&payload, catalog, latest)?;
    }
    Ok(())
}

fn replay_tail_v6<S: Store>(
    store: &mut S,
    start: u64,
    catalog: &mut Catalog,
    latest: &mut LatestMap,
    indexes: &mut IndexState,
) -> Result<(), DbError> {
    let metas = scan_segments(store, start)?;

    let mut committed: Vec<StagedSegment> = Vec::new();
    let mut in_txn = false;
    let mut pending_txn_id: Option<u64> = None;
    let mut staged: Vec<StagedSegment> = Vec::new();

    for meta in &metas {
        match meta.header.segment_type {
            SegmentType::Manifest | SegmentType::Checkpoint | SegmentType::Temp => {}
            SegmentType::TxnBegin => {
                if in_txn {
                    return Err(DbError::Format(FormatError::InvalidTxnPayload {
                        message: "nested TxnBegin in replay".into(),
                    }));
                }
                let payload = read_segment_payload(store, meta)?;
                let id = decode_txn_payload_v0(&payload)?;
                in_txn = true;
                pending_txn_id = Some(id);
                staged.clear();
            }
            SegmentType::TxnCommit => {
                let payload = read_segment_payload(store, meta)?;
                let id = decode_txn_payload_v0(&payload)?;
                if !in_txn {
                    return Err(DbError::Format(FormatError::InvalidTxnPayload {
                        message: "TxnCommit outside transaction in replay".into(),
                    }));
                }
                if pending_txn_id != Some(id) {
                    return Err(DbError::Format(FormatError::InvalidTxnPayload {
                        message: "TxnCommit txn_id mismatch in replay".into(),
                    }));
                }
                committed.append(&mut staged);
                in_txn = false;
                pending_txn_id = None;
            }
            SegmentType::TxnAbort => {
                let _ = decode_txn_payload_v0(&read_segment_payload(store, meta)?)?;
                staged.clear();
                in_txn = false;
                pending_txn_id = None;
            }
            SegmentType::Schema => {
                if !in_txn {
                    return Err(DbError::Format(FormatError::InvalidTxnPayload {
                        message: "unframed data segment in format minor 6".into(),
                    }));
                }
                let payload = read_segment_payload(store, meta)?;
                staged.push(StagedSegment::Schema(payload));
            }
            SegmentType::Index => {
                if !in_txn {
                    return Err(DbError::Format(FormatError::InvalidTxnPayload {
                        message: "unframed data segment in format minor 6".into(),
                    }));
                }
                let payload = read_segment_payload(store, meta)?;
                staged.push(StagedSegment::Index(payload));
            }
            SegmentType::Record => {
                if !in_txn {
                    return Err(DbError::Format(FormatError::InvalidTxnPayload {
                        message: "unframed data segment in format minor 6".into(),
                    }));
                }
                let payload = read_segment_payload(store, meta)?;
                staged.push(StagedSegment::Record(payload));
            }
        }
    }

    if in_txn {
        return Err(DbError::Format(FormatError::InvalidTxnPayload {
            message: "unclosed transaction at end of log (recovery should truncate)".into(),
        }));
    }

    for seg in &committed {
        if let StagedSegment::Schema(bytes) = seg {
            let record = decode_catalog_payload(bytes)?;
            catalog.apply_record(record)?;
        }
    }
    for seg in &committed {
        if let StagedSegment::Index(bytes) = seg {
            let entries = decode_index_payload(bytes)?;
            for e in entries {
                indexes.apply(e)?;
            }
        }
    }
    for seg in &committed {
        if let StagedSegment::Record(bytes) = seg {
            apply_record_segment(bytes, catalog, latest)?;
        }
    }

    Ok(())
}

fn load_catalog_latest_and_indexes_legacy<S: Store>(
    store: &mut S,
    segment_start: u64,
) -> Result<(Catalog, LatestMap, IndexState), DbError> {
    let metas = scan_segments(store, segment_start)?;
    let mut catalog = Catalog::default();
    for meta in &metas {
        if meta.header.segment_type != SegmentType::Schema {
            continue;
        }
        let payload = read_segment_payload(store, meta)?;
        let record = decode_catalog_payload(&payload)?;
        catalog.apply_record(record)?;
    }

    let mut latest = HashMap::new();
    let mut indexes = IndexState::default();
    for meta in &metas {
        if meta.header.segment_type != SegmentType::Index {
            continue;
        }
        let payload = read_segment_payload(store, meta)?;
        let entries = decode_index_payload(&payload)?;
        for e in entries {
            indexes.apply(e)?;
        }
    }
    for meta in &metas {
        if meta.header.segment_type != SegmentType::Record {
            continue;
        }
        let payload = read_segment_payload(store, meta)?;
        apply_record_segment(&payload, &catalog, &mut latest)?;
    }
    Ok((catalog, latest, indexes))
}

fn load_catalog_latest_and_indexes_v6<S: Store>(
    store: &mut S,
    segment_start: u64,
) -> Result<(Catalog, LatestMap, IndexState), DbError> {
    let metas = scan_segments(store, segment_start)?;

    // Collect only segments that are part of a committed transaction.
    let mut committed: Vec<StagedSegment> = Vec::new();
    let mut in_txn = false;
    let mut pending_txn_id: Option<u64> = None;
    let mut staged: Vec<StagedSegment> = Vec::new();

    for meta in &metas {
        match meta.header.segment_type {
            SegmentType::Manifest | SegmentType::Checkpoint | SegmentType::Temp => {}
            SegmentType::TxnBegin => {
                if in_txn {
                    return Err(DbError::Format(FormatError::InvalidTxnPayload {
                        message: "nested TxnBegin in replay".into(),
                    }));
                }
                let payload = read_segment_payload(store, meta)?;
                let id = decode_txn_payload_v0(&payload)?;
                in_txn = true;
                pending_txn_id = Some(id);
                staged.clear();
            }
            SegmentType::TxnCommit => {
                let payload = read_segment_payload(store, meta)?;
                let id = decode_txn_payload_v0(&payload)?;
                if !in_txn {
                    return Err(DbError::Format(FormatError::InvalidTxnPayload {
                        message: "TxnCommit outside transaction in replay".into(),
                    }));
                }
                if pending_txn_id != Some(id) {
                    return Err(DbError::Format(FormatError::InvalidTxnPayload {
                        message: "TxnCommit txn_id mismatch in replay".into(),
                    }));
                }
                committed.append(&mut staged);
                in_txn = false;
                pending_txn_id = None;
            }
            SegmentType::TxnAbort => {
                let _ = decode_txn_payload_v0(&read_segment_payload(store, meta)?)?;
                staged.clear();
                in_txn = false;
                pending_txn_id = None;
            }
            SegmentType::Schema => {
                // Format minor 6 mandates transactional framing. If we encounter unframed
                // segments, treat them as invalid.
                if !in_txn {
                    return Err(DbError::Format(FormatError::InvalidTxnPayload {
                        message: "unframed data segment in format minor 6".into(),
                    }));
                }
                let payload = read_segment_payload(store, meta)?;
                staged.push(StagedSegment::Schema(payload));
            }
            SegmentType::Index => {
                if !in_txn {
                    return Err(DbError::Format(FormatError::InvalidTxnPayload {
                        message: "unframed data segment in format minor 6".into(),
                    }));
                }
                let payload = read_segment_payload(store, meta)?;
                staged.push(StagedSegment::Index(payload));
            }
            SegmentType::Record => {
                if !in_txn {
                    return Err(DbError::Format(FormatError::InvalidTxnPayload {
                        message: "unframed data segment in format minor 6".into(),
                    }));
                }
                let payload = read_segment_payload(store, meta)?;
                staged.push(StagedSegment::Record(payload));
            }
        }
    }

    if in_txn {
        return Err(DbError::Format(FormatError::InvalidTxnPayload {
            message: "unclosed transaction at end of log (recovery should truncate)".into(),
        }));
    }

    // Apply committed segments with the same semantics as legacy replay:
    // - build the full catalog from Schema segments first,
    // - then apply Index batches,
    // - then apply Record segments (validated against the final catalog version).
    let mut catalog = Catalog::default();
    for seg in &committed {
        if let StagedSegment::Schema(bytes) = seg {
            let record = decode_catalog_payload(bytes)?;
            catalog.apply_record(record)?;
        }
    }

    let mut indexes = IndexState::default();
    for seg in &committed {
        if let StagedSegment::Index(bytes) = seg {
            let entries = decode_index_payload(bytes)?;
            for e in entries {
                indexes.apply(e)?;
            }
        }
    }

    let mut latest = HashMap::new();
    for seg in &committed {
        if let StagedSegment::Record(bytes) = seg {
            apply_record_segment(bytes, &catalog, &mut latest)?;
        }
    }

    Ok((catalog, latest, indexes))
}

#[cfg(test)]
mod tests {
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
}

fn apply_record_segment(
    payload: &[u8],
    catalog: &Catalog,
    latest: &mut LatestMap,
) -> Result<(), DbError> {
    if payload.len() < 6 {
        return Err(DbError::Format(FormatError::TruncatedRecordPayload));
    }
    let collection_id = u32::from_le_bytes([payload[2], payload[3], payload[4], payload[5]]);
    let col = catalog
        .get(CollectionId(collection_id))
        .ok_or(DbError::Schema(SchemaError::UnknownCollection {
            id: collection_id,
        }))?;
    let pk_name = match &col.primary_field {
        Some(s) => s.as_str(),
        None => return Ok(()),
    };
    let pk_ty = col
        .fields
        .iter()
        .find(|f| f.path.0.len() == 1 && f.path.0[0] == pk_name)
        .map(|f| &f.ty)
        .ok_or(DbError::Schema(SchemaError::PrimaryFieldNotFound {
            name: pk_name.to_string(),
        }))?;
    let decoded = decode_record_payload(payload, pk_name, pk_ty, &col.fields)?;
    if decoded.schema_version != col.current_version.0 {
        return Err(DbError::Schema(SchemaError::InvalidSchemaVersion {
            expected: col.current_version.0,
            got: decoded.schema_version,
        }));
    }
    let pk_key = decoded.pk.canonical_key_bytes();
    if decoded.op == crate::record::OP_DELETE {
        latest.remove(&(collection_id, pk_key));
        return Ok(());
    }

    let mut full: BTreeMap<String, RowValue> = BTreeMap::new();
    full.insert(
        pk_name.to_string(),
        RowValue::from_scalar(decoded.pk.clone()),
    );
    for (k, v) in decoded.fields {
        full.insert(k, v);
    }
    latest.insert((collection_id, pk_key), full);
    Ok(())
}
