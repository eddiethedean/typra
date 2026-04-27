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
        match meta.header.segment_type {
            SegmentType::Schema => {
                let payload = read_segment_payload(store, meta)?;
                let record = decode_catalog_payload(&payload)?;
                catalog.apply_record(record)?;
            }
            _ => continue,
        }
    }
    for meta in &metas {
        match meta.header.segment_type {
            SegmentType::Index => {
                let payload = read_segment_payload(store, meta)?;
                let entries = decode_index_payload(&payload)?;
                for e in entries {
                    indexes.apply(e)?;
                }
            }
            _ => continue,
        }
    }
    for meta in &metas {
        match meta.header.segment_type {
            SegmentType::Record => {
                let payload = read_segment_payload(store, meta)?;
                apply_record_segment(&payload, catalog, latest)?;
            }
            _ => continue,
        }
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
                match in_txn {
                    true => {
                        return Err(DbError::Format(FormatError::InvalidTxnPayload {
                            message: "nested TxnBegin in replay".into(),
                        }))
                    }
                    false => {}
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
                match in_txn {
                    true => {}
                    false => {
                        return Err(DbError::Format(FormatError::InvalidTxnPayload {
                            message: "TxnCommit outside transaction in replay".into(),
                        }))
                    }
                }
                match pending_txn_id {
                    None => {
                        return Err(DbError::Format(FormatError::InvalidTxnPayload {
                            message: "TxnCommit txn_id mismatch in replay".into(),
                        }))
                    }
                    Some(pt) => {
                        match pt == id {
                            true => {}
                            false => {
                                return Err(DbError::Format(FormatError::InvalidTxnPayload {
                                    message: "TxnCommit txn_id mismatch in replay".into(),
                                }))
                            }
                        }
                    }
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
            SegmentType::Schema | SegmentType::Index | SegmentType::Record => {
                match in_txn {
                    true => {}
                    false => {
                        return Err(DbError::Format(FormatError::InvalidTxnPayload {
                            message: "unframed data segment in format minor 6".into(),
                        }))
                    }
                }
                let payload = read_segment_payload(store, meta)?;
                match meta.header.segment_type {
                    SegmentType::Schema => staged.push(StagedSegment::Schema(payload)),
                    SegmentType::Index => staged.push(StagedSegment::Index(payload)),
                    SegmentType::Record => staged.push(StagedSegment::Record(payload)),
                    _ => {}
                }
            }
        }
    }

    if in_txn {
        return Err(DbError::Format(FormatError::InvalidTxnPayload {
            message: "unclosed transaction at end of log (recovery should truncate)".into(),
        }));
    }

    for seg in &committed {
        match seg {
            StagedSegment::Schema(bytes) => {
                let record = decode_catalog_payload(bytes)?;
                catalog.apply_record(record)?;
            }
            _ => {}
        }
    }
    for seg in &committed {
        match seg {
            StagedSegment::Index(bytes) => {
                let entries = decode_index_payload(bytes)?;
                for e in entries {
                    indexes.apply(e)?;
                }
            }
            _ => {}
        }
    }
    for seg in &committed {
        match seg {
            StagedSegment::Record(bytes) => apply_record_segment(bytes, catalog, latest)?,
            _ => {}
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
        match meta.header.segment_type {
            SegmentType::Schema => {
                let payload = read_segment_payload(store, meta)?;
                let record = decode_catalog_payload(&payload)?;
                catalog.apply_record(record)?;
            }
            _ => continue,
        }
    }

    let mut latest = HashMap::new();
    let mut indexes = IndexState::default();
    for meta in &metas {
        match meta.header.segment_type {
            SegmentType::Index => {
                let payload = read_segment_payload(store, meta)?;
                let entries = decode_index_payload(&payload)?;
                for e in entries {
                    indexes.apply(e)?;
                }
            }
            _ => continue,
        }
    }
    for meta in &metas {
        match meta.header.segment_type {
            SegmentType::Record => {
                let payload = read_segment_payload(store, meta)?;
                apply_record_segment(&payload, &catalog, &mut latest)?;
            }
            _ => continue,
        }
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
            SegmentType::Schema | SegmentType::Index | SegmentType::Record => {
                // Format minor 6 mandates transactional framing. If we encounter unframed
                // segments, treat them as invalid.
                if !in_txn {
                    return Err(DbError::Format(FormatError::InvalidTxnPayload {
                        message: "unframed data segment in format minor 6".into(),
                    }));
                }
                let payload = read_segment_payload(store, meta)?;
                match meta.header.segment_type {
                    SegmentType::Schema => staged.push(StagedSegment::Schema(payload)),
                    SegmentType::Index => staged.push(StagedSegment::Index(payload)),
                    SegmentType::Record => staged.push(StagedSegment::Record(payload)),
                    _ => {}
                }
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
    let mut pk_ty = None;
    for f in &col.fields {
        if f.path.0.len() != 1 {
            continue;
        }
        if f.path.0[0] != pk_name {
            continue;
        }
        pk_ty = Some(&f.ty);
        break;
    }
    let pk_ty = pk_ty.unwrap();
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

#[cfg(test)]
mod tests {
    use super::{load_catalog_latest_and_indexes, replay_tail_into, replay_tail_legacy};
    use crate::catalog::Catalog;
    use crate::catalog::{encode_catalog_payload, CatalogRecordWire};
    use crate::error::{DbError, FormatError};
    use crate::file_format::FORMAT_MINOR_V6;
    use crate::index::IndexState;
    use crate::index::{encode_index_payload, IndexEntry, IndexOp};
    use crate::record::{encode_record_payload_v3, encode_record_payload_v3_op, OP_DELETE};
    use crate::record::RowValue;
    use crate::schema::{FieldDef, FieldPath, IndexKind, Type};
    use crate::segments::header::{SegmentHeader, SegmentType};
    use crate::segments::writer::SegmentWriter;
    use crate::storage::{FileStore, VecStore};
    use crate::txn::encode_txn_payload_v0;
    use std::collections::HashMap;
    use std::borrow::Cow;
    use std::fs::OpenOptions as FsOpenOptions;

    #[test]
    fn replay_v6_rejects_unframed_data_segment() {
        let mut store = VecStore::new();
        let mut w = SegmentWriter::new(&mut store, 0);
        w.append(
            SegmentHeader {
                segment_type: SegmentType::Schema,
                payload_len: 0,
                payload_crc32c: 0,
            },
            &[],
        )
        .unwrap();

        let mut catalog = Catalog::default();
        let mut latest = HashMap::new();
        let mut idx = IndexState::default();
        let e = replay_tail_into(&mut store, 0, FORMAT_MINOR_V6, &mut catalog, &mut latest, &mut idx)
            .unwrap_err();
        assert!(matches!(
            e,
            DbError::Format(FormatError::InvalidTxnPayload { .. })
        ));
    }

    #[test]
    fn replay_v6_rejects_nested_begin() {
        let mut store = VecStore::new();
        let mut w = SegmentWriter::new(&mut store, 0);
        let begin = encode_txn_payload_v0(1);
        w.append(
            SegmentHeader {
                segment_type: SegmentType::TxnBegin,
                payload_len: 0,
                payload_crc32c: 0,
            },
            begin.as_slice(),
        )
        .unwrap();
        w.append(
            SegmentHeader {
                segment_type: SegmentType::TxnBegin,
                payload_len: 0,
                payload_crc32c: 0,
            },
            begin.as_slice(),
        )
        .unwrap();

        let mut catalog = Catalog::default();
        let mut latest = HashMap::new();
        let mut idx = IndexState::default();
        let e = replay_tail_into(&mut store, 0, FORMAT_MINOR_V6, &mut catalog, &mut latest, &mut idx)
            .unwrap_err();
        assert!(matches!(
            e,
            DbError::Format(FormatError::InvalidTxnPayload { .. })
        ));
    }

    #[test]
    fn replay_v6_rejects_commit_outside_transaction() {
        let mut store = VecStore::new();
        let mut w = SegmentWriter::new(&mut store, 0);
        let commit = encode_txn_payload_v0(1);
        w.append(
            SegmentHeader {
                segment_type: SegmentType::TxnCommit,
                payload_len: 0,
                payload_crc32c: 0,
            },
            commit.as_slice(),
        )
        .unwrap();

        let mut catalog = Catalog::default();
        let mut latest = HashMap::new();
        let mut idx = IndexState::default();
        let e = replay_tail_into(&mut store, 0, FORMAT_MINOR_V6, &mut catalog, &mut latest, &mut idx)
            .unwrap_err();
        assert!(matches!(
            e,
            DbError::Format(FormatError::InvalidTxnPayload { .. })
        ));
    }

    #[test]
    fn replay_v6_rejects_commit_id_mismatch() {
        let mut store = VecStore::new();
        let mut w = SegmentWriter::new(&mut store, 0);
        let begin = encode_txn_payload_v0(1);
        let commit = encode_txn_payload_v0(2);
        w.append(
            SegmentHeader {
                segment_type: SegmentType::TxnBegin,
                payload_len: 0,
                payload_crc32c: 0,
            },
            begin.as_slice(),
        )
        .unwrap();
        w.append(
            SegmentHeader {
                segment_type: SegmentType::TxnCommit,
                payload_len: 0,
                payload_crc32c: 0,
            },
            commit.as_slice(),
        )
        .unwrap();

        let mut catalog = Catalog::default();
        let mut latest = HashMap::new();
        let mut idx = IndexState::default();
        let e = replay_tail_into(&mut store, 0, FORMAT_MINOR_V6, &mut catalog, &mut latest, &mut idx)
            .unwrap_err();
        assert!(matches!(
            e,
            DbError::Format(FormatError::InvalidTxnPayload { .. })
        ));
    }

    #[test]
    fn replay_v6_errors_on_unclosed_txn_at_end() {
        let mut store = VecStore::new();
        let mut w = SegmentWriter::new(&mut store, 0);
        let begin = encode_txn_payload_v0(7);
        w.append(
            SegmentHeader {
                segment_type: SegmentType::TxnBegin,
                payload_len: 0,
                payload_crc32c: 0,
            },
            begin.as_slice(),
        )
        .unwrap();

        let mut catalog = Catalog::default();
        let mut latest = HashMap::new();
        let mut idx = IndexState::default();
        let e = replay_tail_into(&mut store, 0, FORMAT_MINOR_V6, &mut catalog, &mut latest, &mut idx)
            .unwrap_err();
        assert!(matches!(
            e,
            DbError::Format(FormatError::InvalidTxnPayload { .. })
        ));
    }

    #[test]
    fn replay_legacy_exercises_schema_index_and_record_loops_with_mixed_segments() {
        let mut store = VecStore::new();
        let mut w = SegmentWriter::new(&mut store, 0);

        // Write an Index segment first so the schema pass takes the `continue` branch at least once.
        let idx_bytes = encode_index_payload(&[IndexEntry {
            collection_id: 1,
            index_name: "i".to_string(),
            kind: IndexKind::Unique,
            op: IndexOp::Insert,
            index_key: vec![1],
            pk_key: vec![2],
        }]);
        w.append(
            SegmentHeader {
                segment_type: SegmentType::Index,
                payload_len: 0,
                payload_crc32c: 0,
            },
            &idx_bytes,
        )
        .unwrap();

        // Then a Schema segment that creates a collection with primary key `id`.
        let fields = vec![FieldDef {
            path: FieldPath(vec![Cow::Owned("id".to_string())]),
            ty: Type::Int64,
            constraints: vec![],
        }];
        let schema_bytes = encode_catalog_payload(&CatalogRecordWire::CreateCollection {
            collection_id: 1,
            name: "t".to_string(),
            schema_version: 1,
            fields: fields.clone(),
            indexes: vec![],
            primary_field: Some("id".to_string()),
        });
        w.append(
            SegmentHeader {
                segment_type: SegmentType::Schema,
                payload_len: 0,
                payload_crc32c: 0,
            },
            &schema_bytes,
        )
        .unwrap();

        // Finally a Record segment to exercise the record pass.
        let rec_bytes = encode_record_payload_v3(
            1,
            1,
            &crate::ScalarValue::Int64(7),
            &Type::Int64,
            &[],
        )
        .unwrap();
        w.append(
            SegmentHeader {
                segment_type: SegmentType::Record,
                payload_len: 0,
                payload_crc32c: 0,
            },
            &rec_bytes,
        )
        .unwrap();

        let mut catalog = Catalog::default();
        let mut latest = HashMap::new();
        let mut idx = IndexState::default();
        replay_tail_legacy(&mut store, 0, &mut catalog, &mut latest, &mut idx).unwrap();

        // The record should have been applied to latest.
        let key = crate::ScalarValue::Int64(7).canonical_key_bytes();
        let got = latest.get(&(1u32, key)).unwrap();
        assert_eq!(got.get("id"), Some(&RowValue::from_scalar(crate::ScalarValue::Int64(7))));
    }

    #[test]
    fn load_v6_rejects_unframed_data_segment() {
        let mut store = VecStore::new();
        let mut w = SegmentWriter::new(&mut store, 0);
        w.append(
            SegmentHeader {
                segment_type: SegmentType::Schema,
                payload_len: 0,
                payload_crc32c: 0,
            },
            &[],
        )
        .unwrap();

        let e = load_catalog_latest_and_indexes(&mut store, 0, FORMAT_MINOR_V6).unwrap_err();
        assert!(matches!(
            e,
            DbError::Format(FormatError::InvalidTxnPayload { .. })
        ));
    }

    #[test]
    fn load_v6_rejects_nested_begin() {
        let mut store = VecStore::new();
        let mut w = SegmentWriter::new(&mut store, 0);
        let begin = encode_txn_payload_v0(1);
        w.append(
            SegmentHeader {
                segment_type: SegmentType::TxnBegin,
                payload_len: 0,
                payload_crc32c: 0,
            },
            begin.as_slice(),
        )
        .unwrap();
        w.append(
            SegmentHeader {
                segment_type: SegmentType::TxnBegin,
                payload_len: 0,
                payload_crc32c: 0,
            },
            begin.as_slice(),
        )
        .unwrap();

        let e = load_catalog_latest_and_indexes(&mut store, 0, FORMAT_MINOR_V6).unwrap_err();
        assert!(matches!(
            e,
            DbError::Format(FormatError::InvalidTxnPayload { .. })
        ));
    }

    #[test]
    fn load_v6_rejects_commit_outside_transaction() {
        let mut store = VecStore::new();
        let mut w = SegmentWriter::new(&mut store, 0);
        let commit = encode_txn_payload_v0(1);
        w.append(
            SegmentHeader {
                segment_type: SegmentType::TxnCommit,
                payload_len: 0,
                payload_crc32c: 0,
            },
            commit.as_slice(),
        )
        .unwrap();

        let e = load_catalog_latest_and_indexes(&mut store, 0, FORMAT_MINOR_V6).unwrap_err();
        assert!(matches!(
            e,
            DbError::Format(FormatError::InvalidTxnPayload { .. })
        ));
    }

    #[test]
    fn load_v6_rejects_commit_id_mismatch() {
        let mut store = VecStore::new();
        let mut w = SegmentWriter::new(&mut store, 0);
        let begin = encode_txn_payload_v0(1);
        let commit = encode_txn_payload_v0(2);
        w.append(
            SegmentHeader {
                segment_type: SegmentType::TxnBegin,
                payload_len: 0,
                payload_crc32c: 0,
            },
            begin.as_slice(),
        )
        .unwrap();
        w.append(
            SegmentHeader {
                segment_type: SegmentType::TxnCommit,
                payload_len: 0,
                payload_crc32c: 0,
            },
            commit.as_slice(),
        )
        .unwrap();

        let e = load_catalog_latest_and_indexes(&mut store, 0, FORMAT_MINOR_V6).unwrap_err();
        assert!(matches!(
            e,
            DbError::Format(FormatError::InvalidTxnPayload { .. })
        ));
    }

    #[test]
    fn load_v6_errors_on_unclosed_txn_at_end() {
        let mut store = VecStore::new();
        let mut w = SegmentWriter::new(&mut store, 0);
        let begin = encode_txn_payload_v0(7);
        w.append(
            SegmentHeader {
                segment_type: SegmentType::TxnBegin,
                payload_len: 0,
                payload_crc32c: 0,
            },
            begin.as_slice(),
        )
        .unwrap();

        let e = load_catalog_latest_and_indexes(&mut store, 0, FORMAT_MINOR_V6).unwrap_err();
        assert!(matches!(
            e,
            DbError::Format(FormatError::InvalidTxnPayload { .. })
        ));
    }

    #[test]
    fn load_v6_applies_committed_schema_index_and_record_delete() {
        let mut store = VecStore::new();
        let mut w = SegmentWriter::new(&mut store, 0);

        let begin = encode_txn_payload_v0(1);
        let commit = encode_txn_payload_v0(1);
        w.append(
            SegmentHeader {
                segment_type: SegmentType::TxnBegin,
                payload_len: 0,
                payload_crc32c: 0,
            },
            begin.as_slice(),
        )
        .unwrap();

        let fields = vec![FieldDef {
            path: FieldPath(vec![Cow::Owned("id".to_string())]),
            ty: Type::Int64,
            constraints: vec![],
        }];
        let schema_bytes = encode_catalog_payload(&CatalogRecordWire::CreateCollection {
            collection_id: 1,
            name: "t".to_string(),
            schema_version: 1,
            fields: fields.clone(),
            indexes: vec![],
            primary_field: Some("id".to_string()),
        });
        w.append(
            SegmentHeader {
                segment_type: SegmentType::Schema,
                payload_len: 0,
                payload_crc32c: 0,
            },
            &schema_bytes,
        )
        .unwrap();

        let idx_bytes = encode_index_payload(&[IndexEntry {
            collection_id: 1,
            index_name: "i".to_string(),
            kind: IndexKind::NonUnique,
            op: IndexOp::Insert,
            index_key: b"k".to_vec(),
            pk_key: b"p".to_vec(),
        }]);
        w.append(
            SegmentHeader {
                segment_type: SegmentType::Index,
                payload_len: 0,
                payload_crc32c: 0,
            },
            &idx_bytes,
        )
        .unwrap();

        // Insert then delete the same PK; latest should end empty.
        let ins = encode_record_payload_v3(1, 1, &crate::ScalarValue::Int64(7), &Type::Int64, &[])
            .unwrap();
        w.append(
            SegmentHeader {
                segment_type: SegmentType::Record,
                payload_len: 0,
                payload_crc32c: 0,
            },
            &ins,
        )
        .unwrap();
        let del = encode_record_payload_v3_op(
            1,
            1,
            OP_DELETE,
            &crate::ScalarValue::Int64(7),
            &Type::Int64,
            &[],
        )
        .unwrap();
        w.append(
            SegmentHeader {
                segment_type: SegmentType::Record,
                payload_len: 0,
                payload_crc32c: 0,
            },
            &del,
        )
        .unwrap();

        w.append(
            SegmentHeader {
                segment_type: SegmentType::TxnCommit,
                payload_len: 0,
                payload_crc32c: 0,
            },
            commit.as_slice(),
        )
        .unwrap();

        let (catalog, latest, idx) = load_catalog_latest_and_indexes(&mut store, 0, FORMAT_MINOR_V6)
            .unwrap();
        assert!(catalog.get(crate::schema::CollectionId(1)).is_some());
        assert!(latest.is_empty());
        assert!(idx
            .non_unique_lookup(1, "i", b"k")
            .is_some());
    }

    #[test]
    fn replay_tail_v6_applies_committed_segments() {
        let mut store = VecStore::new();
        let mut w = SegmentWriter::new(&mut store, 0);

        let begin = encode_txn_payload_v0(1);
        let commit = encode_txn_payload_v0(1);
        w.append(
            SegmentHeader {
                segment_type: SegmentType::TxnBegin,
                payload_len: 0,
                payload_crc32c: 0,
            },
            begin.as_slice(),
        )
        .unwrap();

        let fields = vec![FieldDef {
            path: FieldPath(vec![Cow::Owned("id".to_string())]),
            ty: Type::Int64,
            constraints: vec![],
        }];
        let schema_bytes = encode_catalog_payload(&CatalogRecordWire::CreateCollection {
            collection_id: 1,
            name: "t".to_string(),
            schema_version: 1,
            fields: fields.clone(),
            indexes: vec![],
            primary_field: Some("id".to_string()),
        });
        w.append(
            SegmentHeader {
                segment_type: SegmentType::Schema,
                payload_len: 0,
                payload_crc32c: 0,
            },
            &schema_bytes,
        )
        .unwrap();

        let idx_bytes = encode_index_payload(&[IndexEntry {
            collection_id: 1,
            index_name: "i".to_string(),
            kind: IndexKind::NonUnique,
            op: IndexOp::Insert,
            index_key: b"k".to_vec(),
            pk_key: b"p".to_vec(),
        }]);
        w.append(
            SegmentHeader {
                segment_type: SegmentType::Index,
                payload_len: 0,
                payload_crc32c: 0,
            },
            &idx_bytes,
        )
        .unwrap();

        let rec = encode_record_payload_v3(1, 1, &crate::ScalarValue::Int64(7), &Type::Int64, &[])
            .unwrap();
        w.append(
            SegmentHeader {
                segment_type: SegmentType::Record,
                payload_len: 0,
                payload_crc32c: 0,
            },
            &rec,
        )
        .unwrap();

        w.append(
            SegmentHeader {
                segment_type: SegmentType::TxnCommit,
                payload_len: 0,
                payload_crc32c: 0,
            },
            commit.as_slice(),
        )
        .unwrap();

        let mut catalog = Catalog::default();
        let mut latest = HashMap::new();
        let mut idx = IndexState::default();
        replay_tail_into(&mut store, 0, FORMAT_MINOR_V6, &mut catalog, &mut latest, &mut idx)
            .unwrap();

        assert!(catalog.get(crate::schema::CollectionId(1)).is_some());
        let key = crate::ScalarValue::Int64(7).canonical_key_bytes();
        assert!(latest.get(&(1u32, key)).is_some());
        assert!(idx.non_unique_lookup(1, "i", b"k").is_some());
    }

    #[test]
    fn replay_tail_v6_skips_records_when_catalog_has_no_primary_key() {
        let mut store = VecStore::new();
        let mut w = SegmentWriter::new(&mut store, 0);

        let begin = encode_txn_payload_v0(1);
        let commit = encode_txn_payload_v0(1);
        w.append(
            SegmentHeader {
                segment_type: SegmentType::TxnBegin,
                payload_len: 0,
                payload_crc32c: 0,
            },
            begin.as_slice(),
        )
        .unwrap();

        let fields = vec![FieldDef {
            path: FieldPath(vec![Cow::Owned("id".to_string())]),
            ty: Type::Int64,
            constraints: vec![],
        }];
        let schema_bytes = encode_catalog_payload(&CatalogRecordWire::CreateCollection {
            collection_id: 1,
            name: "t".to_string(),
            schema_version: 1,
            fields,
            indexes: vec![],
            primary_field: None,
        });
        w.append(
            SegmentHeader {
                segment_type: SegmentType::Schema,
                payload_len: 0,
                payload_crc32c: 0,
            },
            &schema_bytes,
        )
        .unwrap();

        let rec = encode_record_payload_v3(1, 1, &crate::ScalarValue::Int64(7), &Type::Int64, &[])
            .unwrap();
        w.append(
            SegmentHeader {
                segment_type: SegmentType::Record,
                payload_len: 0,
                payload_crc32c: 0,
            },
            &rec,
        )
        .unwrap();

        w.append(
            SegmentHeader {
                segment_type: SegmentType::TxnCommit,
                payload_len: 0,
                payload_crc32c: 0,
            },
            commit.as_slice(),
        )
        .unwrap();

        let mut catalog = Catalog::default();
        let mut latest = HashMap::new();
        let mut idx = IndexState::default();
        replay_tail_into(&mut store, 0, FORMAT_MINOR_V6, &mut catalog, &mut latest, &mut idx)
            .unwrap();
        assert!(latest.is_empty());
    }

    #[test]
    fn replay_tail_v6_applies_committed_segments_with_filestore() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("replay_v6.typra");
        let f = FsOpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        let mut store = FileStore::new(f);

        let mut w = SegmentWriter::new(&mut store, 0);
        let begin = encode_txn_payload_v0(1);
        let commit = encode_txn_payload_v0(1);
        w.append(
            SegmentHeader {
                segment_type: SegmentType::TxnBegin,
                payload_len: 0,
                payload_crc32c: 0,
            },
            begin.as_slice(),
        )
        .unwrap();

        let schema_bytes = encode_catalog_payload(&CatalogRecordWire::CreateCollection {
            collection_id: 1,
            name: "t".to_string(),
            schema_version: 1,
            fields: vec![FieldDef {
                path: FieldPath(vec![Cow::Owned("id".to_string())]),
                ty: Type::Int64,
                constraints: vec![],
            }],
            indexes: vec![],
            primary_field: Some("id".to_string()),
        });
        w.append(
            SegmentHeader {
                segment_type: SegmentType::Schema,
                payload_len: 0,
                payload_crc32c: 0,
            },
            &schema_bytes,
        )
        .unwrap();

        let rec = encode_record_payload_v3(1, 1, &crate::ScalarValue::Int64(7), &Type::Int64, &[])
            .unwrap();
        w.append(
            SegmentHeader {
                segment_type: SegmentType::Record,
                payload_len: 0,
                payload_crc32c: 0,
            },
            &rec,
        )
        .unwrap();

        w.append(
            SegmentHeader {
                segment_type: SegmentType::TxnCommit,
                payload_len: 0,
                payload_crc32c: 0,
            },
            commit.as_slice(),
        )
        .unwrap();

        let mut catalog = Catalog::default();
        let mut latest = HashMap::new();
        let mut idx = IndexState::default();
        replay_tail_into(&mut store, 0, FORMAT_MINOR_V6, &mut catalog, &mut latest, &mut idx)
            .unwrap();
        let key = crate::ScalarValue::Int64(7).canonical_key_bytes();
        assert!(latest.get(&(1u32, key)).is_some());
    }

    #[test]
    fn apply_record_segment_pk_scan_skips_non_matching_field_first() {
        // Exercise the `for f in &col.fields { if ... }` loop both false and true branches.
        let mut store = VecStore::new();
        let mut w = SegmentWriter::new(&mut store, 0);

        let begin = encode_txn_payload_v0(1);
        let commit = encode_txn_payload_v0(1);
        w.append(
            SegmentHeader {
                segment_type: SegmentType::TxnBegin,
                payload_len: 0,
                payload_crc32c: 0,
            },
            begin.as_slice(),
        )
        .unwrap();

        let x_def = FieldDef {
            path: FieldPath(vec![Cow::Owned("x".to_string())]),
            ty: Type::Int64,
            constraints: vec![],
        };
        let id_def = FieldDef {
            path: FieldPath(vec![Cow::Owned("id".to_string())]),
            ty: Type::Int64,
            constraints: vec![],
        };
        let fields = vec![x_def.clone(), id_def];
        let schema_bytes = encode_catalog_payload(&CatalogRecordWire::CreateCollection {
            collection_id: 1,
            name: "t".to_string(),
            schema_version: 1,
            fields,
            indexes: vec![],
            primary_field: Some("id".to_string()),
        });
        w.append(
            SegmentHeader {
                segment_type: SegmentType::Schema,
                payload_len: 0,
                payload_crc32c: 0,
            },
            &schema_bytes,
        )
        .unwrap();

        // Record payload must include all non-PK fields in schema order (here: "x").
        let rec = encode_record_payload_v3(
            1,
            1,
            &crate::ScalarValue::Int64(7),
            &Type::Int64,
            &[(x_def, RowValue::Int64(1))],
        )
        .unwrap();
        w.append(
            SegmentHeader {
                segment_type: SegmentType::Record,
                payload_len: 0,
                payload_crc32c: 0,
            },
            &rec,
        )
        .unwrap();

        w.append(
            SegmentHeader {
                segment_type: SegmentType::TxnCommit,
                payload_len: 0,
                payload_crc32c: 0,
            },
            commit.as_slice(),
        )
        .unwrap();

        let mut catalog = Catalog::default();
        let mut latest = HashMap::new();
        let mut idx = IndexState::default();
        replay_tail_into(&mut store, 0, FORMAT_MINOR_V6, &mut catalog, &mut latest, &mut idx)
            .unwrap();
        let key = crate::ScalarValue::Int64(7).canonical_key_bytes();
        assert!(latest.get(&(1u32, key)).is_some());
    }
}
