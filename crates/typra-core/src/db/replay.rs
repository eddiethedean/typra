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
            SegmentType::Schema | SegmentType::Index | SegmentType::Record => {
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
    for f in &col.fields {
        if f.path.0.len() != 1 {
            return Err(DbError::NotImplemented);
        }
    }
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
