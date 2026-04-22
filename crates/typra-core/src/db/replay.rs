//! Replay schema and record segments into [`crate::catalog::Catalog`] and in-memory latest-row maps.
//!
//! Legacy catalog entries may omit `primary_field`; record segments for those collections are
//! skipped during replay (insert was never supported without a primary key).

use std::collections::{BTreeMap, HashMap};

use crate::catalog::{decode_catalog_payload, Catalog};
use crate::error::{DbError, FormatError, SchemaError};
use crate::record::{decode_record_payload, RowValue};
use crate::schema::CollectionId;
use crate::segments::header::SegmentType;
use crate::segments::reader::{read_segment_payload, scan_segments};
use crate::storage::Store;

use super::LatestMap;

/// One [`scan_segments`] pass, then apply schema segments to build `Catalog`, then replay record
/// segments in order using the final catalog (same semantics as two separate full scans).
pub(crate) fn load_catalog_and_latest_rows<S: Store>(
    store: &mut S,
    segment_start: u64,
) -> Result<(Catalog, LatestMap), DbError> {
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
    for meta in &metas {
        if meta.header.segment_type != SegmentType::Record {
            continue;
        }
        let payload = read_segment_payload(store, meta)?;
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
            None => continue,
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
        let decoded = decode_record_payload(&payload, pk_name, pk_ty, &col.fields)?;
        if decoded.schema_version != col.current_version.0 {
            return Err(DbError::Schema(SchemaError::InvalidSchemaVersion {
                expected: col.current_version.0,
                got: decoded.schema_version,
            }));
        }
        let mut full: BTreeMap<String, RowValue> = BTreeMap::new();
        full.insert(
            pk_name.to_string(),
            RowValue::from_scalar(decoded.pk.clone()),
        );
        for (k, v) in decoded.fields {
            full.insert(k, v);
        }
        latest.insert((collection_id, decoded.pk.canonical_key_bytes()), full);
    }
    Ok((catalog, latest))
}
