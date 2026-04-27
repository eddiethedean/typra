//! Checkpoint payloads: persisted logical state snapshots to accelerate open/replay.

use std::collections::{BTreeMap, HashMap};

use crate::catalog::{encode_catalog_payload, Catalog, CatalogRecordWire};
use crate::error::{DbError, FormatError, SchemaError};
use crate::index::{decode_index_payload, encode_index_payload, IndexEntry, IndexState};
use crate::record::{encode_record_payload_v2, RowValue, ScalarValue};
use crate::schema::CollectionId;

use crate::db::LatestMap;

pub const CHECKPOINT_VERSION_V0: u16 = 0;

#[derive(Debug, Clone)]
pub struct CheckpointV0 {
    pub replay_from_offset: u64,
    pub catalog_records: Vec<CatalogRecordWire>,
    pub record_payloads: Vec<Vec<u8>>,
    pub index_entries: Vec<IndexEntry>,
}

pub fn encode_checkpoint_payload_v0(cp: &CheckpointV0) -> Vec<u8> {
    // NOTE: caller is responsible for setting replay_from_offset (we may patch it later).
    let mut out = Vec::new();
    out.extend_from_slice(&CHECKPOINT_VERSION_V0.to_le_bytes());
    out.extend_from_slice(&cp.replay_from_offset.to_le_bytes());

    out.extend_from_slice(&(cp.catalog_records.len() as u32).to_le_bytes());
    for r in &cp.catalog_records {
        let b = encode_catalog_payload(r);
        out.extend_from_slice(&(b.len() as u32).to_le_bytes());
        out.extend_from_slice(b.as_slice());
    }

    out.extend_from_slice(&(cp.record_payloads.len() as u32).to_le_bytes());
    for b in &cp.record_payloads {
        out.extend_from_slice(&(b.len() as u32).to_le_bytes());
        out.extend_from_slice(b.as_slice());
    }

    let idx_blob = encode_index_payload(&cp.index_entries);
    out.extend_from_slice(&(idx_blob.len() as u32).to_le_bytes());
    out.extend_from_slice(&idx_blob);

    out
}

pub fn decode_checkpoint_payload(bytes: &[u8]) -> Result<CheckpointV0, DbError> {
    let mut cur = Cursor::new(bytes);
    let ver = cur.take_u16()?;
    if ver != CHECKPOINT_VERSION_V0 {
        return Err(DbError::Format(FormatError::UnsupportedVersion {
            major: 0,
            minor: ver,
        }));
    }
    let replay_from_offset = cur.take_u64()?;

    let n_catalog = cur.take_u32()? as usize;
    let mut catalog_records = Vec::with_capacity(n_catalog.min(1024));
    for _ in 0..n_catalog {
        let n = cur.take_u32()? as usize;
        let b = cur.take_bytes(n)?;
        let rec = crate::catalog::decode_catalog_payload(&b)?;
        catalog_records.push(rec);
    }

    let n_records = cur.take_u32()? as usize;
    let mut record_payloads = Vec::with_capacity(n_records.min(1024));
    for _ in 0..n_records {
        let n = cur.take_u32()? as usize;
        record_payloads.push(cur.take_bytes(n)?);
    }

    let idx_blob_len = cur.take_u32()? as usize;
    let idx_blob = cur.take_bytes(idx_blob_len)?;
    let index_entries = decode_index_payload(&idx_blob)?;

    if cur.remaining() != 0 {
        return Err(DbError::Format(FormatError::InvalidCatalogPayload {
            message: "trailing bytes in checkpoint payload".to_string(),
        }));
    }

    Ok(CheckpointV0 {
        replay_from_offset,
        catalog_records,
        record_payloads,
        index_entries,
    })
}

/// Build a checkpoint representation from current in-memory engine state.
///
/// This encodes the *current* catalog/schema only (Typra validates record segments against the
/// current schema version during replay).
pub fn checkpoint_from_state(
    catalog: &Catalog,
    latest: &LatestMap,
    indexes: &IndexState,
) -> Result<CheckpointV0, DbError> {
    let mut catalog_records: Vec<CatalogRecordWire> = Vec::new();
    let mut cols = catalog.collections();
    cols.sort_by_key(|c| c.id.0);
    for c in &cols {
        let pk = c
            .primary_field
            .as_deref()
            .ok_or(DbError::Schema(SchemaError::NoPrimaryKey {
                collection_id: c.id.0,
            }))?;
        catalog_records.push(CatalogRecordWire::CreateCollection {
            collection_id: c.id.0,
            name: c.name.clone(),
            schema_version: 1,
            fields: c.fields.clone(),
            indexes: c.indexes.clone(),
            primary_field: Some(pk.to_string()),
        });
        for v in 2..=c.current_version.0 {
            catalog_records.push(CatalogRecordWire::NewSchemaVersion {
                collection_id: c.id.0,
                schema_version: v,
                fields: c.fields.clone(),
                indexes: c.indexes.clone(),
            });
        }
    }

    // Encode latest rows as v2 record payloads (insert op semantics).
    let mut record_payloads: Vec<Vec<u8>> = Vec::with_capacity(latest.len().min(1_000_000));
    for ((cid, _pk_key), row) in latest.iter() {
        let col = catalog
            .get(CollectionId(*cid))
            .ok_or(DbError::Schema(SchemaError::UnknownCollection { id: *cid }))?;
        let pk_name =
            col.primary_field
                .as_deref()
                .ok_or(DbError::Schema(SchemaError::NoPrimaryKey {
                    collection_id: col.id.0,
                }))?;
        let mut pk_def = None;
        let mut non_pk_defs: Vec<crate::schema::FieldDef> = Vec::new();
        for f in &col.fields {
            if f.path.0.len() != 1 {
                continue;
            }
            if f.path.0[0] == pk_name {
                pk_def = Some(f.clone());
            } else {
                non_pk_defs.push(f.clone());
            }
        }
        let pk_def = pk_def.ok_or(DbError::Schema(SchemaError::PrimaryFieldNotFound {
            name: pk_name.to_string(),
        }))?;
        let pk_cell = row
            .get(pk_name)
            .ok_or(DbError::Schema(SchemaError::RowMissingPrimary {
                name: pk_name.to_string(),
            }))?;
        let pk_scalar: ScalarValue = pk_cell.clone().into_scalar()?;

        let mut ordered: Vec<(crate::schema::FieldDef, RowValue)> = Vec::new();
        for def in non_pk_defs {
            let key = def.path.0[0].as_ref();
            let v = row.get(key).cloned().unwrap_or(RowValue::None);
            ordered.push((def, v));
        }

        record_payloads.push(encode_record_payload_v2(
            *cid,
            col.current_version.0,
            &pk_scalar,
            &pk_def.ty,
            &ordered,
        )?);
    }

    let index_entries = indexes.entries_for_checkpoint();

    Ok(CheckpointV0 {
        replay_from_offset: 0,
        catalog_records,
        record_payloads,
        index_entries,
    })
}

/// Decode a checkpoint payload into engine state (catalog/latest/indexes).
pub fn state_from_checkpoint_payload(
    payload: &[u8],
) -> Result<(u64, Catalog, LatestMap, IndexState), DbError> {
    let cp = decode_checkpoint_payload(payload)?;

    let mut catalog = Catalog::default();
    for r in &cp.catalog_records {
        catalog.apply_record(r.clone())?;
    }

    let mut latest: LatestMap = HashMap::new();
    for rec in &cp.record_payloads {
        apply_checkpoint_record_payload(rec, &catalog, &mut latest)?;
    }

    let mut indexes = IndexState::default();
    for e in cp.index_entries {
        indexes.apply(e)?;
    }

    Ok((cp.replay_from_offset, catalog, latest, indexes))
}

fn apply_checkpoint_record_payload(
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
    let pk_name =
        col.primary_field
            .as_deref()
            .ok_or(DbError::Schema(SchemaError::NoPrimaryKey {
                collection_id: col.id.0,
            }))?;
    let pk_ty = col
        .fields
        .iter()
        .filter(|f| f.path.0.len() == 1)
        .find_map(|f| {
            if f.path.0[0] == pk_name {
                Some(&f.ty)
            } else {
                None
            }
        })
        .ok_or(DbError::Schema(SchemaError::PrimaryFieldNotFound {
            name: pk_name.to_string(),
        }))?;

    let decoded = crate::record::decode_record_payload(payload, pk_name, pk_ty, &col.fields)?;
    let pk_key = decoded.pk.canonical_key_bytes();
    let mut full: BTreeMap<String, RowValue> = BTreeMap::new();
    full.insert(pk_name.to_string(), RowValue::from_scalar(decoded.pk));
    for (k, v) in decoded.fields {
        full.insert(k, v);
    }
    latest.insert((collection_id, pk_key), full);
    Ok(())
}

struct Cursor<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.pos)
    }

    fn take_u16(&mut self) -> Result<u16, DbError> {
        if self.remaining() < 2 {
            return Err(DbError::Format(FormatError::InvalidCatalogPayload {
                message: "unexpected eof".to_string(),
            }));
        }
        let v = u16::from_le_bytes([self.bytes[self.pos], self.bytes[self.pos + 1]]);
        self.pos += 2;
        Ok(v)
    }

    fn take_u32(&mut self) -> Result<u32, DbError> {
        if self.remaining() < 4 {
            return Err(DbError::Format(FormatError::InvalidCatalogPayload {
                message: "unexpected eof".to_string(),
            }));
        }
        let v = u32::from_le_bytes([
            self.bytes[self.pos],
            self.bytes[self.pos + 1],
            self.bytes[self.pos + 2],
            self.bytes[self.pos + 3],
        ]);
        self.pos += 4;
        Ok(v)
    }

    fn take_u64(&mut self) -> Result<u64, DbError> {
        if self.remaining() < 8 {
            return Err(DbError::Format(FormatError::InvalidCatalogPayload {
                message: "unexpected eof".to_string(),
            }));
        }
        let v = u64::from_le_bytes(self.bytes[self.pos..self.pos + 8].try_into().unwrap());
        self.pos += 8;
        Ok(v)
    }

    fn take_bytes(&mut self, n: usize) -> Result<Vec<u8>, DbError> {
        if self.remaining() < n {
            return Err(DbError::Format(FormatError::InvalidCatalogPayload {
                message: "unexpected eof".to_string(),
            }));
        }
        let slice = &self.bytes[self.pos..self.pos + n];
        self.pos += n;
        Ok(slice.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use std::collections::BTreeMap;

    use super::{checkpoint_from_state, decode_checkpoint_payload, CheckpointV0};
    use crate::catalog::{Catalog, CatalogRecordWire};
    use crate::index::IndexState;
    use crate::record::{RowValue, ScalarValue};
    use crate::schema::{FieldDef, FieldPath, Type};

    fn field(name: &str, ty: Type) -> FieldDef {
        FieldDef {
            path: FieldPath(vec![Cow::Owned(name.to_string())]),
            ty,
            constraints: vec![],
        }
    }

    #[test]
    fn checkpoint_from_state_emits_new_schema_versions_when_current_version_gt_1() {
        let mut catalog = Catalog::default();
        catalog
            .apply_record(CatalogRecordWire::CreateCollection {
                collection_id: 1,
                name: "books".to_string(),
                schema_version: 1,
                // Put a non-PK first so PK lookup must scan at least one non-match.
                // Also include a multi-segment field path to exercise the "skip non-top-level" path.
                fields: vec![
                    field("title", Type::String),
                    field("id", Type::Int64),
                    FieldDef {
                        path: FieldPath(vec![
                            Cow::Owned("meta".to_string()),
                            Cow::Owned("x".to_string()),
                        ]),
                        ty: Type::String,
                        constraints: vec![],
                    },
                ],
                indexes: vec![],
                primary_field: Some("id".to_string()),
            })
            .unwrap();
        catalog
            .apply_record(CatalogRecordWire::NewSchemaVersion {
                collection_id: 1,
                schema_version: 2,
                fields: vec![
                    field("title", Type::String),
                    field("id", Type::Int64),
                    FieldDef {
                        path: FieldPath(vec![
                            Cow::Owned("meta".to_string()),
                            Cow::Owned("x".to_string()),
                        ]),
                        ty: Type::String,
                        constraints: vec![],
                    },
                ],
                indexes: vec![],
            })
            .unwrap();

        let latest = crate::db::LatestMap::new();
        let indexes = IndexState::default();

        let cp = checkpoint_from_state(&catalog, &latest, &indexes).unwrap();
        // CreateCollection + 1x NewSchemaVersion
        assert_eq!(cp.catalog_records.len(), 2);
        assert!(matches!(
            cp.catalog_records[0],
            CatalogRecordWire::CreateCollection { .. }
        ));
        assert!(matches!(
            cp.catalog_records[1],
            CatalogRecordWire::NewSchemaVersion { schema_version: 2, .. }
        ));
    }

    #[test]
    fn decode_checkpoint_payload_rejects_truncated_before_replay_offset() {
        // version only, missing replay_from_offset u64.
        let bytes = super::CHECKPOINT_VERSION_V0.to_le_bytes().to_vec();
        assert!(decode_checkpoint_payload(&bytes).is_err());
    }

    #[test]
    fn decode_checkpoint_payload_rejects_empty_bytes() {
        assert!(decode_checkpoint_payload(&[]).is_err());
    }

    #[test]
    fn decode_checkpoint_payload_rejects_truncated_before_catalog_count_u32() {
        // version + replay_from_offset, missing n_catalog u32.
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&super::CHECKPOINT_VERSION_V0.to_le_bytes());
        bytes.extend_from_slice(&0u64.to_le_bytes());
        assert!(decode_checkpoint_payload(&bytes).is_err());
    }

    #[test]
    fn decode_checkpoint_payload_rejects_truncated_mid_catalog_record_bytes() {
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&super::CHECKPOINT_VERSION_V0.to_le_bytes());
        bytes.extend_from_slice(&0u64.to_le_bytes());
        bytes.extend_from_slice(&1u32.to_le_bytes()); // 1 catalog record
        bytes.extend_from_slice(&10u32.to_le_bytes()); // claims 10 bytes
        bytes.extend_from_slice(&[0u8; 3]); // too short
        assert!(decode_checkpoint_payload(&bytes).is_err());
    }

    #[test]
    fn decode_checkpoint_payload_rejects_truncated_mid_index_blob_bytes() {
        // Valid empty catalog/records, but truncated index blob bytes.
        let cp = CheckpointV0 {
            replay_from_offset: 0,
            catalog_records: vec![],
            record_payloads: vec![],
            index_entries: vec![],
        };
        let mut bytes = super::encode_checkpoint_payload_v0(&cp);
        // Append an index blob length then truncate so take_bytes fails.
        bytes.extend_from_slice(&4u32.to_le_bytes());
        bytes.extend_from_slice(&[1u8, 2u8]); // only 2/4 bytes present
        assert!(decode_checkpoint_payload(&bytes).is_err());
    }

    #[test]
    fn state_from_checkpoint_payload_rejects_truncated_record_payload() {
        let cp = CheckpointV0 {
            replay_from_offset: 0,
            catalog_records: vec![],
            record_payloads: vec![vec![0u8; 5]], // <6 bytes triggers TruncatedRecordPayload
            index_entries: vec![],
        };
        let payload = super::encode_checkpoint_payload_v0(&cp);
        assert!(super::state_from_checkpoint_payload(&payload).is_err());
    }

    #[test]
    fn state_from_checkpoint_payload_rejects_unknown_collection_in_record_payload() {
        let cp = CheckpointV0 {
            replay_from_offset: 0,
            catalog_records: vec![],
            // 6+ bytes so it passes the length check; bytes[2..6] encode collection_id.
            record_payloads: vec![vec![0u8, 0u8, 99u8, 0u8, 0u8, 0u8]],
            index_entries: vec![],
        };
        let payload = super::encode_checkpoint_payload_v0(&cp);
        assert!(super::state_from_checkpoint_payload(&payload).is_err());
    }

    #[test]
    fn state_from_checkpoint_payload_roundtrips_one_row_and_exercises_field_scans() {
        let mut catalog = Catalog::default();
        catalog
            .apply_record(CatalogRecordWire::CreateCollection {
                collection_id: 1,
                name: "books".to_string(),
                schema_version: 1,
                // Non-PK first; includes multi-segment field to exercise skip path.
                fields: vec![
                    field("title", Type::String),
                    field("id", Type::Int64),
                    FieldDef {
                        path: FieldPath(vec![
                            Cow::Owned("meta".to_string()),
                            Cow::Owned("x".to_string()),
                        ]),
                        ty: Type::String,
                        constraints: vec![],
                    },
                ],
                indexes: vec![],
                primary_field: Some("id".to_string()),
            })
            .unwrap();

        let mut row = BTreeMap::new();
        row.insert("id".to_string(), RowValue::Int64(1));
        row.insert("title".to_string(), RowValue::String("a".to_string()));
        // meta.x is omitted; not top-level and should not affect encoding.

        let pk_key = ScalarValue::Int64(1).canonical_key_bytes();
        let mut latest = crate::db::LatestMap::new();
        latest.insert((1u32, pk_key.clone()), row);

        let indexes = IndexState::default();
        let cp = checkpoint_from_state(&catalog, &latest, &indexes).unwrap();
        let payload = super::encode_checkpoint_payload_v0(&cp);

        let (_off, _cat2, latest2, _idx2) = super::state_from_checkpoint_payload(&payload).unwrap();
        assert!(latest2.contains_key(&(1u32, pk_key)));
    }
}
