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
        let pk_def = col
            .fields
            .iter()
            .find(|f| f.path.0.len() == 1 && f.path.0[0] == pk_name)
            .ok_or(DbError::Schema(SchemaError::PrimaryFieldNotFound {
                name: pk_name.to_string(),
            }))?;
        let pk_cell = row
            .get(pk_name)
            .ok_or(DbError::Schema(SchemaError::RowMissingPrimary {
                name: pk_name.to_string(),
            }))?;
        let pk_scalar: ScalarValue = pk_cell.clone().into_scalar()?;

        let non_pk_defs: Vec<_> = col
            .fields
            .iter()
            .filter(|f| f.path.0.len() == 1 && f.path.0[0] != pk_name)
            .cloned()
            .collect();
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
        .find(|f| f.path.0.len() == 1 && f.path.0[0] == pk_name)
        .map(|f| &f.ty)
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
}
