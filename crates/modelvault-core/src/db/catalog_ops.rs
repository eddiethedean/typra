//! Catalog registration, schema migration planning, and index rebuild helpers.

use std::collections::BTreeMap;

use crate::catalog::{encode_catalog_payload, Catalog, CatalogRecordWire};
use crate::error::{DbError, SchemaError};
use crate::index::{encode_index_payload, IndexEntry, IndexOp};
use crate::record::RowValue;
use crate::schema::CollectionId;
use crate::schema::{classify_schema_update, FieldDef, SchemaChange, SchemaVersion};
use crate::storage::Store;

use super::{helpers, row_value_at_path_segments, scalar_at_path, Database};

impl<S: Store> Database<S> {
    /// Register the collection schema defined by `T` (schema version 1).
    pub fn register_model<T: crate::schema::DbModel>(
        &mut self,
    ) -> Result<(CollectionId, SchemaVersion), DbError> {
        self.register_collection_with_indexes(
            T::collection_name(),
            T::fields(),
            T::indexes(),
            T::primary_field(),
        )
    }

    /// Create a new collection at schema version `1`.
    ///
    /// `primary_field` must name a **single-segment** (top-level) field present in `fields`.
    /// Appends a catalog segment and updates the in-memory catalog.
    pub fn register_collection(
        &mut self,
        name: &str,
        fields: Vec<FieldDef>,
        primary_field: &str,
    ) -> Result<(CollectionId, SchemaVersion), DbError> {
        self.register_collection_with_indexes(name, fields, vec![], primary_field)
    }

    pub fn register_collection_with_indexes(
        &mut self,
        name: &str,
        fields: Vec<FieldDef>,
        indexes: Vec<crate::schema::IndexDef>,
        primary_field: &str,
    ) -> Result<(CollectionId, SchemaVersion), DbError> {
        let name = helpers::normalize_collection_name(name)?;
        let pk = primary_field.trim();
        if pk.is_empty() {
            return Err(DbError::Schema(SchemaError::InvalidCollectionName));
        }
        if !Catalog::has_top_level_field(&fields, pk) {
            return Err(DbError::Schema(SchemaError::PrimaryFieldNotFound {
                name: pk.to_string(),
            }));
        }
        if let Some(st) = &mut self.txn_staging {
            let id = st.shadow_catalog.next_collection_id().0;
            let wire = CatalogRecordWire::CreateCollection {
                collection_id: id,
                name: name.clone(),
                schema_version: 1,
                fields,
                indexes,
                primary_field: Some(pk.to_string()),
            };
            let payload = encode_catalog_payload(&wire);
            st.shadow_catalog.apply_record(wire)?;
            st.pending
                .push((crate::segments::header::SegmentType::Schema, payload));
            return Ok((CollectionId(id), SchemaVersion(1)));
        }
        let id = self.catalog.next_collection_id().0;
        let wire = CatalogRecordWire::CreateCollection {
            collection_id: id,
            name: name.clone(),
            schema_version: 1,
            fields,
            indexes,
            primary_field: Some(pk.to_string()),
        };
        let payload = encode_catalog_payload(&wire);
        let tid = self.next_txn_id();
        self.commit_write_batch(
            tid,
            &[(
                crate::segments::header::SegmentType::Schema,
                payload.as_slice(),
            )],
        )?;
        self.apply_catalog_record(wire)?;
        self.push_shared_mirror();
        Ok((CollectionId(id), SchemaVersion(1)))
    }

    /// Bump the schema version for `id` to `current + 1` with a new field set.
    ///
    /// The primary-key field must remain present as a top-level field (see catalog rules).
    pub fn register_schema_version(
        &mut self,
        id: CollectionId,
        fields: Vec<FieldDef>,
    ) -> Result<SchemaVersion, DbError> {
        self.register_schema_version_with_indexes(id, fields, vec![])
    }

    pub fn register_schema_version_with_indexes(
        &mut self,
        id: CollectionId,
        fields: Vec<FieldDef>,
        indexes: Vec<crate::schema::IndexDef>,
    ) -> Result<SchemaVersion, DbError> {
        let current = self
            .catalog_for_read()
            .get(id)
            .ok_or(DbError::Schema(SchemaError::UnknownCollection { id: id.0 }))?;
        // `classify_schema_update` only returns `Ok(...)` variants today; keep it infallible here.
        match classify_schema_update(&current.fields, &current.indexes, &fields, &indexes)? {
            SchemaChange::Safe => {}
            SchemaChange::NeedsMigration { reason, .. } => {
                return Err(DbError::Schema(SchemaError::MigrationRequired {
                    message: reason,
                }));
            }
            SchemaChange::Breaking { reason } => {
                return Err(DbError::Schema(SchemaError::IncompatibleSchemaChange {
                    message: reason,
                }));
            }
        }
        let next_v = current
            .current_version
            .0
            .checked_add(1)
            .ok_or(DbError::Schema(SchemaError::SchemaVersionExhausted))?;
        let wire = CatalogRecordWire::NewSchemaVersion {
            collection_id: id.0,
            schema_version: next_v,
            fields,
            indexes,
        };
        let payload = encode_catalog_payload(&wire);
        if let Some(st) = &mut self.txn_staging {
            st.shadow_catalog.apply_record(wire.clone())?;
            st.pending
                .push((crate::segments::header::SegmentType::Schema, payload));
            self.rewrite_collection_rows_at_current_version(id)?;
            return Ok(SchemaVersion(next_v));
        }
        self.begin_transaction()?;
        if let Some(st) = &mut self.txn_staging {
            st.shadow_catalog.apply_record(wire.clone())?;
            st.pending
                .push((crate::segments::header::SegmentType::Schema, payload));
        }
        self.rewrite_collection_rows_at_current_version(id)?;
        self.commit_transaction()?;
        Ok(SchemaVersion(next_v))
    }

    /// Plan a schema version bump and return the required migration steps, if any.
    pub fn plan_schema_version_with_indexes(
        &self,
        id: CollectionId,
        fields: Vec<FieldDef>,
        indexes: Vec<crate::schema::IndexDef>,
    ) -> Result<crate::MigrationPlan, DbError> {
        let current = self
            .catalog_for_read()
            .get(id)
            .ok_or(DbError::Schema(SchemaError::UnknownCollection { id: id.0 }))?;
        // Same infallibility contract as `register_schema_version_with_indexes` above.
        let change = classify_schema_update(&current.fields, &current.indexes, &fields, &indexes)?;
        let mut steps = Vec::new();
        match &change {
            SchemaChange::Safe => {}
            SchemaChange::Breaking { .. } => {}
            SchemaChange::NeedsMigration {
                reason,
                backfill_top_level_field,
                backfill_field_path,
            } => {
                if let Some(field) = backfill_top_level_field {
                    steps.push(crate::MigrationStep::BackfillTopLevelField {
                        field: field.clone(),
                    });
                } else if let Some(path) = backfill_field_path {
                    steps.push(crate::MigrationStep::BackfillFieldAtPath { path: path.clone() });
                } else if reason.contains("unique index") {
                    steps.push(crate::MigrationStep::RebuildIndexes);
                }
            }
        }
        Ok(crate::MigrationPlan { change, steps })
    }

    /// Backfill a missing top-level field with a fixed value for all rows in a collection.
    ///
    /// This helper is intentionally simple so it can be bound to other languages.
    pub fn backfill_top_level_field_with_value(
        &mut self,
        collection_id: CollectionId,
        field: &str,
        value: RowValue,
    ) -> Result<(), DbError> {
        let path = crate::schema::FieldPath(vec![std::borrow::Cow::Owned(field.to_string())]);
        self.backfill_field_at_path_with_value(collection_id, &path, value)
    }

    /// Backfill a missing field (any segment path) with a fixed value for all rows.
    pub fn backfill_field_at_path_with_value(
        &mut self,
        collection_id: CollectionId,
        path: &crate::schema::FieldPath,
        value: RowValue,
    ) -> Result<(), DbError> {
        let col = self
            .catalog_for_read()
            .get(collection_id)
            .ok_or(DbError::Schema(SchemaError::UnknownCollection {
                id: collection_id.0,
            }))?;
        let _field_def = col.fields.iter().find(|f| f.path == *path).ok_or_else(|| {
            DbError::Schema(SchemaError::RowUnknownField {
                name: path.0.last().map(|s| s.to_string()).unwrap_or_default(),
            })
        })?;

        let mut rows: Vec<BTreeMap<String, RowValue>> = Vec::new();
        for ((cid, _), row) in self.latest_for_read().iter() {
            if *cid != collection_id.0 {
                continue;
            }
            rows.push(row.clone());
        }

        self.transaction(|db| {
            for mut row in rows {
                if row_value_at_path_segments(&row, &path.0).is_some() {
                    continue;
                }
                crate::record::insert_value_at_path(&mut row, path, value.clone())?;
                db.insert(collection_id, row)?;
            }
            Ok(())
        })
    }

    /// Rebuild index entries for all rows in `collection_id` using the current schema’s index defs.
    pub fn rebuild_indexes_for_collection(
        &mut self,
        collection_id: CollectionId,
    ) -> Result<(), DbError> {
        let col = self
            .catalog_for_read()
            .get(collection_id)
            .ok_or(DbError::Schema(SchemaError::UnknownCollection {
                id: collection_id.0,
            }))?;
        let pk_name =
            col.primary_field
                .as_deref()
                .ok_or(DbError::Schema(SchemaError::NoPrimaryKey {
                    collection_id: collection_id.0,
                }))?;
        let pk_def = col
            .fields
            .iter()
            .find(|f| f.path.0.len() == 1 && f.path.0[0] == pk_name)
            .ok_or(DbError::Schema(SchemaError::PrimaryFieldNotFound {
                name: pk_name.to_string(),
            }))?;

        let mut entries: Vec<IndexEntry> = Vec::new();
        for e in self.indexes_for_read().entries_for_checkpoint() {
            if e.collection_id != collection_id.0 {
                continue;
            }
            entries.push(IndexEntry {
                collection_id: e.collection_id,
                index_name: e.index_name.clone(),
                kind: e.kind,
                op: IndexOp::Delete,
                index_key: e.index_key.clone(),
                pk_key: e.pk_key.clone(),
            });
        }
        for ((cid, _), row) in self.latest_for_read().iter() {
            if *cid != collection_id.0 {
                continue;
            }
            let Some(pk_cell) = row.get(pk_name) else {
                continue;
            };
            let pk_scalar = pk_cell.clone().into_scalar()?;
            if !pk_scalar.ty_matches(&pk_def.ty) {
                continue;
            }
            for idx in &col.indexes {
                let Some(v) = scalar_at_path(row, &idx.path) else {
                    continue;
                };
                entries.push(IndexEntry {
                    collection_id: collection_id.0,
                    index_name: idx.name.clone(),
                    kind: idx.kind,
                    op: IndexOp::Insert,
                    index_key: v.canonical_key_bytes(),
                    pk_key: pk_scalar.canonical_key_bytes(),
                });
            }
        }

        self.transaction(|db| {
            if entries.is_empty() {
                return Ok(());
            }
            // Apply in-memory + persist as one index segment batch.
            // `begin_transaction` always installs `txn_staging` before this closure runs.
            let st = db
                .txn_staging
                .as_mut()
                .expect("transaction staging must be active");
            let b = encode_index_payload(&entries);
            st.pending
                .push((crate::segments::header::SegmentType::Index, b));
            for e in entries {
                st.shadow_indexes.apply(e)?;
            }
            Ok(())
        })
    }

    /// Force-register a new schema version, bypassing compatibility checks.
    ///
    /// This is an escape hatch for advanced workflows where the caller performs an out-of-band
    /// data rewrite (or accepts inconsistent index/query behavior until a rebuild).
    pub fn register_schema_version_with_indexes_force(
        &mut self,
        id: CollectionId,
        fields: Vec<FieldDef>,
        indexes: Vec<crate::schema::IndexDef>,
    ) -> Result<SchemaVersion, DbError> {
        let current = self
            .catalog_for_read()
            .get(id)
            .ok_or(DbError::Schema(SchemaError::UnknownCollection { id: id.0 }))?;
        let next_v = current
            .current_version
            .0
            .checked_add(1)
            .ok_or(DbError::Schema(SchemaError::SchemaVersionExhausted))?;
        let wire = CatalogRecordWire::NewSchemaVersion {
            collection_id: id.0,
            schema_version: next_v,
            fields,
            indexes,
        };
        let payload = encode_catalog_payload(&wire);
        if let Some(st) = &mut self.txn_staging {
            st.shadow_catalog.apply_record(wire.clone())?;
            st.pending
                .push((crate::segments::header::SegmentType::Schema, payload));
            return Ok(SchemaVersion(next_v));
        }
        let tid = self.next_txn_id();
        self.commit_write_batch(
            tid,
            &[(
                crate::segments::header::SegmentType::Schema,
                payload.as_slice(),
            )],
        )?;
        self.apply_catalog_record(wire)?;
        self.push_shared_mirror();
        Ok(SchemaVersion(next_v))
    }

    pub(crate) fn rewrite_collection_rows_at_current_version(
        &mut self,
        collection_id: CollectionId,
    ) -> Result<(), DbError> {
        let rows: Vec<BTreeMap<String, RowValue>> = self
            .latest_for_read()
            .iter()
            .filter(|((cid, _), _)| *cid == collection_id.0)
            .map(|(_, row)| row.clone())
            .collect();
        for row in rows {
            self.insert(collection_id, row)?;
        }
        Ok(())
    }
}
