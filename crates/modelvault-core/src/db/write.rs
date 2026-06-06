//! Row insert/delete and transaction staging.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::catalog::CatalogRecordWire;
use crate::error::{DbError, FormatError, SchemaError, TransactionError};
use crate::index::{encode_index_payload, IndexOp};
use crate::record::{
    encode_record_payload_v2_op, encode_record_payload_v3_op, non_pk_defs_in_order, RowValue,
    ScalarValue, OP_DELETE, OP_REPLACE,
};
use crate::schema::{CollectionId, FieldDef};
use crate::storage::Store;

use super::{
    handle_registry, index_deletes_for_existing_row, plan_insert_row, row_value_at_path_segments,
    Database,
};

impl<S: Store> Database<S> {
    pub(crate) fn next_txn_id(&mut self) -> u64 {
        self.txn_seq = self.txn_seq.saturating_add(1);
        self.txn_seq
    }

    #[inline]
    pub(crate) fn commit_write_batch(
        &mut self,
        txn_id: u64,
        body: &[(crate::segments::header::SegmentType, &[u8])],
    ) -> Result<(), DbError> {
        super::segment_write::commit_write_txn_v6(
            &mut self.store,
            self.segment_start,
            &mut self.format_minor,
            txn_id,
            body,
        )
    }

    #[inline]
    pub(crate) fn apply_catalog_record(&mut self, wire: CatalogRecordWire) -> Result<(), DbError> {
        self.catalog.apply_record(wire)
    }

    /// Run `f` inside a multi-write transaction: durable segments are written on success.
    ///
    /// On error, staged work is discarded and nothing new is appended to the log.
    pub fn transaction<R>(
        &mut self,
        f: impl FnOnce(&mut Self) -> Result<R, DbError>,
    ) -> Result<R, DbError> {
        self.begin_transaction()?;
        match f(self) {
            Ok(v) => match self.commit_transaction() {
                Ok(()) => Ok(v),
                Err(e) => {
                    self.rollback_transaction();
                    Err(e)
                }
            },
            Err(e) => {
                self.rollback_transaction();
                Err(e)
            }
        }
    }

    /// Start a transaction (for bindings that cannot use the closure API). Pairs with
    /// [`Self::commit_transaction`] or [`Self::rollback_transaction`].
    pub fn begin_transaction(&mut self) -> Result<(), DbError> {
        if self.txn_staging.is_some() {
            return Err(DbError::Transaction(TransactionError::NestedTransaction));
        }
        let tid = self.next_txn_id();
        self.txn_staging = Some(super::TxnStaging {
            txn_id: tid,
            shadow_catalog: self.catalog.clone(),
            shadow_latest: self.latest.clone(),
            shadow_indexes: self.indexes.clone(),
            pending: Vec::new(),
        });
        Ok(())
    }

    /// Commit the active transaction started with [`Self::begin_transaction`].
    pub fn commit_transaction(&mut self) -> Result<(), DbError> {
        self.commit_txn_staging()
    }

    /// Discard the active transaction without writing to the log.
    pub fn rollback_transaction(&mut self) {
        self.txn_staging = None;
    }

    fn commit_txn_staging(&mut self) -> Result<(), DbError> {
        let Some(st) = self.txn_staging.take() else {
            return Err(DbError::Transaction(TransactionError::NoActiveTransaction));
        };
        if st.pending.is_empty() {
            self.catalog = st.shadow_catalog;
            self.latest = st.shadow_latest;
            self.indexes = st.shadow_indexes;
            return Ok(());
        }
        let batch: Vec<(crate::segments::header::SegmentType, &[u8])> =
            st.pending.iter().map(|(t, b)| (*t, b.as_slice())).collect();
        self.commit_write_batch(st.txn_id, &batch)?;
        self.catalog = st.shadow_catalog;
        self.latest = st.shadow_latest;
        self.indexes = st.shadow_indexes;
        self.push_shared_mirror();
        Ok(())
    }

    pub(crate) fn push_shared_mirror(&mut self) {
        let Some(ref shared) = self.shared_mirror else {
            return;
        };
        if let Ok(mut g) = shared.write() {
            let generation = g.generation.saturating_add(1);
            *g = Arc::new(handle_registry::SharedDbState {
                catalog: self.catalog.clone(),
                latest: self.latest.clone(),
                indexes: self.indexes.clone(),
                segment_start: self.segment_start,
                format_minor: self.format_minor,
                generation,
            });
        }
    }

    /// Insert or replace the row for `collection_id` identified by its primary-key field.
    ///
    /// `row` maps **top-level** field names to [`RowValue`]. The primary key field must be present.
    /// Only single-segment field paths are supported in 0.6.x.
    pub fn insert(
        &mut self,
        collection_id: CollectionId,
        row: BTreeMap<String, RowValue>,
    ) -> Result<(), DbError> {
        if self.read_only_attached {
            return Err(DbError::Io(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "database opened read-only",
            )));
        }
        super::segment_write::ensure_header_v0_5(&mut self.store, &mut self.format_minor)?;
        let (mut payload, full, mut index_entries, pk_scalar) =
            plan_insert_row(self.catalog_for_read(), collection_id, row)?;
        #[cfg(test)]
        let mut full = full;
        let existing = self
            .latest_for_read()
            .get(&(collection_id.0, full.0.clone()))
            .cloned();
        if existing.is_some() {
            #[cfg(test)]
            if let Some(poison) = self.test_poison_planned_replace_row.take() {
                poison(collection_id, &mut full.1);
            }
            // Re-encode with explicit replace opcode.
            let col = self
                .catalog_for_read()
                .get(collection_id)
                .ok_or(DbError::Schema(SchemaError::UnknownCollection {
                    id: collection_id.0,
                }))?;
            let has_multi_segment_schema = col.fields.iter().any(|f| f.path.0.len() != 1);
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

            let non_pk_defs = if has_multi_segment_schema {
                col.fields
                    .iter()
                    .filter(|f| !(f.path.0.len() == 1 && f.path.0[0] == pk_name))
                    .collect::<Vec<_>>()
            } else {
                non_pk_defs_in_order(&col.fields, pk_name)
            };
            let mut non_pk: Vec<(FieldDef, RowValue)> = Vec::with_capacity(non_pk_defs.len());
            for def in &non_pk_defs {
                let v = row_value_at_path_segments(&full.1, &def.path.0).unwrap_or(RowValue::None);
                non_pk.push(((*def).clone(), v));
            }
            payload = (if has_multi_segment_schema {
                encode_record_payload_v3_op(
                    collection_id.0,
                    col.current_version.0,
                    OP_REPLACE,
                    &pk_scalar,
                    &pk_def.ty,
                    &non_pk,
                )
            } else {
                encode_record_payload_v2_op(
                    collection_id.0,
                    col.current_version.0,
                    OP_REPLACE,
                    &pk_scalar,
                    &pk_def.ty,
                    &non_pk,
                )
            })?;
            // Prepend index deletes for any existing row.
            if let Some(ref old_row) = existing {
                let mut deletes = index_deletes_for_existing_row(
                    collection_id,
                    &pk_scalar,
                    &col.indexes,
                    old_row,
                );
                deletes.append(&mut index_entries);
                index_entries = deletes;
            }
        }
        for e in &index_entries {
            if e.kind != crate::schema::IndexKind::Unique {
                continue;
            }
            let Some(existing) =
                self.indexes_for_read()
                    .unique_lookup(e.collection_id, &e.index_name, &e.index_key)
            else {
                continue;
            };
            if e.op != IndexOp::Insert {
                continue;
            }
            if existing == e.pk_key.as_slice() {
                continue;
            }
            return Err(DbError::Schema(SchemaError::UniqueIndexViolation));
        }
        if let Some(st) = &mut self.txn_staging {
            if !index_entries.is_empty() {
                let b = encode_index_payload(&index_entries);
                st.pending
                    .push((crate::segments::header::SegmentType::Index, b));
            }
            st.pending.push((
                crate::segments::header::SegmentType::Record,
                payload.clone(),
            ));
            st.shadow_latest
                .insert((collection_id.0, full.0.clone()), full.1.clone());
            for e in index_entries {
                st.shadow_indexes.apply(e)?;
            }
            return Ok(());
        }
        let tid = self.next_txn_id();
        let index_bytes = if index_entries.is_empty() {
            None
        } else {
            Some(encode_index_payload(&index_entries))
        };
        let mut batch: Vec<(crate::segments::header::SegmentType, &[u8])> = Vec::new();
        if let Some(ref b) = index_bytes {
            batch.push((crate::segments::header::SegmentType::Index, b.as_slice()));
        }
        batch.push((
            crate::segments::header::SegmentType::Record,
            payload.as_slice(),
        ));
        self.commit_write_batch(tid, &batch)?;
        self.latest.insert((collection_id.0, full.0), full.1);
        for e in index_entries {
            self.indexes.apply(e)?;
        }
        self.push_shared_mirror();
        Ok(())
    }

    /// Delete the row for `collection_id` identified by its primary key.
    pub fn delete(&mut self, collection_id: CollectionId, pk: &ScalarValue) -> Result<(), DbError> {
        if self.read_only_attached {
            return Err(DbError::Io(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "database opened read-only",
            )));
        }
        super::segment_write::ensure_header_v0_5(&mut self.store, &mut self.format_minor)?;
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
        if !pk.ty_matches(&pk_def.ty) {
            return Err(DbError::Format(FormatError::RecordPayloadTypeMismatch));
        }
        let pk_key = pk.canonical_key_bytes();
        let existing = self
            .latest_for_read()
            .get(&(collection_id.0, pk_key.clone()))
            .cloned();
        let Some(old_row) = existing else {
            return Ok(());
        };
        let indexes = col.indexes.clone();
        let schema_ver = col.current_version.0;
        let pk_ty = pk_def.ty.clone();
        let has_multi_segment_schema = col.fields.iter().any(|f| f.path.0.len() != 1);

        let mut index_entries =
            index_deletes_for_existing_row(collection_id, pk, &indexes, &old_row);
        #[cfg(not(test))]
        let pk_for_record = pk.clone();
        #[cfg(test)]
        let pk_for_record = {
            let mut p = pk.clone();
            if let Some(poison) = self.test_poison_delete_encode_scalar.take() {
                p = poison(p);
            }
            p
        };
        let record_payload = (if has_multi_segment_schema {
            encode_record_payload_v3_op(
                collection_id.0,
                schema_ver,
                OP_DELETE,
                &pk_for_record,
                &pk_ty,
                &[],
            )
        } else {
            encode_record_payload_v2_op(
                collection_id.0,
                schema_ver,
                OP_DELETE,
                &pk_for_record,
                &pk_ty,
                &[],
            )
        })?;

        if let Some(st) = &mut self.txn_staging {
            if !index_entries.is_empty() {
                let b = encode_index_payload(&index_entries);
                st.pending
                    .push((crate::segments::header::SegmentType::Index, b));
            }
            st.pending.push((
                crate::segments::header::SegmentType::Record,
                record_payload.clone(),
            ));
            st.shadow_latest.remove(&(collection_id.0, pk_key));
            for e in index_entries.drain(..) {
                st.shadow_indexes.apply(e)?;
            }
            return Ok(());
        }

        let tid = self.next_txn_id();
        let index_bytes = if index_entries.is_empty() {
            None
        } else {
            Some(encode_index_payload(&index_entries))
        };
        let mut batch: Vec<(crate::segments::header::SegmentType, &[u8])> = Vec::new();
        if let Some(ref b) = index_bytes {
            batch.push((crate::segments::header::SegmentType::Index, b.as_slice()));
        }
        batch.push((
            crate::segments::header::SegmentType::Record,
            record_payload.as_slice(),
        ));
        self.commit_write_batch(tid, &batch)?;
        self.latest.remove(&(collection_id.0, pk_key));
        for e in index_entries {
            self.indexes.apply(e)?;
        }
        self.push_shared_mirror();
        Ok(())
    }
}
