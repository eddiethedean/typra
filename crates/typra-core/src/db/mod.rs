//! Database handle and orchestration.
//!
//! [`Database`] is implemented using internal modules `open` (bootstrap), `replay` (catalog and
//! rows from segments), `write` (append segments and publish), and `helpers` (name rules).

mod helpers;
mod fs_ops;
mod open;
mod recover;
mod replay;
mod write;

use std::collections::{BTreeMap, HashMap};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use crate::catalog::{encode_catalog_payload, Catalog, CatalogRecordWire};
use crate::config::{OpenMode, OpenOptions};
use crate::error::{DbError, FormatError, SchemaError, TransactionError};
use crate::index::IndexState;
use crate::index::{encode_index_payload, IndexEntry, IndexOp};
use crate::record::{
    encode_record_payload_v2, encode_record_payload_v2_op, encode_record_payload_v3,
    encode_record_payload_v3_op, non_pk_defs_in_order, RowValue, ScalarValue, OP_DELETE,
    OP_REPLACE,
};
use crate::schema::{classify_schema_update, SchemaChange};
use crate::schema::{CollectionId, FieldDef, SchemaVersion};
use crate::segments::header::{SegmentHeader, SegmentType, SEGMENT_HEADER_LEN};
use crate::segments::writer::SegmentWriter;
use crate::storage::{FileStore, Store, VecStore};
use crate::validation;
use crate::{checkpoint, publish};
use crate::{MigrationPlan, MigrationStep};

use self::fs_ops::{FsOps, StdFsOps};

pub(crate) type LatestMap = HashMap<(u32, Vec<u8>), BTreeMap<String, RowValue>>;

type PlannedInsert = (
    Vec<u8>,
    (Vec<u8>, BTreeMap<String, RowValue>),
    Vec<IndexEntry>,
    ScalarValue,
);

fn plan_insert_row(
    catalog: &Catalog,
    collection_id: CollectionId,
    mut row: BTreeMap<String, RowValue>,
) -> Result<PlannedInsert, DbError> {
    let col =
        catalog
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
        // Catalog invariants guarantee the declared primary key exists in fields.
        .unwrap();
    let pk_ty = &pk_def.ty;
    validation::ensure_pk_type_primitive(pk_ty)?;
    let mut pk_path = vec![pk_name.to_string()];
    let pk_cell = row
        .get(pk_name)
        .ok_or(DbError::Schema(SchemaError::RowMissingPrimary {
            name: pk_name.to_string(),
        }))?;
    validation::validate_value(&mut pk_path, pk_ty, &pk_def.constraints, pk_cell)?;
    // Validate unknown fields: for nested schema paths we validate by traversing row objects.
    // For legacy single-segment schemas, keep the existing top-level validation.
    let has_multi_segment_schema = col.fields.iter().any(|f| f.path.0.len() != 1);
    if !has_multi_segment_schema {
        validation::validate_top_level_row(&col.fields, pk_name, &row)?;
    } else {
        fn walk_row(out: &mut Vec<Vec<String>>, prefix: &mut Vec<String>, v: &RowValue) {
            match v {
                RowValue::Object(map) => {
                    for (k, child) in map {
                        prefix.push(k.clone());
                        walk_row(out, prefix, child);
                        prefix.pop();
                    }
                }
                // Lists/enums/scalars/None are treated as leaves at this path.
                _ => out.push(prefix.clone()),
            }
        }

        let mut leaf_paths: Vec<Vec<String>> = Vec::new();
        for (k, v) in &row {
            if k == pk_name {
                continue;
            }
            let mut prefix = vec![k.clone()];
            walk_row(&mut leaf_paths, &mut prefix, v);
        }

        // Allowed leaf paths are exactly the schema field defs (excluding PK).
        let mut allowed: std::collections::HashSet<Vec<String>> = std::collections::HashSet::new();
        for f in &col.fields {
            if f.path.0.len() == 1 && f.path.0[0] == pk_name {
                continue;
            }
            allowed.insert(f.path.0.iter().map(|s| s.as_ref().to_string()).collect());
        }

        for p in &leaf_paths {
            if !allowed.contains(p) {
                return Err(DbError::Schema(SchemaError::RowUnknownField {
                    name: p.join("."),
                }));
            }
        }
    }

    // `pk_cell` is already present (validated above), so remove must succeed.
    let pk_val = row.remove(pk_name).unwrap();
    // PK type and value were validated as a primitive scalar.
    let pk_scalar = pk_val
        .clone()
        .into_scalar()
        .expect("validated primary key must be scalar");

    // Build non-PK values in schema order.
    // - legacy v2: single-segment top-level field defs
    // - v3: full FieldPath for each non-PK def (multi-segment allowed)
    let non_pk_defs = if has_multi_segment_schema {
        col.fields
            .iter()
            .filter(|f| !(f.path.0.len() == 1 && f.path.0[0] == pk_name))
            .collect::<Vec<_>>()
    } else {
        non_pk_defs_in_order(&col.fields, pk_name)
    };

    fn row_value_at_path(
        row: &BTreeMap<String, RowValue>,
        path: &[std::borrow::Cow<'static, str>],
    ) -> Option<RowValue> {
        let mut cur = row.get(
            path.first()
                .expect("catalog field paths are validated as non-empty")
                .as_ref(),
        )?;
        for seg in path.iter().skip(1) {
            cur = cur.as_object_map()?.get(seg.as_ref())?;
        }
        Some(cur.clone())
    }

    let mut non_pk: Vec<(FieldDef, RowValue)> = Vec::with_capacity(non_pk_defs.len());
    for def in &non_pk_defs {
        let v = match row_value_at_path(&row, &def.path.0) {
            Some(x) => x,
            None if validation::allows_absent_root(&def.ty) => RowValue::None,
            None => {
                return Err(DbError::Schema(SchemaError::RowMissingField {
                    name: def
                        .path
                        .0
                        .iter()
                        .map(|s| s.as_ref())
                        .collect::<Vec<_>>()
                        .join("."),
                }));
            }
        };
        non_pk.push(((*def).clone(), v));
    }

    let payload = if has_multi_segment_schema {
        encode_record_payload_v3(collection_id.0, col.current_version.0, &pk_scalar, pk_ty, &non_pk)
            .expect("record payload encoding must succeed after validation")
    } else {
        encode_record_payload_v2(collection_id.0, col.current_version.0, &pk_scalar, pk_ty, &non_pk)
            .expect("record payload encoding must succeed after validation")
    };

    // Build full row map (top-level root objects as needed).
    #[rustfmt::skip]
    fn merge_non_pk_into_full_map(full_map: &mut BTreeMap<String, RowValue>, parts: &[String], v: &RowValue) {
        let mut cur: &mut RowValue = full_map.entry(parts[0].clone()).or_insert_with(|| RowValue::Object(BTreeMap::new()));
        for seg in parts.iter().skip(1).take(parts.len().saturating_sub(2)) {
            if !matches!(cur, RowValue::Object(_)) { *cur = RowValue::Object(BTreeMap::new()); } if let RowValue::Object(m) = cur { cur = m.entry(seg.clone()).or_insert_with(|| RowValue::Object(BTreeMap::new())); }
        }
        if let RowValue::Object(m) = cur { m.insert(parts.last().unwrap().clone(), v.clone()); }
    }

    let mut full_map: BTreeMap<String, RowValue> = BTreeMap::new();
    full_map.insert(pk_name.to_string(), pk_val);
    for (def, v) in &non_pk {
        let parts: Vec<String> = def.path.0.iter().map(|s| s.as_ref().to_string()).collect();
        if parts.len() == 1 {
            full_map.insert(parts[0].clone(), v.clone());
        } else {
            debug_assert!(parts.len() >= 2);
            merge_non_pk_into_full_map(&mut full_map, &parts, v);
        }
    }
    let mut index_entries: Vec<IndexEntry> = Vec::new();
    for idx in &col.indexes {
        let Some(v) = scalar_at_path(&full_map, &idx.path) else {
            continue;
        };
        index_entries.push(IndexEntry {
            collection_id: collection_id.0,
            index_name: idx.name.clone(),
            kind: idx.kind,
            op: IndexOp::Insert,
            index_key: v.canonical_key_bytes(),
            pk_key: pk_scalar.canonical_key_bytes(),
        });
    }
    let pk_key = pk_scalar.canonical_key_bytes();
    Ok((payload, (pk_key, full_map), index_entries, pk_scalar))
}

fn index_deletes_for_existing_row(
    collection_id: CollectionId,
    pk_scalar: &ScalarValue,
    indexes: &[crate::schema::IndexDef],
    existing_row: &BTreeMap<String, RowValue>,
) -> Vec<IndexEntry> {
    let mut out = Vec::new();
    for idx in indexes {
        let Some(v) = scalar_at_path(existing_row, &idx.path) else {
            continue;
        };
        out.push(IndexEntry {
            collection_id: collection_id.0,
            index_name: idx.name.clone(),
            kind: idx.kind,
            op: IndexOp::Delete,
            index_key: v.canonical_key_bytes(),
            pk_key: pk_scalar.canonical_key_bytes(),
        });
    }
    out
}

/// Staged writes while [`Database::transaction`] is executing.
pub(crate) struct TxnStaging {
    pub(crate) txn_id: u64,
    pub(crate) shadow_catalog: Catalog,
    pub(crate) shadow_latest: LatestMap,
    pub(crate) shadow_indexes: IndexState,
    pub(crate) pending: Vec<(crate::segments::header::SegmentType, Vec<u8>)>,
}

/// Opened Typra database: generic over a [`Store`] ([`FileStore`] on disk, [`VecStore`] in memory).
pub struct Database<S: Store = FileStore> {
    /// Path shown by [`Database::path`] (`":memory:"` for [`VecStore`]).
    path: PathBuf,
    store: S,
    /// In-memory view of schema segments replayed from disk.
    catalog: Catalog,
    /// Byte offset where the append-only segment log begins (after header and superblocks).
    segment_start: u64,
    /// Format minor from the file header; may be lazily upgraded (`3` → `4` → `5`) on write.
    format_minor: u16,
    /// Latest row per `(collection_id, canonical primary-key bytes)`; last replayed insert wins.
    latest: LatestMap,
    /// Secondary indexes rebuilt from replayed `Index` segments.
    indexes: IndexState,
    /// Monotonic id for transaction marker segments (format minor 6+).
    txn_seq: u64,
    /// When set, [`insert`] / [`register_collection`] append to this batch instead of autocommit.
    txn_staging: Option<TxnStaging>,
}

impl<S: Store> Database<S> {
    fn compact_snapshot_bytes(&self) -> Result<Vec<u8>, DbError> {
        let mut out = Database::<VecStore>::open_in_memory()
            .expect("in-memory database open must not fail");

        // Recreate catalog (stable ids if created in id order).
        let mut cols = self.catalog_for_read().collections();
        cols.sort_by_key(|c| c.id.0);
        for c in &cols {
            let pk = c.primary_field.as_deref().unwrap();
            let (new_id, _v1) = out
                .register_collection_with_indexes(&c.name, c.fields.clone(), c.indexes.clone(), pk)
                .expect("compaction catalog rebuild must succeed");
            // Bump schema version counter to match current_version (repeat identical schema).
            for _ in 2..=c.current_version.0 {
                let _ = out
                    .register_schema_version_with_indexes_force(
                        new_id,
                        c.fields.clone(),
                        c.indexes.clone(),
                    )
                    .expect("schema version bump must succeed");
            }
        }

        // Copy latest rows (in-memory snapshot semantics).
        for ((cid, _), row) in self.latest_for_read().iter() {
            let collection_id = CollectionId(*cid);
            out.insert(collection_id, row.clone())
                .expect("snapshot row insert must succeed");
        }

        Ok(out.into_snapshot_bytes())
    }

    pub(crate) fn open_with_store(
        path: PathBuf,
        store: S,
        opts: OpenOptions,
    ) -> Result<Self, DbError> {
        open::open_with_store(path, store, opts)
    }

    fn next_txn_id(&mut self) -> u64 {
        self.txn_seq = self.txn_seq.saturating_add(1);
        self.txn_seq
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
            Ok(v) => {
                self.commit_transaction()
                    .expect("commit must succeed after successful transaction body");
                Ok(v)
            }
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
        self.txn_staging = Some(TxnStaging {
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
            return Ok(());
        };
        if st.pending.is_empty() {
            self.catalog = st.shadow_catalog;
            self.latest = st.shadow_latest;
            self.indexes = st.shadow_indexes;
            return Ok(());
        }
        let batch: Vec<(crate::segments::header::SegmentType, &[u8])> =
            st.pending.iter().map(|(t, b)| (*t, b.as_slice())).collect();
        write::commit_write_txn_v6(&mut self.store, self.segment_start, &mut self.format_minor, st.txn_id, &batch)?;
        self.catalog = st.shadow_catalog;
        self.latest = st.shadow_latest;
        self.indexes = st.shadow_indexes;
        Ok(())
    }

    fn catalog_for_read(&self) -> &Catalog {
        if let Some(ref st) = self.txn_staging {
            &st.shadow_catalog
        } else {
            &self.catalog
        }
    }

    fn indexes_for_read(&self) -> &IndexState {
        if let Some(ref st) = self.txn_staging {
            &st.shadow_indexes
        } else {
            &self.indexes
        }
    }

    fn latest_for_read(&self) -> &LatestMap {
        if let Some(ref st) = self.txn_staging {
            &st.shadow_latest
        } else {
            &self.latest
        }
    }

    /// Path passed to [`Database::open`](Database::<FileStore>::open), or `":memory:"` for [`VecStore`].
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Read-only view of the schema catalog built from `Schema` segments.
    pub fn catalog(&self) -> &Catalog {
        self.catalog_for_read()
    }

    /// All registered collection names in lexicographic order.
    pub fn collection_names(&self) -> Vec<String> {
        self.catalog_for_read().collection_names()
    }

    /// Read-only access to the in-memory secondary index state (rebuilt from `Index` segments).
    pub fn index_state(&self) -> &IndexState {
        self.indexes_for_read()
    }

    /// Execute a query against the current in-memory snapshot of the database.
    pub fn query(
        &self,
        q: &crate::query::Query,
    ) -> Result<Vec<BTreeMap<String, RowValue>>, DbError> {
        crate::query::execute_query(
            self.catalog_for_read(),
            self.indexes_for_read(),
            self.latest_for_read(),
            q,
        )
    }

    /// Return a human-readable explanation of the chosen plan for `q`.
    pub fn explain_query(&self, q: &crate::query::Query) -> Result<String, DbError> {
        crate::query::explain_query(self.catalog_for_read(), q)
    }

    /// Lazy iterator over query rows (same semantics as [`Self::query`]).
    ///
    /// See [`crate::query::QueryRowIter`] — this is the v0.7 pull-based execution boundary, not a
    /// full operator graph.
    pub fn query_iter(
        &self,
        q: &crate::query::Query,
    ) -> Result<crate::query::QueryRowIter<'_>, DbError> {
        crate::query::execute_query_iter_with_spill_path(
            self.catalog_for_read(),
            self.indexes_for_read(),
            self.latest_for_read(),
            q,
            Some(self.path.as_path()),
        )
    }

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

    /// Typed handle over a registered collection; `T` may be a *subset model*.
    pub fn collection<'a, T: crate::schema::DbModel>(
        &'a self,
    ) -> Result<Collection<'a, S, T>, DbError> {
        let cid = self.collection_id_named(T::collection_name())?;
        let col = self
            .catalog_for_read()
            .get(cid)
            .expect("collection id from name lookup must exist in catalog");
        validate_subset_model::<T>(col)?;
        Ok(Collection {
            db: self,
            collection_id: cid,
            _marker: PhantomData,
        })
    }

    /// Look up [`CollectionId`] by collection name (leading/trailing whitespace trimmed).
    ///
    /// Returns [`SchemaError::UnknownCollectionName`] when the name is not registered.
    pub fn collection_id_named(&self, name: &str) -> Result<CollectionId, DbError> {
        self.catalog_for_read()
            .lookup_name(name)
            .ok_or(DbError::Schema(SchemaError::UnknownCollectionName {
                name: name.trim().to_string(),
            }))
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
        write::commit_write_txn_v6(&mut self.store, self.segment_start, &mut self.format_minor, tid, &[(crate::segments::header::SegmentType::Schema, payload.as_slice())])?;
        self.catalog.apply_record(wire)?;
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
        match classify_schema_update(&current.fields, &current.indexes, &fields, &indexes).unwrap() {
            SchemaChange::Safe => {}
            SchemaChange::NeedsMigration { reason } => {
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
            return Ok(SchemaVersion(next_v));
        }
        let tid = self.next_txn_id();
        write::commit_write_txn_v6(&mut self.store, self.segment_start, &mut self.format_minor, tid, &[(crate::segments::header::SegmentType::Schema, payload.as_slice())])?;
        self.catalog.apply_record(wire)?;
        Ok(SchemaVersion(next_v))
    }

    /// Plan a schema version bump and return the required migration steps, if any.
    pub fn plan_schema_version_with_indexes(
        &self,
        id: CollectionId,
        fields: Vec<FieldDef>,
        indexes: Vec<crate::schema::IndexDef>,
    ) -> Result<MigrationPlan, DbError> {
        let current = self
            .catalog_for_read()
            .get(id)
            .ok_or(DbError::Schema(SchemaError::UnknownCollection { id: id.0 }))?;
        // Same infallibility contract as `register_schema_version_with_indexes` above.
        let change =
            classify_schema_update(&current.fields, &current.indexes, &fields, &indexes).unwrap();
        let mut steps = Vec::new();
        match &change {
            SchemaChange::Safe => {}
            SchemaChange::Breaking { .. } => {}
            SchemaChange::NeedsMigration { reason } => {
                if reason.contains("new required field") {
                    // Best-effort extract.
                    steps.push(MigrationStep::BackfillTopLevelField {
                        field: reason.to_string(),
                    });
                } else {
                    steps.push(MigrationStep::RebuildIndexes);
                }
            }
        }
        Ok(MigrationPlan { change, steps })
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
        let col = self
            .catalog_for_read()
            .get(collection_id)
            .ok_or(DbError::Schema(SchemaError::UnknownCollection {
                id: collection_id.0,
            }))?;
        let _pk_name =
            col.primary_field
                .as_deref()
                .ok_or(DbError::Schema(SchemaError::NoPrimaryKey {
                    collection_id: collection_id.0,
                }))?;

        // Snapshot the current rows so we can mutate the DB while iterating.
        let mut rows: Vec<BTreeMap<String, RowValue>> = Vec::new();
        for ((cid, _), row) in self.latest_for_read().iter() {
            if *cid != collection_id.0 {
                continue;
            }
            rows.push(row.clone());
        }

        self.transaction(|db| {
            for mut row in rows {
                if row.contains_key(field) {
                    continue;
                }
                row.insert(field.to_string(), value.clone());
                // `insert` performs replace-by-PK semantics and index maintenance.
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
        write::commit_write_txn_v6(&mut self.store, self.segment_start, &mut self.format_minor, tid, &[(crate::segments::header::SegmentType::Schema, payload.as_slice())])?;
        self.catalog.apply_record(wire)?;
        Ok(SchemaVersion(next_v))
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
        write::ensure_header_v0_5(&mut self.store, &mut self.format_minor)?;
        let (mut payload, full, mut index_entries, pk_scalar) =
            plan_insert_row(self.catalog_for_read(), collection_id, row)?;
        let existing = self
            .latest_for_read()
            .get(&(collection_id.0, full.0.clone()))
            .cloned();
        if existing.is_some() {
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
            payload = if has_multi_segment_schema {
                encode_record_payload_v3_op(collection_id.0, col.current_version.0, OP_REPLACE, &pk_scalar, &pk_def.ty, &non_pk)?
            } else {
                encode_record_payload_v2_op(collection_id.0, col.current_version.0, OP_REPLACE, &pk_scalar, &pk_def.ty, &non_pk)?
            };
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
            if e.kind == crate::schema::IndexKind::Unique {
                if let Some(existing) = self.indexes_for_read().unique_lookup(
                    e.collection_id,
                    &e.index_name,
                    &e.index_key,
                ) {
                    if e.op == IndexOp::Insert && existing != e.pk_key.as_slice() { return Err(DbError::Schema(SchemaError::UniqueIndexViolation)); }
                }
            }
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
        write::commit_write_txn_v6(&mut self.store, self.segment_start, &mut self.format_minor, tid, &batch)?;
        self.latest.insert((collection_id.0, full.0), full.1);
        for e in index_entries {
            self.indexes.apply(e)?;
        }
        Ok(())
    }

    /// Delete the row for `collection_id` identified by its primary key.
    pub fn delete(&mut self, collection_id: CollectionId, pk: &ScalarValue) -> Result<(), DbError> {
        write::ensure_header_v0_5(&mut self.store, &mut self.format_minor)?;
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
        let Some(old_row) = existing else { return Ok(()); };
        let mut index_entries =
            index_deletes_for_existing_row(collection_id, pk, &col.indexes, &old_row);
        let has_multi_segment_schema = col.fields.iter().any(|f| f.path.0.len() != 1);
        let record_payload = if has_multi_segment_schema {
            encode_record_payload_v3_op(collection_id.0, col.current_version.0, OP_DELETE, pk, &pk_def.ty, &[])?
        } else {
            encode_record_payload_v2_op(collection_id.0, col.current_version.0, OP_DELETE, pk, &pk_def.ty, &[])?
        };

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
        batch.push((crate::segments::header::SegmentType::Record, record_payload.as_slice()));
        write::commit_write_txn_v6(&mut self.store, self.segment_start, &mut self.format_minor, tid, &batch)?;
        self.latest.remove(&(collection_id.0, pk_key));
        for e in index_entries {
            self.indexes.apply(e)?;
        }
        Ok(())
    }

    /// Return the latest stored row for `pk`, or `None` if no insert has been replayed for that key.
    ///
    /// `pk` must match the declared primary field’s [`crate::schema::Type`].
    pub fn get(
        &self,
        collection_id: CollectionId,
        pk: &ScalarValue,
    ) -> Result<Option<BTreeMap<String, RowValue>>, DbError> {
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
        let pk_ty = col
            .fields
            .iter()
            .find(|f| f.path.0.len() == 1 && f.path.0[0] == pk_name)
            .map(|f| &f.ty)
            .ok_or(DbError::Schema(SchemaError::PrimaryFieldNotFound {
                name: pk_name.to_string(),
            }))?;
        if !pk.ty_matches(pk_ty) {
            return Err(DbError::Format(FormatError::RecordPayloadTypeMismatch));
        }
        let key = (collection_id.0, pk.canonical_key_bytes());
        Ok(self.latest_for_read().get(&key).cloned())
    }
}

impl Database<FileStore> {
    /// Rewrite the database into a compacted single-file image at `dest_path`.
    ///
    /// The destination file is truncated/overwritten if it exists.
    pub fn compact_to(&self, dest_path: impl AsRef<Path>) -> Result<(), DbError> {
        self.compact_to_with_fsops(&StdFsOps, dest_path)
    }

    pub(crate) fn compact_to_with_fsops(
        &self,
        fs: &dyn FsOps,
        dest_path: impl AsRef<Path>,
    ) -> Result<(), DbError> {
        let bytes = self.compact_snapshot_bytes()?;
        let path = dest_path.as_ref();
        let file = fs
            .open_read_write_create_truncate(path)
            .map_err(DbError::Io)?;
        let mut store = FileStore::new(file);
        store.write_all_at(0, &bytes)?;
        store.truncate(bytes.len() as u64)?;
        store.sync()?;
        Ok(())
    }

    /// Compact and rewrite this database in place.
    pub fn compact_in_place(&mut self) -> Result<(), DbError> {
        self.compact_in_place_with_fsops(&StdFsOps)
    }

    pub(crate) fn compact_in_place_with_fsops(&mut self, fs: &dyn FsOps) -> Result<(), DbError> {
        // Crash-safety: write a full new image to a sidecar file, fsync it, then atomically
        // replace the live path via rename (using a backup on platforms where rename does not
        // overwrite an existing destination).
        let bytes = self.compact_snapshot_bytes()?;
        let live_path = self.path.clone();
        let parent = live_path
            .parent()
            .ok_or_else(|| DbError::Io(std::io::Error::other("no parent")))?;

        // Pick unique temp + backup names in the same directory (so rename stays atomic on POSIX).
        let pid = std::process::id();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let base = live_path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("db.typra");
        let tmp_path = parent.join(format!("{base}.compact.{pid}.{nanos}.tmp"));
        let bak_path = parent.join(format!("{base}.compact.{pid}.{nanos}.bak"));

        // 1) Write the compacted image to tmp and fsync it.
        {
            let file = fs.open_read_write_create_new(&tmp_path).map_err(DbError::Io)?;
            let mut store = FileStore::new(file);
            store.write_all_at(0, &bytes)?;
            store.truncate(bytes.len() as u64)?;
            store.sync()?;
        }

        // 2) Replace the live file path with the tmp image, preserving a backup until success.
        //
        // We do not rely on "rename over existing" being supported across platforms. Instead:
        // - move live → bak
        // - move tmp → live
        // - fsync directory (best-effort)
        // - remove bak
        //
        // If tmp → live fails, attempt to restore bak → live.
        let _ = fs.remove_file(&bak_path);
        fs.rename(&live_path, &bak_path).map_err(DbError::Io)?;
        let replace_res = fs.rename(&tmp_path, &live_path);
        if let Err(e) = replace_res {
            // Best-effort restore: move backup back into place.
            let _ = fs.rename(&bak_path, &live_path);
            // Clean up tmp if it still exists.
            let _ = fs.remove_file(&tmp_path);
            return Err(DbError::Io(e));
        }

        // Best-effort directory sync: helps make the rename durable on POSIX.
        #[cfg(unix)]
        {
            // Best-effort: on many Unix platforms, opening a directory and syncing it will persist
            // the rename in the directory entry. If this fails, the data file itself is still
            // fsync'd and the operation remains logically correct; only rename durability is weaker.
            if let Ok(dir_f) = fs.open_dir(parent) {
                let _ = dir_f.sync_all();
            }
        }

        let _ = fs.remove_file(&bak_path);

        // 3) Refresh in-memory state by reopening.
        let reopened = Database::open_with_options(live_path, OpenOptions::default())?;
        *self = reopened;
        Ok(())
    }

    /// Write a durable checkpoint and publish it via the superblock.
    ///
    /// The checkpoint stores the logical state (catalog + latest rows + index state) so open can
    /// avoid scanning/replaying the full log.
    pub fn checkpoint(&mut self) -> Result<(), DbError> {
        if self.txn_staging.is_some() {
            return Err(DbError::Transaction(TransactionError::NestedTransaction));
        }

        write::ensure_header_v0_6(&mut self.store, &mut self.format_minor)?;

        let mut cp = checkpoint::checkpoint_from_state(self.catalog_for_read(), self.latest_for_read(), self.indexes_for_read())?;

        let file_len = self.store.len()?;
        let mut writer = SegmentWriter::new(&mut self.store, file_len.max(self.segment_start));
        let checkpoint_offset = writer.offset();

        let payload_len = checkpoint::encode_checkpoint_payload_v0(&cp).len() as u64;
        let replay_from = checkpoint_offset + SEGMENT_HEADER_LEN as u64 + payload_len;
        cp.replay_from_offset = replay_from;
        let payload = checkpoint::encode_checkpoint_payload_v0(&cp);

        let hdr = SegmentHeader { segment_type: SegmentType::Checkpoint, payload_len: 0, payload_crc32c: 0 };
        writer.append(hdr, &payload)?;

        let _ = publish::append_manifest_and_publish_with_checkpoint(&mut self.store, self.segment_start, Some((checkpoint_offset, payload.len() as u32)))?;
        self.store.sync()?;
        Ok(())
    }

    /// Create a consistent backup copy of this on-disk database.
    ///
    /// This writes a checkpoint (for fast reopen and a stable state marker) and then copies the
    /// underlying file bytes to `dest_path`.
    pub fn export_snapshot_to_path(&mut self, dest_path: impl AsRef<Path>) -> Result<(), DbError> {
        self.checkpoint()?;
        let dest_path = dest_path.as_ref();
        std::fs::copy(&self.path, dest_path)?;
        // Strengthen durability of the copied snapshot: fsync the destination and best-effort
        // fsync its parent directory so the directory entry is persisted.
        if let Ok(f) = std::fs::OpenOptions::new().read(true).open(dest_path) {
            let _ = f.sync_all();
        }
        #[cfg(unix)]
        {
            if let Some(parent) = dest_path.parent() { if let Ok(dir_f) = std::fs::File::open(parent) { let _ = dir_f.sync_all(); } }
        }
        Ok(())
    }

    /// Restore a snapshot file into `dest_path` by atomically replacing the destination.
    ///
    /// This is a file operation helper intended for operational tooling.
    pub fn restore_snapshot_to_path(
        snapshot_path: impl AsRef<Path>,
        dest_path: impl AsRef<Path>,
    ) -> Result<(), DbError> {
        Self::restore_snapshot_to_path_with_fsops(&StdFsOps, snapshot_path, dest_path)
    }

    pub(crate) fn restore_snapshot_to_path_with_fsops(
        fs: &dyn FsOps,
        snapshot_path: impl AsRef<Path>,
        dest_path: impl AsRef<Path>,
    ) -> Result<(), DbError> {
        let snapshot_path = snapshot_path.as_ref();
        let dest_path = dest_path.as_ref();
        let parent = dest_path
            .parent()
            .ok_or_else(|| DbError::Io(std::io::Error::other("no parent")))?;

        let pid = std::process::id();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let base = dest_path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("db.typra");
        let tmp_path = parent.join(format!("{base}.restore.{pid}.{nanos}.tmp"));
        let bak_path = parent.join(format!("{base}.restore.{pid}.{nanos}.bak"));

        // Copy snapshot bytes into a temp file and fsync it.
        fs.copy(snapshot_path, &tmp_path).map_err(DbError::Io)?;
        if let Ok(f) = fs.open_read(&tmp_path) {
            let _ = f.sync_all();
        }

        // Replace destination with backup/restore semantics.
        if dest_path.exists() {
            let _ = fs.remove_file(&bak_path);
            fs.rename(dest_path, &bak_path).map_err(DbError::Io)?;
        }
        let replace_res = fs.rename(&tmp_path, dest_path);
        if let Err(e) = replace_res {
            // Best-effort restore original.
            if bak_path.exists() {
                let _ = fs.rename(&bak_path, dest_path);
            }
            let _ = fs.remove_file(&tmp_path);
            return Err(DbError::Io(e));
        }

        #[cfg(unix)]
        {
            if let Ok(dir_f) = fs.open_dir(parent) {
                let _ = dir_f.sync_all();
            }
        }
        let _ = fs.remove_file(&bak_path);
        Ok(())
    }
}

pub struct Collection<'a, S: Store, T: crate::schema::DbModel> {
    db: &'a Database<S>,
    collection_id: CollectionId,
    _marker: PhantomData<T>,
}

impl<'a, S: Store, T: crate::schema::DbModel> Collection<'a, S, T> {
    pub fn where_eq(
        &self,
        path: crate::schema::FieldPath,
        value: ScalarValue,
    ) -> QueryBuilder<'a, S, T> {
        QueryBuilder {
            db: self.db,
            collection_id: self.collection_id,
            predicate: Some(crate::query::Predicate::Eq { path, value }),
            limit: None,
            _marker: PhantomData,
        }
    }

    pub fn all(&self) -> Result<Vec<BTreeMap<String, RowValue>>, DbError> {
        let q = crate::query::Query {
            collection: self.collection_id,
            predicate: None,
            limit: None,
            order_by: None,
        };
        let rows = self.db.query(&q)?;
        Ok(rows.into_iter().map(project_row::<T>).collect())
    }
}

pub struct QueryBuilder<'a, S: Store, T: crate::schema::DbModel> {
    db: &'a Database<S>,
    collection_id: CollectionId,
    predicate: Option<crate::query::Predicate>,
    limit: Option<usize>,
    _marker: PhantomData<T>,
}

impl<'a, S: Store, T: crate::schema::DbModel> QueryBuilder<'a, S, T> {
    pub fn limit(mut self, n: usize) -> Self {
        self.limit = Some(n);
        self
    }

    pub fn all(self) -> Result<Vec<BTreeMap<String, RowValue>>, DbError> {
        let q = crate::query::Query {
            collection: self.collection_id,
            predicate: self.predicate,
            limit: self.limit,
            order_by: None,
        };
        let rows = self.db.query(&q)?;
        Ok(rows.into_iter().map(project_row::<T>).collect())
    }

    pub fn explain(self) -> Result<String, DbError> {
        let q = crate::query::Query {
            collection: self.collection_id,
            predicate: self.predicate,
            limit: self.limit,
            order_by: None,
        };
        self.db.explain_query(&q)
    }
}

fn validate_subset_model<T: crate::schema::DbModel>(
    col: &crate::catalog::CollectionInfo,
) -> Result<(), DbError> {
    let want_primary = T::primary_field();
    let Some(pk) = col.primary_field.as_deref() else {
        return Err(DbError::Schema(SchemaError::NoPrimaryKey {
            collection_id: col.id.0,
        }));
    };
    if pk != want_primary {
        return Err(DbError::Schema(SchemaError::PrimaryFieldNotFound {
            name: want_primary.to_string(),
        }));
    }
    let model_fields = T::fields();
    for mf in &model_fields {
        let Some(cf) = col.fields.iter().find(|f| f.path == mf.path) else {
            return Err(DbError::Schema(SchemaError::RowUnknownField {
                name: mf.path.0.last().map(|s| s.to_string()).unwrap_or_default(),
            }));
        };
        if cf.ty != mf.ty {
            return Err(DbError::Format(FormatError::RecordPayloadTypeMismatch));
        }
    }
    Ok(())
}

/// Build a row map containing only the listed fields (same rules as subset-model projection).
pub fn row_subset_by_field_defs(
    row: &BTreeMap<String, RowValue>,
    wanted: &[FieldDef],
) -> BTreeMap<String, RowValue> {
    let mut out: BTreeMap<String, RowValue> = BTreeMap::new();
    for f in wanted {
        let segs = &f.path.0;
        if segs.is_empty() {
            continue;
        }
        let Some(leaf) = row_value_at_path_segments(row, segs) else {
            continue;
        };
        let root = segs[0].to_string();
        if segs.len() == 1 {
            out.insert(root, leaf);
        } else {
            let nested = row_value_nested_object_path(&segs[1..], leaf);
            match out.get_mut(&root) {
                Some(existing) => merge_row_value_trees(existing, nested),
                None => {
                    out.insert(root, nested);
                }
            }
        }
    }
    out
}

fn row_value_at_path_segments(
    row: &BTreeMap<String, RowValue>,
    path: &[std::borrow::Cow<'static, str>],
) -> Option<RowValue> {
    if path.is_empty() {
        return None;
    }
    let mut cur = row.get(path[0].as_ref())?;
    for seg in path.iter().skip(1) {
        cur = match cur {
            RowValue::Object(m) => m.get(seg.as_ref())?,
            RowValue::None => return None,
            _ => return None,
        };
    }
    Some(cur.clone())
}

/// Build `Object({ seg[0]: Object({ seg[1]: ... leaf }) })` for non-empty `seg`.
fn row_value_nested_object_path(
    segments: &[std::borrow::Cow<'static, str>],
    leaf: RowValue,
) -> RowValue {
    debug_assert!(!segments.is_empty());
    if segments.len() == 1 {
        let mut m = BTreeMap::new();
        m.insert(segments[0].to_string(), leaf);
        RowValue::Object(m)
    } else {
        let mut m = BTreeMap::new();
        m.insert(
            segments[0].to_string(),
            row_value_nested_object_path(&segments[1..], leaf),
        );
        RowValue::Object(m)
    }
}

fn merge_row_value_trees(into: &mut RowValue, from: RowValue) {
    match (&mut *into, from) {
        (RowValue::Object(m1), RowValue::Object(m2)) => {
            for (k, v2) in m2 {
                match m1.entry(k) {
                    std::collections::btree_map::Entry::Vacant(e) => {
                        e.insert(v2);
                    }
                    std::collections::btree_map::Entry::Occupied(mut e) => {
                        merge_row_value_trees(e.get_mut(), v2);
                    }
                }
            }
        }
        (slot, from) => *slot = from,
    }
}

fn project_row<T: crate::schema::DbModel>(
    row: BTreeMap<String, RowValue>,
) -> BTreeMap<String, RowValue> {
    row_subset_by_field_defs(&row, &T::fields())
}

pub(crate) fn scalar_at_path(
    row: &BTreeMap<String, RowValue>,
    path: &crate::schema::FieldPath,
) -> Option<ScalarValue> {
    let mut cur: Option<&RowValue> = None;
    for (i, seg) in path.0.iter().enumerate() {
        let key = seg.as_ref();
        cur = match (i, cur) {
            (0, _) => row.get(key),
            (_, Some(RowValue::Object(map))) => map.get(key),
            (_, Some(RowValue::None)) => return None,
            _ => return None,
        };
    }
    cur.and_then(|v| v.as_scalar())
}

impl Database<FileStore> {
    /// Open an existing file or create a new database at `path`.
    ///
    /// Creates parent directories as needed via the OS; the file is opened read/write.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, DbError> {
        Self::open_with_options(path, crate::config::OpenOptions::default())
    }

    /// Open an existing file read-only (does not create it).
    pub fn open_read_only(path: impl AsRef<Path>) -> Result<Self, DbError> {
        Self::open_with_options(
            path,
            crate::config::OpenOptions {
                recovery: crate::config::RecoveryMode::Strict,
                mode: OpenMode::ReadOnly,
            },
        )
    }

    /// Open with recovery and other options (see [`crate::config::OpenOptions`]).
    pub fn open_with_options(
        path: impl AsRef<Path>,
        opts: crate::config::OpenOptions,
    ) -> Result<Self, DbError> {
        let path = path.as_ref().to_path_buf();
        let store = FileStore::open_locked(&path, opts.mode)?;
        Self::open_with_store(path, store, opts)
    }
}

impl Database<VecStore> {
    /// New empty in-memory database (same on-disk layout as a new file image in a [`VecStore`]).
    pub fn open_in_memory() -> Result<Self, DbError> {
        Self::open_in_memory_with_options(crate::config::OpenOptions::default())
    }

    /// In-memory open with [`crate::config::OpenOptions`].
    pub fn open_in_memory_with_options(opts: crate::config::OpenOptions) -> Result<Self, DbError> {
        Self::open_with_store(PathBuf::from(":memory:"), VecStore::new(), opts)
    }

    /// Deserialize a full database image from bytes (e.g. from [`into_snapshot_bytes`](Self::into_snapshot_bytes)).
    pub fn from_snapshot_bytes(bytes: Vec<u8>) -> Result<Self, DbError> {
        Self::open_with_store(
            PathBuf::from(":memory:"),
            VecStore::from_vec(bytes),
            crate::config::OpenOptions::default(),
        )
    }

    /// Consume `self` and return the owned byte buffer backing the store.
    pub fn into_snapshot_bytes(self) -> Vec<u8> {
        self.store.into_inner()
    }

    /// Clone of the full serialized database image (alias of the buffer returned by [`into_snapshot_bytes`](Self::into_snapshot_bytes)).
    pub fn snapshot_bytes(&self) -> Vec<u8> {
        self.store.as_slice().to_vec()
    }

    /// Write the full in-memory database image to `dest_path`.
    pub fn export_snapshot_to_path(&self, dest_path: impl AsRef<Path>) -> Result<(), DbError> {
        std::fs::write(dest_path.as_ref(), self.snapshot_bytes())?;
        Ok(())
    }

    /// Open an in-memory database from a snapshot file.
    pub fn open_snapshot_path(path: impl AsRef<Path>) -> Result<Self, DbError> {
        let bytes = std::fs::read(path.as_ref())?;
        Self::from_snapshot_bytes(bytes)
    }
}

#[cfg(test)]
mod scalar_at_path_tests {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/unit/src_db_mod_scalar_at_path_tests.rs"
    ));
}

#[cfg(test)]
mod tests {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/unit/src_db_mod_tests.rs"
    ));
}
