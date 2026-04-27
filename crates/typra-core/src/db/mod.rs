//! Database handle and orchestration.
//!
//! [`Database`] is implemented using internal modules `open` (bootstrap), `replay` (catalog and
//! rows from segments), `write` (append segments and publish), and `helpers` (name rules).

mod helpers;
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

pub(crate) type LatestMap = HashMap<(u32, Vec<u8>), BTreeMap<String, RowValue>>;

type PlannedInsert = (
    Vec<u8>,
    (Vec<u8>, BTreeMap<String, RowValue>),
    Vec<IndexEntry>,
    ScalarValue,
);

/// Read a value from a row map following a multi-segment path (used by [`plan_insert_row`]).
fn row_value_at_path_for_plan(
    row: &BTreeMap<String, RowValue>,
    path: &[std::borrow::Cow<'static, str>],
) -> Option<RowValue> {
    let first = path.first()?;
    let mut cur = row.get(first.as_ref())?;
    for seg in path.iter().skip(1) {
        cur = match cur {
            RowValue::Object(m) => m.get(seg.as_ref())?,
            RowValue::None => return None,
            _ => return None,
        };
    }
    Some(cur.clone())
}

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
    // Catalog invariants: if a collection has a primary key, its schema must contain a matching
    // top-level field. Catalog creation/versioning enforces this.
    let mut pk_def = None;
    for f in &col.fields {
        if f.path.0.len() != 1 {
            continue;
        }
        if f.path.0[0] != pk_name {
            continue;
        }
        pk_def = Some(f);
        break;
    }
    let pk_def = pk_def.unwrap();
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

    // We validated `pk_cell` above, so removal must succeed.
    let pk_val = row.remove(pk_name).unwrap();
    let pk_scalar = pk_val.clone().into_scalar()?;

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

    let mut non_pk: Vec<(FieldDef, RowValue)> = Vec::with_capacity(non_pk_defs.len());
    for def in &non_pk_defs {
        let v = match row_value_at_path_for_plan(&row, &def.path.0) {
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
        encode_record_payload_v3(
            collection_id.0,
            col.current_version.0,
            &pk_scalar,
            pk_ty,
            &non_pk,
        )?
    } else {
        encode_record_payload_v2(
            collection_id.0,
            col.current_version.0,
            &pk_scalar,
            pk_ty,
            &non_pk,
        )?
    };

    // Build full row map (top-level root objects as needed).
    let mut full_map: BTreeMap<String, RowValue> = BTreeMap::new();
    full_map.insert(pk_name.to_string(), pk_val);
    for (def, v) in &non_pk {
        let parts: Vec<String> = def.path.0.iter().map(|s| s.as_ref().to_string()).collect();
        debug_assert!(!parts.is_empty(), "catalog field paths are non-empty");
        let root = &parts[0];
        if parts.len() == 1 {
            full_map.insert(root.clone(), v.clone());
        } else {
            // Reuse local helper below (same as projection merge semantics).
            let mut cur: &mut RowValue = full_map
                .entry(root.clone())
                .or_insert_with(|| RowValue::Object(BTreeMap::new()));
            for seg in parts.iter().skip(1).take(parts.len() - 2) {
                cur = match cur {
                    RowValue::Object(m) => m
                        .entry(seg.clone())
                        .or_insert_with(|| RowValue::Object(BTreeMap::new())),
                    other => {
                        *other = RowValue::Object(BTreeMap::new());
                        match other {
                            RowValue::Object(m) => m
                                .entry(seg.clone())
                                .or_insert_with(|| RowValue::Object(BTreeMap::new())),
                            _ => unreachable!(),
                        }
                    }
                };
            }
            if let RowValue::Object(m) = cur {
                m.insert(parts.last().expect("len > 1").clone(), v.clone());
            }
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
        let mut out = Database::<VecStore>::open_in_memory()?;

        // Recreate catalog (stable ids if created in id order).
        let mut cols = self.catalog_for_read().collections();
        cols.sort_by_key(|c| c.id.0);
        for c in &cols {
            let pk =
                c.primary_field
                    .as_deref()
                    .ok_or(DbError::Schema(SchemaError::NoPrimaryKey {
                        collection_id: c.id.0,
                    }))?;
            let (new_id, _v1) = out.register_collection_with_indexes(
                &c.name,
                c.fields.clone(),
                c.indexes.clone(),
                pk,
            )?;
            if new_id.0 != c.id.0 {
                return Err(DbError::Schema(SchemaError::IncompatibleSchemaChange {
                    message: "collection id mismatch during compaction".to_string(),
                }));
            }
            // Bump schema version counter to match current_version (repeat identical schema).
            for _ in 2..=c.current_version.0 {
                let _ = out.register_schema_version_with_indexes_force(
                    new_id,
                    c.fields.clone(),
                    c.indexes.clone(),
                )?;
            }
        }

        // Copy latest rows (in-memory snapshot semantics).
        for ((cid, _), row) in self.latest_for_read().iter() {
            let collection_id = CollectionId(*cid);
            out.insert(collection_id, row.clone())?;
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
        // `saturating_add` never yields 0 from any starting `txn_seq`, so a post-increment
        // zero-check would be dead code (and shows up as an impossible branch in llvm-cov).
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
                self.commit_transaction()?;
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
        match self.txn_staging.take() {
            None => Ok(()),
            Some(st) => {
                match st.pending.is_empty() {
                    true => {}
                    false => {
                        let batch: Vec<(crate::segments::header::SegmentType, &[u8])> =
                            st.pending.iter().map(|(t, b)| (*t, b.as_slice())).collect();
                        write::commit_write_txn_v6(
                            &mut self.store,
                            self.segment_start,
                            &mut self.format_minor,
                            st.txn_id,
                            &batch,
                        )?;
                    }
                }
                self.catalog = st.shadow_catalog;
                self.latest = st.shadow_latest;
                self.indexes = st.shadow_indexes;
                Ok(())
            }
        }
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
        validate_subset_model::<T>(self.catalog_for_read().get(cid).ok_or(DbError::Schema(
            SchemaError::UnknownCollection { id: cid.0 },
        ))?)?;
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
        write::commit_write_txn_v6(
            &mut self.store,
            self.segment_start,
            &mut self.format_minor,
            tid,
            &[(
                crate::segments::header::SegmentType::Schema,
                payload.as_slice(),
            )],
        )?;
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
        match classify_schema_update(&current.fields, &current.indexes, &fields, &indexes)? {
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
        write::commit_write_txn_v6(
            &mut self.store,
            self.segment_start,
            &mut self.format_minor,
            tid,
            &[(
                crate::segments::header::SegmentType::Schema,
                payload.as_slice(),
            )],
        )?;
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
        let change = classify_schema_update(&current.fields, &current.indexes, &fields, &indexes)?;
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
        // Catalog invariants: if a collection has a primary key, its schema must contain a matching
        // top-level field. Catalog creation/versioning enforces this.
        let pk_def = col
            .fields
            .iter()
            .find(|f| f.path.0.len() == 1 && f.path.0[0] == pk_name)
            .unwrap();

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
            if let Some(st) = &mut db.txn_staging {
                let b = encode_index_payload(&entries);
                st.pending
                    .push((crate::segments::header::SegmentType::Index, b));
                for e in entries {
                    st.shadow_indexes.apply(e)?;
                }
                return Ok(());
            }
            // Should never reach here: `transaction` always sets staging.
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
        write::commit_write_txn_v6(
            &mut self.store,
            self.segment_start,
            &mut self.format_minor,
            tid,
            &[(
                crate::segments::header::SegmentType::Schema,
                payload.as_slice(),
            )],
        )?;
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
                .unwrap();

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
                encode_record_payload_v3_op(
                    collection_id.0,
                    col.current_version.0,
                    OP_REPLACE,
                    &pk_scalar,
                    &pk_def.ty,
                    &non_pk,
                )?
            } else {
                encode_record_payload_v2_op(
                    collection_id.0,
                    col.current_version.0,
                    OP_REPLACE,
                    &pk_scalar,
                    &pk_def.ty,
                    &non_pk,
                )?
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
                    if e.op == IndexOp::Insert && existing != e.pk_key.as_slice() {
                        return Err(DbError::Schema(SchemaError::UniqueIndexViolation));
                    }
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
        write::commit_write_txn_v6(
            &mut self.store,
            self.segment_start,
            &mut self.format_minor,
            tid,
            &batch,
        )?;
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
            .unwrap();
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
        let mut index_entries =
            index_deletes_for_existing_row(collection_id, pk, &col.indexes, &old_row);
        let has_multi_segment_schema = col.fields.iter().any(|f| f.path.0.len() != 1);
        let record_payload = if has_multi_segment_schema {
            encode_record_payload_v3_op(
                collection_id.0,
                col.current_version.0,
                OP_DELETE,
                pk,
                &pk_def.ty,
                &[],
            )?
        } else {
            encode_record_payload_v2_op(
                collection_id.0,
                col.current_version.0,
                OP_DELETE,
                pk,
                &pk_def.ty,
                &[],
            )?
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
        batch.push((
            crate::segments::header::SegmentType::Record,
            record_payload.as_slice(),
        ));
        write::commit_write_txn_v6(
            &mut self.store,
            self.segment_start,
            &mut self.format_minor,
            tid,
            &batch,
        )?;
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
            .unwrap();
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
        let bytes = self.compact_snapshot_bytes()?;
        let path = dest_path.as_ref();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)?;
        let mut store = FileStore::new(file);
        store.write_all_at(0, &bytes)?;
        store.truncate(bytes.len() as u64)?;
        store.sync()?;
        Ok(())
    }

    /// Compact and rewrite this database in place.
    pub fn compact_in_place(&mut self) -> Result<(), DbError> {
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
            let file = std::fs::OpenOptions::new()
                .read(true)
                .write(true)
                .create_new(true)
                .open(&tmp_path)?;
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
        if bak_path.exists() {
            let _ = std::fs::remove_file(&bak_path);
        }
        std::fs::rename(&live_path, &bak_path)?;
        let replace_res = std::fs::rename(&tmp_path, &live_path);
        if let Err(e) = replace_res {
            // Best-effort restore: move backup back into place.
            let _ = std::fs::rename(&bak_path, &live_path);
            // Clean up tmp if it still exists.
            let _ = std::fs::remove_file(&tmp_path);
            return Err(DbError::Io(e));
        }

        // Best-effort directory sync: helps make the rename durable on POSIX.
        #[cfg(unix)]
        {
            // Best-effort: on many Unix platforms, opening a directory and syncing it will persist
            // the rename in the directory entry. If this fails, the data file itself is still
            // fsync'd and the operation remains logically correct; only rename durability is weaker.
            if let Ok(dir_f) = std::fs::File::open(parent) {
                let _ = dir_f.sync_all();
            }
        }

        let _ = std::fs::remove_file(&bak_path);

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

        let mut cp = checkpoint::checkpoint_from_state(
            self.catalog_for_read(),
            self.latest_for_read(),
            self.indexes_for_read(),
        )?;

        let file_len = self.store.len()?;
        let mut writer = SegmentWriter::new(&mut self.store, file_len.max(self.segment_start));
        let checkpoint_offset = writer.offset();

        let payload_len = checkpoint::encode_checkpoint_payload_v0(&cp).len() as u64;
        let replay_from = checkpoint_offset + SEGMENT_HEADER_LEN as u64 + payload_len;
        cp.replay_from_offset = replay_from;
        let payload = checkpoint::encode_checkpoint_payload_v0(&cp);

        writer.append(
            SegmentHeader {
                segment_type: SegmentType::Checkpoint,
                payload_len: 0,
                payload_crc32c: 0,
            },
            &payload,
        )?;

        let _ = publish::append_manifest_and_publish_with_checkpoint(
            &mut self.store,
            self.segment_start,
            Some((checkpoint_offset, payload.len() as u32)),
        )?;
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
            if let Some(parent) = dest_path.parent() {
                if let Ok(dir_f) = std::fs::File::open(parent) {
                    let _ = dir_f.sync_all();
                }
            }
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
        std::fs::copy(snapshot_path, &tmp_path)?;
        if let Ok(f) = std::fs::OpenOptions::new().read(true).open(&tmp_path) {
            let _ = f.sync_all();
        }

        // Replace destination with backup/restore semantics.
        if dest_path.exists() {
            if bak_path.exists() {
                let _ = std::fs::remove_file(&bak_path);
            }
            std::fs::rename(dest_path, &bak_path)?;
        }
        let replace_res = std::fs::rename(&tmp_path, dest_path);
        if let Err(e) = replace_res {
            // Best-effort restore original.
            if bak_path.exists() {
                let _ = std::fs::rename(&bak_path, dest_path);
            }
            let _ = std::fs::remove_file(&tmp_path);
            return Err(DbError::Io(e));
        }

        #[cfg(unix)]
        {
            if let Ok(dir_f) = std::fs::File::open(parent) {
                let _ = dir_f.sync_all();
            }
        }
        let _ = std::fs::remove_file(&bak_path);
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
        match segs.is_empty() {
            true => continue,
            false => {}
        }
        let leaf = match row_value_at_path_segments(row, segs) {
            Some(v) => v,
            None => continue,
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
mod tests {
    use super::Database;
    use crate::db::open;
    use crate::db::write;
    use crate::catalog::{encode_catalog_payload, CatalogRecordWire};
    use crate::error::FormatError;
    use crate::file_format::{FileHeader, FILE_HEADER_SIZE};
    use crate::index::IndexState;
    use crate::index::{encode_index_payload, IndexEntry};
    use crate::schema::{FieldDef, Type};
    use crate::segments::header::{SegmentHeader, SegmentType};
    use crate::segments::writer::SegmentWriter;
    use crate::storage::{FileStore, Store};
    use crate::superblock::{Superblock, SUPERBLOCK_SIZE};
    use crate::{DbError, RowValue, ScalarValue};
    use crate::MigrationStep;
    use std::borrow::Cow;
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    fn new_store() -> FileStore {
        let f = tempfile::NamedTempFile::new().unwrap();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(f.path())
            .unwrap();
        FileStore::new(file)
    }

    fn path_field(name: &str) -> FieldDef {
        FieldDef {
            path: crate::schema::FieldPath(vec![Cow::Owned(name.to_string())]),
            ty: Type::String,
            constraints: vec![],
        }
    }

    fn nested_field(parts: &[&str], ty: Type) -> FieldDef {
        FieldDef {
            path: crate::schema::FieldPath(parts.iter().map(|s| Cow::Owned(s.to_string())).collect()),
            ty,
            constraints: vec![],
        }
    }

    fn base_vecstore_v5_with_superblock(sb: Superblock) -> crate::storage::VecStore {
        use crate::storage::VecStore;
        let mut store = VecStore::new();
        store
            .write_all_at(0, &FileHeader::new_v0_5().encode())
            .unwrap();
        let segment_start = (FILE_HEADER_SIZE + 2 * SUPERBLOCK_SIZE) as u64;
        store.write_all_at(segment_start - 1, &[0u8]).unwrap();
        store
            .write_all_at(FILE_HEADER_SIZE as u64, &sb.encode())
            .unwrap();
        store
            .write_all_at(
                (FILE_HEADER_SIZE + SUPERBLOCK_SIZE) as u64,
                &Superblock::empty().encode(),
            )
            .unwrap();
        store
    }

    #[test]
    fn read_and_select_superblock_errors_when_both_invalid() {
        let mut store = new_store();
        store
            .write_all_at(0, &FileHeader::new_v0_3().encode())
            .unwrap();
        let segment_start = (FILE_HEADER_SIZE + 2 * SUPERBLOCK_SIZE) as u64;
        store.write_all_at(segment_start - 1, &[0u8]).unwrap();

        let mut a = Superblock::empty().encode();
        let mut b = Superblock::empty().encode();
        a[0] ^= 0xff;
        b[0] ^= 0xff;
        store.write_all_at(FILE_HEADER_SIZE as u64, &a).unwrap();
        store
            .write_all_at((FILE_HEADER_SIZE + SUPERBLOCK_SIZE) as u64, &b)
            .unwrap();

        let res = open::read_and_select_superblock(&mut store);
        assert!(matches!(
            res,
            Err(DbError::Format(FormatError::BadSuperblockChecksum))
        ));
    }

    #[test]
    fn read_manifest_rejects_wrong_segment_type() {
        let mut store = new_store();
        store
            .write_all_at(0, &FileHeader::new_v0_3().encode())
            .unwrap();
        let segment_start = (FILE_HEADER_SIZE + 2 * SUPERBLOCK_SIZE) as u64;
        store.write_all_at(segment_start - 1, &[0u8]).unwrap();

        let sb_a = Superblock {
            generation: 1,
            ..Superblock::empty()
        };
        store
            .write_all_at(FILE_HEADER_SIZE as u64, &sb_a.encode())
            .unwrap();
        store
            .write_all_at(
                (FILE_HEADER_SIZE + SUPERBLOCK_SIZE) as u64,
                &Superblock::empty().encode(),
            )
            .unwrap();

        let mut w = SegmentWriter::new(&mut store, segment_start);
        let off = w
            .append(
                SegmentHeader {
                    segment_type: SegmentType::Schema,
                    payload_len: 0,
                    payload_crc32c: 0,
                },
                b"hi",
            )
            .unwrap();

        let sb = Superblock {
            manifest_offset: off,
            manifest_len: 2,
            ..sb_a
        };
        let res = open::read_manifest(&mut store, &sb);
        assert!(matches!(
            res,
            Err(DbError::Format(FormatError::UnsupportedVersion { .. }))
        ));
    }

    #[test]
    fn read_and_select_superblock_prefers_a_when_generation_higher() {
        let mut store = new_store();
        store
            .write_all_at(0, &FileHeader::new_v0_3().encode())
            .unwrap();
        let segment_start = (FILE_HEADER_SIZE + 2 * SUPERBLOCK_SIZE) as u64;
        store.write_all_at(segment_start - 1, &[0u8]).unwrap();

        let sb_a = Superblock {
            generation: 10,
            ..Superblock::empty()
        };
        let sb_b = Superblock {
            generation: 9,
            ..Superblock::empty()
        };
        store
            .write_all_at(FILE_HEADER_SIZE as u64, &sb_a.encode())
            .unwrap();
        store
            .write_all_at((FILE_HEADER_SIZE + SUPERBLOCK_SIZE) as u64, &sb_b.encode())
            .unwrap();

        let selected = open::read_and_select_superblock(&mut store).unwrap();
        assert_eq!(selected.generation, sb_a.generation);
    }

    #[test]
    fn open_strict_errors_on_manifest_read_failure_but_autotruncate_opens() {
        use crate::config::{OpenOptions, RecoveryMode};
        use crate::segments::header::SEGMENT_HEADER_LEN;
        use crate::storage::VecStore;

        let segment_start = (FILE_HEADER_SIZE + 2 * SUPERBLOCK_SIZE) as u64;

        // Build an on-disk image with a valid header + superblocks but a bad manifest pointer:
        // manifest_offset points at a Schema segment, so `read_manifest` fails.
        let mut store = VecStore::new();
        store
            .write_all_at(0, &FileHeader::new_v0_5().encode())
            .unwrap();
        store.write_all_at(segment_start - 1, &[0u8]).unwrap();

        // Write a manifest segment with an invalid payload so `read_manifest` fails during decode,
        // but replay ignores manifest segments.
        store
            .write_all_at(
                segment_start,
                &SegmentHeader {
                    segment_type: SegmentType::Manifest,
                    payload_len: 0,
                    payload_crc32c: 0,
                }
                .encode(),
            )
            .unwrap();
        store
            .write_all_at(segment_start + SEGMENT_HEADER_LEN as u64, b"hi")
            .unwrap();

        let sb_a = Superblock {
            generation: 10,
            manifest_offset: segment_start,
            manifest_len: 2,
            ..Superblock::empty()
        };
        store
            .write_all_at(FILE_HEADER_SIZE as u64, &sb_a.encode())
            .unwrap();
        store
            .write_all_at(
                (FILE_HEADER_SIZE + SUPERBLOCK_SIZE) as u64,
                &Superblock::empty().encode(),
            )
            .unwrap();

        let strict = OpenOptions {
            recovery: RecoveryMode::Strict,
            ..OpenOptions::default()
        };
        let store_strict = VecStore::from_vec(store.as_slice().to_vec());
        let err = match open::open_with_store(PathBuf::from("x.typra"), store_strict, strict) {
            Ok(_) => panic!("expected strict open to fail"),
            Err(e) => e,
        };
        assert!(matches!(err, DbError::Format(_)));

        let auto = OpenOptions {
            recovery: RecoveryMode::AutoTruncate,
            ..OpenOptions::default()
        };
        let _ = open::open_with_store(PathBuf::from("x.typra"), store, auto).unwrap();
    }

    #[test]
    fn open_rejects_minor2_header_when_file_has_more_than_header_bytes() {
        use crate::storage::VecStore;
        let mut store = VecStore::new();
        let mut hdr = FileHeader::new_v0_3().encode();
        hdr[6..8].copy_from_slice(&2u16.to_le_bytes());
        store.write_all_at(0, &hdr).unwrap();
        store.write_all_at(FILE_HEADER_SIZE as u64, &[1]).unwrap();

        let opts = crate::config::OpenOptions::default();
        let err = match open::open_with_store(PathBuf::from("x.typra"), store, opts) {
            Ok(_) => panic!("expected open to fail"),
            Err(e) => e,
        };
        assert!(matches!(
            err,
            DbError::Format(FormatError::UnsupportedVersion { .. })
        ));
    }

    #[test]
    fn open_checkpoint_none_path_when_superblock_has_no_checkpoint_pointer() {
        let segment_start = (FILE_HEADER_SIZE + 2 * SUPERBLOCK_SIZE) as u64;
        let sb = Superblock {
            generation: 10,
            checkpoint_offset: 0,
            checkpoint_len: 0,
            ..Superblock::empty()
        };
        let store = base_vecstore_v5_with_superblock(sb);
        let opts = crate::config::OpenOptions::default();
        let db = open::open_with_store(PathBuf::from("x.typra"), store, opts).unwrap();
        assert_eq!(db.segment_start, segment_start);
        assert!(db.catalog().is_empty());
    }

    #[test]
    fn open_checkpoint_none_path_when_checkpoint_offset_is_out_of_range() {
        let segment_start = (FILE_HEADER_SIZE + 2 * SUPERBLOCK_SIZE) as u64;
        let sb = Superblock {
            generation: 10,
            checkpoint_offset: segment_start + 123, // beyond file len
            checkpoint_len: 1,
            ..Superblock::empty()
        };
        let store = base_vecstore_v5_with_superblock(sb);
        let opts = crate::config::OpenOptions::default();
        let _ = open::open_with_store(PathBuf::from("x.typra"), store, opts).unwrap();
    }

    #[test]
    fn open_checkpoint_none_path_when_checkpoint_segment_type_is_wrong() {
        use crate::segments::header::SEGMENT_HEADER_LEN;

        let segment_start = (FILE_HEADER_SIZE + 2 * SUPERBLOCK_SIZE) as u64;
        let checkpoint_off = segment_start;
        let sb = Superblock {
            generation: 10,
            checkpoint_offset: checkpoint_off,
            checkpoint_len: 1,
            ..Superblock::empty()
        };
        let mut store = base_vecstore_v5_with_superblock(sb);
        store
            .write_all_at(
                checkpoint_off,
                &SegmentHeader {
                    segment_type: SegmentType::Temp,
                    payload_len: 0,
                    payload_crc32c: 0,
                }
                .encode(),
            )
            .unwrap();
        store
            .write_all_at(checkpoint_off + SEGMENT_HEADER_LEN as u64, &[])
            .unwrap();

        let opts = crate::config::OpenOptions::default();
        let _ = open::open_with_store(PathBuf::from("x.typra"), store, opts).unwrap();
    }

    #[test]
    fn open_strict_errors_on_bad_checkpoint_crc_but_autotruncate_falls_back() {
        use crate::config::{OpenOptions, RecoveryMode};
        use crate::segments::header::SEGMENT_HEADER_LEN;
        use crate::storage::VecStore;

        let segment_start = (FILE_HEADER_SIZE + 2 * SUPERBLOCK_SIZE) as u64;
        let checkpoint_off = segment_start;
        let sb = Superblock {
            generation: 10,
            checkpoint_offset: checkpoint_off,
            checkpoint_len: 1,
            ..Superblock::empty()
        };
        let mut store = base_vecstore_v5_with_superblock(sb);

        // Write a checkpoint header with incorrect payload CRC (payload "a" has nonzero crc).
        let hdr = SegmentHeader {
            segment_type: SegmentType::Checkpoint,
            payload_len: 1,
            payload_crc32c: 0,
        }
        .encode();
        store.write_all_at(checkpoint_off, &hdr).unwrap();
        store
            .write_all_at(checkpoint_off + SEGMENT_HEADER_LEN as u64, b"a")
            .unwrap();

        let strict = OpenOptions {
            recovery: RecoveryMode::Strict,
            ..OpenOptions::default()
        };
        assert!(open::open_with_store(PathBuf::from("x.typra"), VecStore::from_vec(store.as_slice().to_vec()), strict).is_err());

        let auto = OpenOptions {
            recovery: RecoveryMode::AutoTruncate,
            ..OpenOptions::default()
        };
        let _ = open::open_with_store(PathBuf::from("x.typra"), store, auto).unwrap();
    }

    #[test]
    fn open_applies_tail_segments_after_checkpoint_when_replay_from_is_after_checkpoint() {
        use crate::checkpoint::{checkpoint_from_state, encode_checkpoint_payload_v0, CheckpointV0};
        use crate::segments::header::SEGMENT_HEADER_LEN;

        let segment_start = (FILE_HEADER_SIZE + 2 * SUPERBLOCK_SIZE) as u64;
        let checkpoint_off = segment_start;

        // Build minimal logical state: one collection, no rows.
        let mut catalog = crate::catalog::Catalog::default();
        catalog
            .apply_record(CatalogRecordWire::CreateCollection {
                collection_id: 1,
                name: "t".to_string(),
                schema_version: 1,
                fields: vec![path_field("id")],
                indexes: vec![],
                primary_field: Some("id".to_string()),
            })
            .unwrap();
        let latest = super::LatestMap::new();
        let indexes = crate::index::IndexState::default();

        // Create a checkpoint payload and set replay_from_offset to *after* the checkpoint segment.
        let mut cp: CheckpointV0 = checkpoint_from_state(&catalog, &latest, &indexes).unwrap();
        let payload0 = encode_checkpoint_payload_v0(&cp);
        let checkpoint_end = checkpoint_off + SEGMENT_HEADER_LEN as u64 + payload0.len() as u64;
        cp.replay_from_offset = checkpoint_end;
        let payload = encode_checkpoint_payload_v0(&cp);

        let sb = Superblock {
            generation: 10,
            checkpoint_offset: checkpoint_off,
            checkpoint_len: payload.len() as u32,
            ..Superblock::empty()
        };
        let mut store = base_vecstore_v5_with_superblock(sb);

        // Write checkpoint segment with correct CRC via SegmentWriter.
        let mut w = SegmentWriter::new(&mut store, checkpoint_off);
        w.append(
            SegmentHeader {
                segment_type: SegmentType::Checkpoint,
                payload_len: 0,
                payload_crc32c: 0,
            },
            &payload,
        )
        .unwrap();

        // Append one schema segment after checkpoint; open should replay it via `replay_tail_into`.
        let schema_bytes = encode_catalog_payload(&CatalogRecordWire::NewSchemaVersion {
            collection_id: 1,
            schema_version: 2,
            fields: vec![path_field("id")],
            indexes: vec![],
        });
        let mut w2 = SegmentWriter::new(&mut store, checkpoint_end);
        w2.append(
            SegmentHeader {
                segment_type: SegmentType::Schema,
                payload_len: 0,
                payload_crc32c: 0,
            },
            &schema_bytes,
        )
        .unwrap();

        let opts = crate::config::OpenOptions::default();
        let db = open::open_with_store(PathBuf::from("x.typra"), store, opts).unwrap();
        let col = db.catalog().get(crate::schema::CollectionId(1)).unwrap();
        assert_eq!(col.current_version.0, 2);
    }

    #[test]
    fn open_upgrades_minor2_header_only_vecstore() {
        use crate::storage::VecStore;
        let mut store = VecStore::new();
        let mut hdr = FileHeader::new_v0_3().encode();
        hdr[6..8].copy_from_slice(&2u16.to_le_bytes());
        store.write_all_at(0, &hdr).unwrap();
        // len == FILE_HEADER_SIZE exactly

        let opts = crate::config::OpenOptions::default();
        let db = open::open_with_store(PathBuf::from("x.typra"), store, opts).unwrap();
        assert_eq!(db.format_minor, crate::file_format::FORMAT_MINOR_V3);
    }

    #[test]
    fn open_upgrades_minor2_header_only_filestore() {
        let mut store = new_store();
        let mut hdr = FileHeader::new_v0_3().encode();
        hdr[6..8].copy_from_slice(&2u16.to_le_bytes());
        store.write_all_at(0, &hdr).unwrap();

        let opts = crate::config::OpenOptions::default();
        let db = open::open_with_store(PathBuf::from("x.typra"), store, opts).unwrap();
        assert_eq!(db.format_minor, crate::file_format::FORMAT_MINOR_V3);
    }

    #[test]
    fn open_with_filestore_hits_tail_replay_branch() {
        // Same as `open_applies_tail_segments_after_checkpoint_when_replay_from_is_after_checkpoint`,
        // but with a FileStore monomorphization to satisfy llvm-cov branch accounting.
        use crate::checkpoint::{checkpoint_from_state, encode_checkpoint_payload_v0, CheckpointV0};
        use crate::segments::header::SEGMENT_HEADER_LEN;

        let segment_start = (FILE_HEADER_SIZE + 2 * SUPERBLOCK_SIZE) as u64;
        let checkpoint_off = segment_start;

        let mut catalog = crate::catalog::Catalog::default();
        catalog
            .apply_record(CatalogRecordWire::CreateCollection {
                collection_id: 1,
                name: "t".to_string(),
                schema_version: 1,
                fields: vec![path_field("id")],
                indexes: vec![],
                primary_field: Some("id".to_string()),
            })
            .unwrap();
        let latest = super::LatestMap::new();
        let indexes = crate::index::IndexState::default();

        let mut cp: CheckpointV0 = checkpoint_from_state(&catalog, &latest, &indexes).unwrap();
        let payload0 = encode_checkpoint_payload_v0(&cp);
        let checkpoint_end = checkpoint_off + SEGMENT_HEADER_LEN as u64 + payload0.len() as u64;
        cp.replay_from_offset = checkpoint_end;
        let payload = encode_checkpoint_payload_v0(&cp);

        let sb = Superblock {
            generation: 10,
            checkpoint_offset: checkpoint_off,
            checkpoint_len: payload.len() as u32,
            ..Superblock::empty()
        };

        let mut store = new_store();
        store
            .write_all_at(0, &FileHeader::new_v0_5().encode())
            .unwrap();
        store.write_all_at(segment_start - 1, &[0u8]).unwrap();
        store
            .write_all_at(FILE_HEADER_SIZE as u64, &sb.encode())
            .unwrap();
        store
            .write_all_at(
                (FILE_HEADER_SIZE + SUPERBLOCK_SIZE) as u64,
                &Superblock::empty().encode(),
            )
            .unwrap();

        let mut w = SegmentWriter::new(&mut store, checkpoint_off);
        w.append(
            SegmentHeader {
                segment_type: SegmentType::Checkpoint,
                payload_len: 0,
                payload_crc32c: 0,
            },
            &payload,
        )
        .unwrap();

        let schema_bytes = encode_catalog_payload(&CatalogRecordWire::NewSchemaVersion {
            collection_id: 1,
            schema_version: 2,
            fields: vec![path_field("id")],
            indexes: vec![],
        });
        let mut w2 = SegmentWriter::new(&mut store, checkpoint_end);
        w2.append(
            SegmentHeader {
                segment_type: SegmentType::Schema,
                payload_len: 0,
                payload_crc32c: 0,
            },
            &schema_bytes,
        )
        .unwrap();

        let opts = crate::config::OpenOptions::default();
        let db = open::open_with_store(PathBuf::from("x.typra"), store, opts).unwrap();
        let col = db.catalog().get(crate::schema::CollectionId(1)).unwrap();
        assert_eq!(col.current_version.0, 2);
    }

    #[test]
    fn open_checkpoint_replay_from_before_segment_start_skips_tail_replay() {
        use crate::checkpoint::{checkpoint_from_state, encode_checkpoint_payload_v0, CheckpointV0};
        use crate::segments::header::SEGMENT_HEADER_LEN;

        let segment_start = (FILE_HEADER_SIZE + 2 * SUPERBLOCK_SIZE) as u64;
        let checkpoint_off = segment_start;

        let mut catalog = crate::catalog::Catalog::default();
        catalog
            .apply_record(CatalogRecordWire::CreateCollection {
                collection_id: 1,
                name: "t".to_string(),
                schema_version: 1,
                fields: vec![path_field("id")],
                indexes: vec![],
                primary_field: Some("id".to_string()),
            })
            .unwrap();
        let latest = super::LatestMap::new();
        let indexes = crate::index::IndexState::default();

        let mut cp: CheckpointV0 = checkpoint_from_state(&catalog, &latest, &indexes).unwrap();
        cp.replay_from_offset = 0; // < segment_start, so open.rs should skip tail replay
        let payload = encode_checkpoint_payload_v0(&cp);

        let sb = Superblock {
            generation: 10,
            checkpoint_offset: checkpoint_off,
            checkpoint_len: payload.len() as u32,
            ..Superblock::empty()
        };
        let mut store = base_vecstore_v5_with_superblock(sb);
        let mut w = SegmentWriter::new(&mut store, checkpoint_off);
        w.append(
            SegmentHeader {
                segment_type: SegmentType::Checkpoint,
                payload_len: 0,
                payload_crc32c: 0,
            },
            &payload,
        )
        .unwrap();

        // Add a schema segment after checkpoint; it should NOT be applied because replay_from is 0.
        let schema_bytes = encode_catalog_payload(&CatalogRecordWire::NewSchemaVersion {
            collection_id: 1,
            schema_version: 2,
            fields: vec![path_field("id")],
            indexes: vec![],
        });
        let checkpoint_end = checkpoint_off + SEGMENT_HEADER_LEN as u64 + payload.len() as u64;
        let mut w2 = SegmentWriter::new(&mut store, checkpoint_end);
        w2.append(
            SegmentHeader {
                segment_type: SegmentType::Schema,
                payload_len: 0,
                payload_crc32c: 0,
            },
            &schema_bytes,
        )
        .unwrap();

        let opts = crate::config::OpenOptions::default();
        let db = open::open_with_store(PathBuf::from("x.typra"), store, opts).unwrap();
        let col = db.catalog().get(crate::schema::CollectionId(1)).unwrap();
        assert_eq!(col.current_version.0, 1);
    }

    #[test]
    fn open_checkpoint_replay_from_beyond_eof_skips_tail_replay() {
        use crate::checkpoint::{checkpoint_from_state, encode_checkpoint_payload_v0, CheckpointV0};

        let segment_start = (FILE_HEADER_SIZE + 2 * SUPERBLOCK_SIZE) as u64;
        let checkpoint_off = segment_start;

        let mut catalog = crate::catalog::Catalog::default();
        catalog
            .apply_record(CatalogRecordWire::CreateCollection {
                collection_id: 1,
                name: "t".to_string(),
                schema_version: 1,
                fields: vec![path_field("id")],
                indexes: vec![],
                primary_field: Some("id".to_string()),
            })
            .unwrap();
        let latest = super::LatestMap::new();
        let indexes = crate::index::IndexState::default();

        let mut cp: CheckpointV0 = checkpoint_from_state(&catalog, &latest, &indexes).unwrap();
        cp.replay_from_offset = u64::MAX; // >= cur_len, so open.rs should skip tail replay
        let payload = encode_checkpoint_payload_v0(&cp);

        let sb = Superblock {
            generation: 10,
            checkpoint_offset: checkpoint_off,
            checkpoint_len: payload.len() as u32,
            ..Superblock::empty()
        };
        let mut store = base_vecstore_v5_with_superblock(sb);
        let mut w = SegmentWriter::new(&mut store, checkpoint_off);
        w.append(
            SegmentHeader {
                segment_type: SegmentType::Checkpoint,
                payload_len: 0,
                payload_crc32c: 0,
            },
            &payload,
        )
        .unwrap();

        let opts = crate::config::OpenOptions::default();
        let _ = open::open_with_store(PathBuf::from("x.typra"), store, opts).unwrap();
    }

    #[test]
    fn open_errors_on_truncated_superblocks_when_len_is_less_than_segment_start() {
        use crate::storage::VecStore;
        let mut store = VecStore::new();
        store
            .write_all_at(0, &FileHeader::new_v0_3().encode())
            .unwrap();
        // len == FILE_HEADER_SIZE, but segment_start is larger => should error.
        let opts = crate::config::OpenOptions::default();
        let err = match open::open_with_store(PathBuf::from("x.typra"), store, opts) {
            Ok(_) => panic!("expected open to fail"),
            Err(e) => e,
        };
        assert!(matches!(
            err,
            DbError::Format(FormatError::TruncatedSuperblock { .. })
        ));
    }

    #[test]
    fn open_strict_reports_unclean_log_tail_when_recovery_finds_torn_tail() {
        use crate::config::{OpenOptions, RecoveryMode};

        let segment_start = (FILE_HEADER_SIZE + 2 * SUPERBLOCK_SIZE) as u64;
        let sb = Superblock {
            generation: 10,
            ..Superblock::empty()
        };
        let mut store = base_vecstore_v5_with_superblock(sb);

        // Append a committed empty transaction and then trailing garbage bytes to force
        // truncate_end_for_recovery to return (safe_end < file_len, Some("torn_tail")).
        let mut w = SegmentWriter::new(&mut store, segment_start);
        let begin = crate::txn::encode_txn_payload_v0(1);
        let commit = crate::txn::encode_txn_payload_v0(1);
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
        let end = store.len().unwrap();
        store.write_all_at(end, &[0xEE]).unwrap();

        let strict = OpenOptions {
            recovery: RecoveryMode::Strict,
            ..OpenOptions::default()
        };
        let err = match open::open_with_store(PathBuf::from("x.typra"), store, strict) {
            Ok(_) => panic!("expected strict open to fail"),
            Err(e) => e,
        };
        assert!(matches!(err, DbError::Format(FormatError::UncleanLogTail { .. })));
    }

    #[test]
    fn open_allows_manifest_offset_zero() {
        let sb = Superblock {
            generation: 10,
            manifest_offset: 0,
            manifest_len: 0,
            ..Superblock::empty()
        };
        let store = base_vecstore_v5_with_superblock(sb);
        let opts = crate::config::OpenOptions::default();
        let _ = open::open_with_store(PathBuf::from("x.typra"), store, opts).unwrap();
    }

    #[test]
    fn open_allows_manifest_offset_zero_with_filestore() {
        let segment_start = (FILE_HEADER_SIZE + 2 * SUPERBLOCK_SIZE) as u64;
        let sb = Superblock {
            generation: 10,
            manifest_offset: 0,
            manifest_len: 0,
            ..Superblock::empty()
        };

        let mut store = new_store();
        store
            .write_all_at(0, &FileHeader::new_v0_5().encode())
            .unwrap();
        store.write_all_at(segment_start - 1, &[0u8]).unwrap();
        store
            .write_all_at(FILE_HEADER_SIZE as u64, &sb.encode())
            .unwrap();
        store
            .write_all_at(
                (FILE_HEADER_SIZE + SUPERBLOCK_SIZE) as u64,
                &Superblock::empty().encode(),
            )
            .unwrap();

        let opts = crate::config::OpenOptions::default();
        let _ = open::open_with_store(PathBuf::from("x.typra"), store, opts).unwrap();
    }

    #[test]
    fn open_strict_reports_unclean_log_tail_with_filestore() {
        use crate::config::{OpenOptions, RecoveryMode};

        let segment_start = (FILE_HEADER_SIZE + 2 * SUPERBLOCK_SIZE) as u64;
        let sb = Superblock {
            generation: 10,
            ..Superblock::empty()
        };

        let mut store = new_store();
        store
            .write_all_at(0, &FileHeader::new_v0_5().encode())
            .unwrap();
        store.write_all_at(segment_start - 1, &[0u8]).unwrap();
        store
            .write_all_at(FILE_HEADER_SIZE as u64, &sb.encode())
            .unwrap();
        store
            .write_all_at(
                (FILE_HEADER_SIZE + SUPERBLOCK_SIZE) as u64,
                &Superblock::empty().encode(),
            )
            .unwrap();

        // Force torn tail: write a committed empty txn then trailing garbage.
        let mut w = SegmentWriter::new(&mut store, segment_start);
        let begin = crate::txn::encode_txn_payload_v0(1);
        let commit = crate::txn::encode_txn_payload_v0(1);
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
        let end = store.len().unwrap();
        store.write_all_at(end, &[0xEE]).unwrap();

        let strict = OpenOptions {
            recovery: RecoveryMode::Strict,
            ..OpenOptions::default()
        };
        let err = match open::open_with_store(PathBuf::from("x.typra"), store, strict) {
            Ok(_) => panic!("expected strict open to fail"),
            Err(e) => e,
        };
        assert!(matches!(err, DbError::Format(FormatError::UncleanLogTail { .. })));
    }

    #[test]
    fn plan_insert_row_errors_on_unknown_collection() {
        let catalog = crate::catalog::Catalog::default();
        let mut row = BTreeMap::new();
        row.insert("id".to_string(), crate::RowValue::String("x".to_string()));
        let err = super::plan_insert_row(&catalog, crate::schema::CollectionId(1), row).unwrap_err();
        assert!(matches!(err, DbError::Schema(crate::error::SchemaError::UnknownCollection { .. })));
    }

    #[test]
    fn plan_insert_row_errors_when_collection_has_no_primary_key() {
        let mut catalog = crate::catalog::Catalog::default();
        catalog
            .apply_record(CatalogRecordWire::CreateCollection {
                collection_id: 1,
                name: "t".to_string(),
                schema_version: 1,
                fields: vec![path_field("id")],
                indexes: vec![],
                primary_field: None,
            })
            .unwrap();

        let mut row = BTreeMap::new();
        row.insert("id".to_string(), crate::RowValue::String("x".to_string()));
        let err = super::plan_insert_row(&catalog, crate::schema::CollectionId(1), row).unwrap_err();
        assert!(matches!(err, DbError::Schema(crate::error::SchemaError::NoPrimaryKey { .. })));
    }

    #[test]
    fn plan_insert_row_multiseg_rejects_unknown_leaf_path() {
        let mut catalog = crate::catalog::Catalog::default();
        catalog
            .apply_record(CatalogRecordWire::CreateCollection {
                collection_id: 1,
                name: "t".to_string(),
                schema_version: 1,
                fields: vec![
                    nested_field(&["id"], Type::String),
                    nested_field(&["a", "b"], Type::String),
                ],
                indexes: vec![],
                primary_field: Some("id".to_string()),
            })
            .unwrap();

        let mut row = BTreeMap::new();
        row.insert("id".to_string(), crate::RowValue::String("x".to_string()));
        row.insert(
            "a".to_string(),
            crate::RowValue::Object(BTreeMap::from([(
                "c".to_string(),
                crate::RowValue::String("nope".to_string()),
            )])),
        );
        let err = super::plan_insert_row(&catalog, crate::schema::CollectionId(1), row).unwrap_err();
        assert!(matches!(
            err,
            DbError::Schema(crate::error::SchemaError::RowUnknownField { .. })
        ));
    }

    #[test]
    fn plan_insert_row_multiseg_errors_on_missing_required_nested_field() {
        let mut catalog = crate::catalog::Catalog::default();
        catalog
            .apply_record(CatalogRecordWire::CreateCollection {
                collection_id: 1,
                name: "t".to_string(),
                schema_version: 1,
                fields: vec![
                    nested_field(&["id"], Type::String),
                    nested_field(&["a", "b"], Type::String),
                ],
                indexes: vec![],
                primary_field: Some("id".to_string()),
            })
            .unwrap();

        let mut row = BTreeMap::new();
        row.insert("id".to_string(), crate::RowValue::String("x".to_string()));
        row.insert("a".to_string(), crate::RowValue::Object(BTreeMap::new()));
        let err = super::plan_insert_row(&catalog, crate::schema::CollectionId(1), row).unwrap_err();
        assert!(matches!(
            err,
            DbError::Schema(crate::error::SchemaError::RowMissingField { .. })
        ));
    }

    #[test]
    fn plan_insert_row_multiseg_missing_optional_field_becomes_none() {
        let mut catalog = crate::catalog::Catalog::default();
        catalog
            .apply_record(CatalogRecordWire::CreateCollection {
                collection_id: 1,
                name: "t".to_string(),
                schema_version: 1,
                fields: vec![
                    nested_field(&["id"], Type::String),
                    nested_field(&["a", "b"], Type::Optional(Box::new(Type::String))),
                ],
                indexes: vec![],
                primary_field: Some("id".to_string()),
            })
            .unwrap();

        let mut row = BTreeMap::new();
        row.insert("id".to_string(), crate::RowValue::String("x".to_string()));
        row.insert("a".to_string(), crate::RowValue::Object(BTreeMap::new()));
        let (_payload, (_pk, full), _idx, _pk_scalar) =
            super::plan_insert_row(&catalog, crate::schema::CollectionId(1), row).unwrap();
        let a = full.get("a").unwrap();
        assert!(matches!(a, crate::RowValue::Object(_)));
    }

    #[test]
    fn plan_schema_version_needs_migration_new_required_field_suggests_backfill_step() {
        use crate::schema::{FieldPath, Type};

        let mut db = Database::open_in_memory().unwrap();
        let (cid, _v) = db
            .register_collection(
                "t",
                vec![nested_field(&["id"], Type::String)],
                "id",
            )
            .unwrap();

        let plan = db
            .plan_schema_version_with_indexes(
                cid,
                vec![
                    nested_field(&["id"], Type::String),
                    // Add a new required field: should be NeedsMigration w/ "new required field".
                    FieldDef {
                        path: FieldPath(vec![Cow::Owned("x".to_string())]),
                        ty: Type::String,
                        constraints: vec![],
                    },
                ],
                vec![],
            )
            .unwrap();

        assert!(matches!(
            plan.steps.as_slice(),
            [MigrationStep::BackfillTopLevelField { .. }]
        ));
    }

    #[test]
    fn plan_schema_version_needs_migration_other_reason_suggests_rebuild_indexes() {
        use crate::schema::{FieldPath, IndexDef, IndexKind, Type};

        let mut db = Database::open_in_memory().unwrap();
        let fields = vec![
            nested_field(&["id"], Type::String),
            nested_field(&["v"], Type::String),
        ];
        let (cid, _v) = db.register_collection("t", fields.clone(), "id").unwrap();

        // Add a unique index: classify_schema_update flags this as NeedsMigration.
        let plan = db
            .plan_schema_version_with_indexes(
                cid,
                fields,
                vec![IndexDef {
                    name: "v_u".to_string(),
                    path: FieldPath(vec![Cow::Owned("v".to_string())]),
                    kind: IndexKind::Unique,
                }],
            )
            .unwrap();

        assert!(plan
            .steps
            .iter()
            .any(|s| matches!(s, MigrationStep::RebuildIndexes)));
    }

    #[test]
    fn backfill_top_level_field_skips_existing_and_inserts_missing() {
        use crate::schema::{FieldPath, Type};
        use crate::{RowValue, ScalarValue};

        let mut db = Database::open_in_memory().unwrap();
        let (cid, _v1) = db
            .register_collection(
                "t",
                vec![
                    nested_field(&["id"], Type::String),
                    FieldDef {
                        path: FieldPath(vec![Cow::Owned("x".to_string())]),
                        ty: Type::Optional(Box::new(Type::Int64)),
                        constraints: vec![],
                    },
                ],
                "id",
            )
            .unwrap();

        // Row1: missing x (but insert will materialize optional fields as None).
        db.insert(
            cid,
            BTreeMap::from([("id".to_string(), RowValue::String("a".to_string()))]),
        )
        .unwrap();
        // Row2: already has x
        db.insert(
            cid,
            BTreeMap::from([
                ("id".to_string(), RowValue::String("b".to_string())),
                ("x".to_string(), RowValue::Int64(5)),
            ]),
        )
        .unwrap();

        // Remove `x` key entirely to simulate a legacy row missing the field.
        let target_key = db
            .latest
            .iter()
            .find_map(|((c, pk), row)| {
                if *c != cid.0 {
                    return None;
                }
                match row.get("id") {
                    Some(RowValue::String(s)) if s == "a" => Some(pk.clone()),
                    _ => None,
                }
            })
            .unwrap();
        db.latest
            .get_mut(&(cid.0, target_key))
            .unwrap()
            .remove("x");

        db.backfill_top_level_field_with_value(cid, "x", RowValue::Int64(9))
            .unwrap();

        let a = db
            .get(cid, &ScalarValue::String("a".to_string()))
            .unwrap()
            .unwrap();
        assert_eq!(a.get("x"), Some(&RowValue::Int64(9)));

        let b = db
            .get(cid, &ScalarValue::String("b".to_string()))
            .unwrap()
            .unwrap();
        assert_eq!(b.get("x"), Some(&RowValue::Int64(5)));
    }

    #[test]
    fn rebuild_indexes_early_returns_when_no_entries() {
        let mut db = Database::open_in_memory().unwrap();
        let (cid, _v) = db.register_collection("t", vec![path_field("id")], "id").unwrap();
        // No indexes, no rows => entries empty => Ok.
        db.rebuild_indexes_for_collection(cid).unwrap();
    }

    #[test]
    fn rebuild_indexes_builds_entries_and_persists_in_index_state() {
        use crate::schema::{FieldPath, IndexDef, IndexKind};
        use crate::{RowValue, ScalarValue};

        let mut db = Database::open_in_memory().unwrap();
        let fields = vec![path_field("id"), path_field("v")];
        let indexes = vec![IndexDef {
            name: "v_idx".to_string(),
            path: FieldPath(vec![Cow::Owned("v".to_string())]),
            kind: IndexKind::NonUnique,
        }];
        let (cid, _v) = db
            .register_collection_with_indexes("t", fields, indexes, "id")
            .unwrap();
        db.insert(
            cid,
            BTreeMap::from([
                ("id".to_string(), RowValue::String("a".to_string())),
                ("v".to_string(), RowValue::String("k".to_string())),
            ]),
        )
        .unwrap();
        db.insert(
            cid,
            BTreeMap::from([
                ("id".to_string(), RowValue::String("b".to_string())),
                ("v".to_string(), RowValue::String("k".to_string())),
            ]),
        )
        .unwrap();

        // Clear index state to prove rebuild repopulates it.
        db.indexes = IndexState::default();
        db.rebuild_indexes_for_collection(cid).unwrap();

        let got = db
            .index_state()
            .non_unique_lookup(cid.0, "v_idx", b"k")
            .unwrap();
        assert_eq!(got.len(), 2);

        // Also sanity-check records are still present.
        assert!(db
            .get(cid, &ScalarValue::String("a".to_string()))
            .unwrap()
            .is_some());
    }

    #[test]
    fn restore_snapshot_errors_when_dest_has_no_parent() {
        let dir = tempfile::tempdir().unwrap();
        let snap = dir.path().join("snap.typra");
        std::fs::write(&snap, b"hello").unwrap();

        // Use filesystem root which has no parent.
        let err = Database::<FileStore>::restore_snapshot_to_path(&snap, PathBuf::from("/"))
            .unwrap_err();
        assert!(matches!(err, DbError::Io(_)));
    }

    #[test]
    fn restore_snapshot_replaces_destination_and_cleans_backup() {
        let dir = tempfile::tempdir().unwrap();
        let snap = dir.path().join("snap.typra");
        let dest = dir.path().join("dest.typra");

        std::fs::write(&snap, b"new").unwrap();
        std::fs::write(&dest, b"old").unwrap();

        Database::<FileStore>::restore_snapshot_to_path(&snap, &dest).unwrap();
        let got = std::fs::read(&dest).unwrap();
        assert_eq!(got, b"new");
    }

    #[test]
    fn register_and_reopen_roundtrip() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.typra");
        {
            let mut db = Database::open(&path).unwrap();
            assert!(db.catalog().is_empty());
            let (id, v) = db
                .register_collection("books", vec![path_field("title")], "title")
                .unwrap();
            assert_eq!(id.0, 1);
            assert_eq!(v.0, 1);
        }
        let db = Database::open(&path).unwrap();
        assert_eq!(db.collection_names(), vec!["books".to_string()]);
        let c = db.catalog().get(crate::schema::CollectionId(1)).unwrap();
        assert_eq!(c.name, "books");
        assert_eq!(c.fields.len(), 1);
    }

    #[test]
    fn index_segment_replay_builds_index_state_on_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.typra");
        {
            let mut db = Database::open(&path).unwrap();
            let (id, _v) = db
                .register_collection("books", vec![path_field("title")], "title")
                .unwrap();
            let payload = encode_index_payload(&[IndexEntry {
                collection_id: id.0,
                index_name: "title_idx".to_string(),
                kind: crate::schema::IndexKind::NonUnique,
                op: crate::index::IndexOp::Insert,
                index_key: b"Hello".to_vec(),
                pk_key: b"Hello".to_vec(),
            }]);
            write::commit_write_txn_v6(
                &mut db.store,
                db.segment_start,
                &mut db.format_minor,
                2,
                &[(
                    crate::segments::header::SegmentType::Index,
                    payload.as_slice(),
                )],
            )
            .unwrap();
        }
        let db = Database::open(&path).unwrap();
        let got = db
            .index_state()
            .non_unique_lookup(1, "title_idx", b"Hello")
            .unwrap();
        assert_eq!(got, vec![b"Hello".to_vec()]);
    }

    #[test]
    fn insert_errors_on_unique_index_violation() {
        use crate::schema::{FieldPath, IndexDef, IndexKind};
        use crate::{RowValue, ScalarValue};

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.typra");
        let mut db = Database::open(&path).unwrap();

        let indexes = vec![IndexDef {
            name: "x_uq".to_string(),
            path: FieldPath(vec![Cow::Owned("x".to_string())]),
            kind: IndexKind::Unique,
        }];
        let (cid, _) = db
            .register_collection_with_indexes(
                "t",
                vec![path_field("id"), path_field("x")],
                indexes,
                "id",
            )
            .unwrap();

        db.insert(cid, {
            let mut m = BTreeMap::new();
            m.insert("id".to_string(), RowValue::String("a".to_string()));
            m.insert("x".to_string(), RowValue::String("v".to_string()));
            m
        })
        .unwrap();

        let err = db
            .insert(cid, {
                let mut m = BTreeMap::new();
                m.insert("id".to_string(), RowValue::String("b".to_string()));
                m.insert("x".to_string(), RowValue::String("v".to_string()));
                m
            })
            .unwrap_err();
        assert!(matches!(
            err,
            DbError::Schema(crate::error::SchemaError::UniqueIndexViolation)
        ));

        // The original row is still present.
        assert!(db
            .get(cid, &ScalarValue::String("a".to_string()))
            .unwrap()
            .is_some());
    }

    #[test]
    fn rebuild_indexes_skips_rows_missing_pk_wrong_pk_type_or_missing_index_value() {
        use crate::schema::{FieldPath, IndexDef, IndexKind};

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.typra");
        let mut db = Database::open(&path).unwrap();

        let indexes = vec![IndexDef {
            name: "x_idx".to_string(),
            path: FieldPath(vec![Cow::Owned("x".to_string())]),
            kind: IndexKind::NonUnique,
        }];
        let (cid, _) = db
            .register_collection_with_indexes(
                "t",
                vec![path_field("id"), path_field("x")],
                indexes,
                "id",
            )
            .unwrap();

        // Valid row (should contribute index entry).
        db.insert(cid, {
            let mut m = BTreeMap::new();
            m.insert("id".to_string(), RowValue::String("a".to_string()));
            m.insert("x".to_string(), RowValue::String("v".to_string()));
            m
        })
        .unwrap();

        // Missing PK: should be skipped.
        db.latest.insert(
            (cid.0, b"missing_pk".to_vec()),
            {
                let mut m = BTreeMap::new();
                m.insert("x".to_string(), RowValue::String("v".to_string()));
                m
            },
        );

        // Wrong PK type: schema says PK is String, but row stores Int64.
        db.latest.insert(
            (cid.0, b"wrong_pk_ty".to_vec()),
            {
                let mut m = BTreeMap::new();
                m.insert("id".to_string(), RowValue::Int64(1));
                m.insert("x".to_string(), RowValue::String("v".to_string()));
                m
            },
        );

        // Missing index value: should be skipped.
        db.latest.insert(
            (cid.0, b"missing_x".to_vec()),
            {
                let mut m = BTreeMap::new();
                m.insert("id".to_string(), RowValue::String("b".to_string()));
                m
            },
        );

        db.rebuild_indexes_for_collection(cid).unwrap();
        let got = db
            .index_state()
            .non_unique_lookup(cid.0, "x_idx", b"v")
            .unwrap();
        assert_eq!(got, vec![b"a".to_vec()]);
    }

    #[test]
    fn commit_transaction_is_noop_when_no_transaction_active() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.typra");
        let mut db = Database::open(&path).unwrap();
        db.commit_transaction().unwrap();
    }

    #[test]
    fn empty_transaction_commits_without_writing_segments() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.typra");
        let mut db = Database::open(&path).unwrap();
        db.begin_transaction().unwrap();
        db.commit_transaction().unwrap();
    }

    #[test]
    fn begin_transaction_rejects_nested_transaction() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.typra");
        let mut db = Database::open(&path).unwrap();
        db.begin_transaction().unwrap();
        let err = db.begin_transaction().unwrap_err();
        assert!(matches!(
            err,
            DbError::Transaction(crate::error::TransactionError::NestedTransaction)
        ));
    }

    #[test]
    fn transaction_rolls_back_on_error() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.typra");
        let mut db = Database::open(&path).unwrap();
        let (cid, _) = db
            .register_collection("t", vec![path_field("id")], "id")
            .unwrap();

        let err = db
            .transaction(|db| {
                db.insert(cid, {
                    let mut m = BTreeMap::new();
                    m.insert("id".to_string(), RowValue::String("a".to_string()));
                    m
                })?;
                Err::<(), DbError>(DbError::Format(FormatError::TruncatedRecordPayload))
            })
            .unwrap_err();
        assert!(matches!(
            err,
            DbError::Format(FormatError::TruncatedRecordPayload)
        ));

        // Rollback should have discarded the inserted row.
        assert!(db
            .get(cid, &ScalarValue::String("a".to_string()))
            .unwrap()
            .is_none());
    }

    #[test]
    fn insert_replace_updates_indexes_and_exercises_staging_path() {
        use crate::schema::{FieldPath, IndexDef, IndexKind};

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.typra");
        let mut db = Database::open(&path).unwrap();

        let indexes = vec![IndexDef {
            name: "x_idx".to_string(),
            path: FieldPath(vec![Cow::Owned("x".to_string())]),
            kind: IndexKind::NonUnique,
        }];
        let (cid, _) = db
            .register_collection_with_indexes(
                "t",
                vec![path_field("id"), path_field("x")],
                indexes,
                "id",
            )
            .unwrap();

        db.insert(cid, {
            let mut m = BTreeMap::new();
            m.insert("id".to_string(), RowValue::String("pk".to_string()));
            m.insert("x".to_string(), RowValue::String("a".to_string()));
            m
        })
        .unwrap();

        // Replace inside a transaction to hit the txn_staging branch.
        db.transaction(|db| {
            db.insert(cid, {
                let mut m = BTreeMap::new();
                m.insert("id".to_string(), RowValue::String("pk".to_string()));
                m.insert("x".to_string(), RowValue::String("b".to_string()));
                m
            })
        })
        .unwrap();

        // Old index key should be deleted, new key inserted.
        assert!(db.index_state().non_unique_lookup(cid.0, "x_idx", b"a").is_none());
        let got = db
            .index_state()
            .non_unique_lookup(cid.0, "x_idx", b"b")
            .unwrap();
        assert_eq!(got, vec![b"pk".to_vec()]);
    }

    #[test]
    fn insert_replace_unique_index_allows_same_pk_existing_entry() {
        use crate::schema::{FieldPath, IndexDef, IndexKind};

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.typra");
        let mut db = Database::open(&path).unwrap();

        let indexes = vec![IndexDef {
            name: "x_uq".to_string(),
            path: FieldPath(vec![Cow::Owned("x".to_string())]),
            kind: IndexKind::Unique,
        }];
        let (cid, _) = db
            .register_collection_with_indexes(
                "t",
                vec![path_field("id"), path_field("x")],
                indexes,
                "id",
            )
            .unwrap();

        db.insert(cid, {
            let mut m = BTreeMap::new();
            m.insert("id".to_string(), RowValue::String("pk".to_string()));
            m.insert("x".to_string(), RowValue::String("v".to_string()));
            m
        })
        .unwrap();

        // Replace the same row but keep the unique index key identical; should not violate.
        db.insert(cid, {
            let mut m = BTreeMap::new();
            m.insert("id".to_string(), RowValue::String("pk".to_string()));
            m.insert("x".to_string(), RowValue::String("v".to_string()));
            m
        })
        .unwrap();
    }

    #[test]
    fn delete_errors_on_pk_type_mismatch_and_is_noop_when_row_missing() {
        use crate::schema::{FieldPath, IndexDef, IndexKind};

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.typra");
        let mut db = Database::open(&path).unwrap();

        let indexes = vec![IndexDef {
            name: "x_idx".to_string(),
            path: FieldPath(vec![Cow::Owned("x".to_string())]),
            kind: IndexKind::NonUnique,
        }];
        let (cid, _) = db
            .register_collection_with_indexes(
                "t",
                vec![path_field("id"), path_field("x")],
                indexes,
                "id",
            )
            .unwrap();

        // Type mismatch for PK (schema: String).
        let err = db.delete(cid, &ScalarValue::Int64(1)).unwrap_err();
        assert!(matches!(
            err,
            DbError::Format(FormatError::RecordPayloadTypeMismatch)
        ));

        // Missing row: should be Ok(()).
        db.delete(cid, &ScalarValue::String("missing".to_string()))
            .unwrap();
    }

    #[test]
    fn query_uses_non_unique_index_for_equality_filter() {
        use crate::query::{Predicate, Query};
        use crate::schema::{FieldPath, IndexDef, IndexKind};
        use crate::{RowValue, ScalarValue};
        use std::collections::BTreeMap;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.typra");
        let mut db = Database::open(&path).unwrap();
        let mut year_def = path_field("year");
        year_def.ty = Type::Int64;
        let fields = vec![path_field("title"), year_def];
        let indexes = vec![IndexDef {
            name: "title_idx".to_string(),
            path: FieldPath(vec![std::borrow::Cow::Owned("title".to_string())]),
            kind: IndexKind::NonUnique,
        }];
        let (cid, _) = db
            .register_collection_with_indexes("books", fields, indexes, "title")
            .unwrap();
        db.insert(cid, {
            let mut m = BTreeMap::new();
            m.insert("title".to_string(), RowValue::String("Hello".to_string()));
            m.insert("year".to_string(), RowValue::Int64(2020));
            m
        })
        .unwrap();
        db.insert(cid, {
            let mut m = BTreeMap::new();
            m.insert("title".to_string(), RowValue::String("World".to_string()));
            m.insert("year".to_string(), RowValue::Int64(2021));
            m
        })
        .unwrap();

        let q = Query {
            collection: cid,
            predicate: Some(Predicate::Eq {
                path: FieldPath(vec![std::borrow::Cow::Owned("title".to_string())]),
                value: ScalarValue::String("Hello".to_string()),
            }),
            limit: None,
            order_by: None,
        };
        let explain = db.explain_query(&q).unwrap();
        assert!(explain.contains("IndexLookup"));
        let rows = db.query(&q).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get("year"), Some(&RowValue::Int64(2020)));
    }

    #[test]
    fn subset_model_projection_returns_only_declared_fields() {
        use crate::schema::{DbModel, FieldDef, FieldPath, Type};
        use crate::RowValue;
        use std::borrow::Cow;
        use std::collections::BTreeMap;

        #[allow(dead_code)]
        struct BookFull {
            title: String,
            year: i64,
        }

        #[allow(dead_code)]
        struct BookTitleOnly {
            title: String,
        }

        impl DbModel for BookFull {
            fn collection_name() -> &'static str {
                "books"
            }
            fn fields() -> Vec<FieldDef> {
                vec![
                    FieldDef {
                        path: FieldPath(vec![Cow::Borrowed("title")]),
                        ty: Type::String,
                        constraints: vec![],
                    },
                    FieldDef {
                        path: FieldPath(vec![Cow::Borrowed("year")]),
                        ty: Type::Int64,
                        constraints: vec![],
                    },
                ]
            }
            fn primary_field() -> &'static str {
                "title"
            }
        }

        impl DbModel for BookTitleOnly {
            fn collection_name() -> &'static str {
                "books"
            }
            fn fields() -> Vec<FieldDef> {
                vec![FieldDef {
                    path: FieldPath(vec![Cow::Borrowed("title")]),
                    ty: Type::String,
                    constraints: vec![],
                }]
            }
            fn primary_field() -> &'static str {
                "title"
            }
        }

        let mut db = Database::open_in_memory().unwrap();
        let (cid, _) = db.register_model::<BookFull>().unwrap();
        db.insert(cid, {
            let mut m = BTreeMap::new();
            m.insert("title".to_string(), RowValue::String("Hello".to_string()));
            m.insert("year".to_string(), RowValue::Int64(2020));
            m
        })
        .unwrap();

        let books = db.collection::<BookTitleOnly>().unwrap();
        let rows = books.all().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0],
            BTreeMap::from([("title".to_string(), RowValue::String("Hello".to_string()))])
        );
    }

    #[test]
    fn subset_model_validation_errors_for_pk_mismatch_unknown_field_and_type_mismatch() {
        use crate::schema::{DbModel, FieldDef, FieldPath, Type};
        use std::borrow::Cow;

        #[allow(dead_code)]
        struct FullOk;
        #[allow(dead_code)]
        struct WrongPrimary;
        #[allow(dead_code)]
        struct UnknownField;
        #[allow(dead_code)]
        struct TypeMismatch;

        impl DbModel for FullOk {
            fn collection_name() -> &'static str {
                "t"
            }
            fn fields() -> Vec<FieldDef> {
                vec![
                    FieldDef {
                        path: FieldPath(vec![Cow::Borrowed("id")]),
                        ty: Type::String,
                        constraints: vec![],
                    },
                    FieldDef {
                        path: FieldPath(vec![Cow::Borrowed("x")]),
                        ty: Type::Int64,
                        constraints: vec![],
                    },
                ]
            }
            fn primary_field() -> &'static str {
                "id"
            }
        }

        impl DbModel for WrongPrimary {
            fn collection_name() -> &'static str {
                "t"
            }
            fn fields() -> Vec<FieldDef> {
                FullOk::fields()
            }
            fn primary_field() -> &'static str {
                "other"
            }
        }

        impl DbModel for UnknownField {
            fn collection_name() -> &'static str {
                "t"
            }
            fn fields() -> Vec<FieldDef> {
                vec![FieldDef {
                    path: FieldPath(vec![Cow::Borrowed("missing")]),
                    ty: Type::String,
                    constraints: vec![],
                }]
            }
            fn primary_field() -> &'static str {
                "id"
            }
        }

        impl DbModel for TypeMismatch {
            fn collection_name() -> &'static str {
                "t"
            }
            fn fields() -> Vec<FieldDef> {
                vec![FieldDef {
                    path: FieldPath(vec![Cow::Borrowed("x")]),
                    ty: Type::String, // catalog has x:Int64
                    constraints: vec![],
                }]
            }
            fn primary_field() -> &'static str {
                "id"
            }
        }

        let mut db = Database::open_in_memory().unwrap();
        let _ = db.register_model::<FullOk>().unwrap();

        let err = match db.collection::<WrongPrimary>() {
            Ok(_) => panic!("expected error"),
            Err(e) => e,
        };
        assert!(matches!(
            err,
            DbError::Schema(crate::error::SchemaError::PrimaryFieldNotFound { .. })
        ));

        let err = match db.collection::<UnknownField>() {
            Ok(_) => panic!("expected error"),
            Err(e) => e,
        };
        assert!(matches!(
            err,
            DbError::Schema(crate::error::SchemaError::RowUnknownField { .. })
        ));

        let err = match db.collection::<TypeMismatch>() {
            Ok(_) => panic!("expected error"),
            Err(e) => e,
        };
        assert!(matches!(err, DbError::Format(FormatError::RecordPayloadTypeMismatch)));
    }

    #[test]
    fn row_subset_by_field_defs_handles_empty_missing_and_nested_merge() {
        use crate::schema::{FieldDef, FieldPath, Type};
        use std::borrow::Cow;

        let row = BTreeMap::from([
            (
                "a".to_string(),
                RowValue::Object(BTreeMap::from([
                    ("b".to_string(), RowValue::Int64(1)),
                    ("c".to_string(), RowValue::Int64(2)),
                    (
                        "d".to_string(),
                        RowValue::Object(BTreeMap::from([("e".to_string(), RowValue::Int64(3))])),
                    ),
                ])),
            ),
            ("x".to_string(), RowValue::String("y".to_string())),
        ]);

        // Empty path => ignored.
        let empty = FieldDef {
            path: FieldPath(vec![]),
            ty: Type::String,
            constraints: vec![],
        };
        // Missing leaf => skipped.
        let missing = FieldDef {
            path: FieldPath(vec![Cow::Borrowed("missing")]),
            ty: Type::String,
            constraints: vec![],
        };
        // Root scalar.
        let x = FieldDef {
            path: FieldPath(vec![Cow::Borrowed("x")]),
            ty: Type::String,
            constraints: vec![],
        };
        // Two nested leaves under same root => must merge.
        let ab = FieldDef {
            path: FieldPath(vec![Cow::Borrowed("a"), Cow::Borrowed("b")]),
            ty: Type::Int64,
            constraints: vec![],
        };
        let ac = FieldDef {
            path: FieldPath(vec![Cow::Borrowed("a"), Cow::Borrowed("c")]),
            ty: Type::Int64,
            constraints: vec![],
        };
        let ade = FieldDef {
            path: FieldPath(vec![
                Cow::Borrowed("a"),
                Cow::Borrowed("d"),
                Cow::Borrowed("e"),
            ]),
            ty: Type::Int64,
            constraints: vec![],
        };

        let out = super::row_subset_by_field_defs(&row, &[empty, missing, x, ab, ac, ade]);
        assert_eq!(out.get("x"), Some(&RowValue::String("y".to_string())));
        let a = out.get("a").unwrap();
        let RowValue::Object(m) = a else {
            panic!("expected object")
        };
        assert!(m.contains_key("b"));
        assert!(m.contains_key("c"));
        // nested len>1 recursion path
        let RowValue::Object(d) = m.get("d").unwrap() else {
            panic!("expected object")
        };
        assert_eq!(d.get("e"), Some(&RowValue::Int64(3)));
    }

    #[test]
    fn subset_model_validation_errors_when_catalog_has_no_primary_key() {
        use crate::catalog::{encode_catalog_payload, CatalogRecordWire};
        use crate::file_format::{FileHeader, FILE_HEADER_SIZE};
        use crate::segments::header::SegmentHeader;
        use crate::segments::writer::SegmentWriter;
        use crate::storage::VecStore;
        use crate::superblock::{Superblock, SUPERBLOCK_SIZE};
        use crate::schema::{DbModel, FieldDef, FieldPath, Type};
        use std::borrow::Cow;
        use std::path::PathBuf;

        #[allow(dead_code)]
        struct T;
        impl DbModel for T {
            fn collection_name() -> &'static str {
                "t"
            }
            fn fields() -> Vec<FieldDef> {
                vec![FieldDef {
                    path: FieldPath(vec![Cow::Borrowed("id")]),
                    ty: Type::String,
                    constraints: vec![],
                }]
            }
            fn primary_field() -> &'static str {
                "id"
            }
        }

        let segment_start = (FILE_HEADER_SIZE + 2 * SUPERBLOCK_SIZE) as u64;
        let mut store = VecStore::new();
        store
            .write_all_at(0, &FileHeader::new_v0_5().encode())
            .unwrap();
        store.write_all_at(segment_start - 1, &[0u8]).unwrap();
        store
            .write_all_at(FILE_HEADER_SIZE as u64, &Superblock::empty().encode())
            .unwrap();
        store
            .write_all_at(
                (FILE_HEADER_SIZE + SUPERBLOCK_SIZE) as u64,
                &Superblock::empty().encode(),
            )
            .unwrap();

        let fields = vec![FieldDef {
            path: FieldPath(vec![Cow::Owned("id".to_string())]),
            ty: Type::String,
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
        let mut w = SegmentWriter::new(&mut store, segment_start);
        w.append(
            SegmentHeader {
                segment_type: SegmentType::Schema,
                payload_len: 0,
                payload_crc32c: 0,
            },
            &schema_bytes,
        )
        .unwrap();

        let db = open::open_with_store(
            PathBuf::from("x.typra"),
            store,
            crate::config::OpenOptions::default(),
        )
            .unwrap();
        let err = match db.collection::<T>() {
            Ok(_) => panic!("expected error"),
            Err(e) => e,
        };
        assert!(matches!(
            err,
            DbError::Schema(crate::error::SchemaError::NoPrimaryKey { .. })
        ));
    }

    #[test]
    fn insert_autocommit_covers_index_batch_some_and_none() {
        use crate::schema::{FieldPath, IndexDef, IndexKind};

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.typra");
        let mut db = Database::open(&path).unwrap();

        // With a non-unique index => index_entries non-empty => index_bytes Some => batch includes Index.
        let idx = vec![IndexDef {
            name: "x_idx".to_string(),
            path: FieldPath(vec![Cow::Owned("x".to_string())]),
            kind: IndexKind::NonUnique,
        }];
        let (cid, _) = db
            .register_collection_with_indexes("t", vec![path_field("id"), path_field("x")], idx, "id")
            .unwrap();
        db.insert(cid, {
            let mut m = BTreeMap::new();
            m.insert("id".to_string(), RowValue::String("a".to_string()));
            m.insert("x".to_string(), RowValue::String("v".to_string()));
            m
        })
        .unwrap();

        // Without indexes => index_entries empty => index_bytes None => batch includes only Record.
        let (cid2, _) = db
            .register_collection("t2", vec![path_field("id")], "id")
            .unwrap();
        db.insert(cid2, {
            let mut m = BTreeMap::new();
            m.insert("id".to_string(), RowValue::String("b".to_string()));
            m
        })
        .unwrap();
    }

    #[test]
    fn insert_replace_multi_segment_schema_uses_v3_replace_encoding() {
        use crate::schema::{FieldDef, FieldPath, Type};
        use std::borrow::Cow;

        let mut db = Database::open_in_memory().unwrap();
        let fields = vec![
            FieldDef {
                path: FieldPath(vec![Cow::Borrowed("id")]),
                ty: Type::String,
                constraints: vec![],
            },
            // multi-segment schema forces v3 encoding paths
            FieldDef {
                path: FieldPath(vec![Cow::Borrowed("a"), Cow::Borrowed("b")]),
                ty: Type::Optional(Box::new(Type::String)),
                constraints: vec![],
            },
        ];
        let (cid, _) = db.register_collection("t", fields, "id").unwrap();

        db.insert(cid, {
            let mut m = BTreeMap::new();
            m.insert("id".to_string(), RowValue::String("pk".to_string()));
            // a is object, b missing => optional
            m.insert("a".to_string(), RowValue::Object(BTreeMap::new()));
            m
        })
        .unwrap();

        // Second insert with same pk triggers replace path + v3 replace opcode.
        db.insert(cid, {
            let mut m = BTreeMap::new();
            m.insert("id".to_string(), RowValue::String("pk".to_string()));
            m.insert(
                "a".to_string(),
                RowValue::Object(BTreeMap::from([(
                    "b".to_string(),
                    RowValue::String("x".to_string()),
                )])),
            );
            m
        })
        .unwrap();
    }

    #[test]
    fn delete_multi_segment_schema_uses_v3_delete_encoding_and_autocommit() {
        use crate::schema::{FieldDef, FieldPath, Type};
        use std::borrow::Cow;

        let mut db = Database::open_in_memory().unwrap();
        let fields = vec![
            FieldDef {
                path: FieldPath(vec![Cow::Borrowed("id")]),
                ty: Type::String,
                constraints: vec![],
            },
            FieldDef {
                path: FieldPath(vec![Cow::Borrowed("a"), Cow::Borrowed("b")]),
                ty: Type::Optional(Box::new(Type::String)),
                constraints: vec![],
            },
        ];
        let (cid, _) = db.register_collection("t", fields, "id").unwrap();
        db.insert(cid, {
            let mut m = BTreeMap::new();
            m.insert("id".to_string(), RowValue::String("pk".to_string()));
            m.insert("a".to_string(), RowValue::Object(BTreeMap::new()));
            m
        })
        .unwrap();

        db.delete(cid, &ScalarValue::String("pk".to_string()))
            .unwrap();
    }

    #[test]
    fn export_snapshot_to_path_happy_path() {
        let dir = tempfile::tempdir().unwrap();
        let db_path = dir.path().join("t.typra");
        let snap_path = dir.path().join("snap.typra");

        let mut db = Database::open(&db_path).unwrap();
        let (cid, _) = db
            .register_collection("t", vec![path_field("id")], "id")
            .unwrap();
        db.insert(cid, {
            let mut m = BTreeMap::new();
            m.insert("id".to_string(), RowValue::String("a".to_string()));
            m
        })
        .unwrap();

        db.export_snapshot_to_path(&snap_path).unwrap();
        assert!(snap_path.exists());
    }

    #[test]
    fn insert_replace_unique_index_changes_key_exercises_delete_entries_without_violation() {
        // Covers the unique-index check loop for:
        // - delete entries (e.op != Insert) where lookup returns Some(...)
        // - insert entries where lookup returns None (new key)
        use crate::schema::{FieldPath, IndexDef, IndexKind};

        let mut db = Database::open_in_memory().unwrap();
        let indexes = vec![IndexDef {
            name: "x_uq".to_string(),
            path: FieldPath(vec![Cow::Owned("x".to_string())]),
            kind: IndexKind::Unique,
        }];
        let (cid, _) = db
            .register_collection_with_indexes(
                "t",
                vec![path_field("id"), path_field("x")],
                indexes,
                "id",
            )
            .unwrap();

        db.insert(cid, {
            let mut m = BTreeMap::new();
            m.insert("id".to_string(), RowValue::String("pk".to_string()));
            m.insert("x".to_string(), RowValue::String("a".to_string()));
            m
        })
        .unwrap();

        // Replace same PK but change unique index key; should not report UniqueIndexViolation.
        db.insert(cid, {
            let mut m = BTreeMap::new();
            m.insert("id".to_string(), RowValue::String("pk".to_string()));
            m.insert("x".to_string(), RowValue::String("b".to_string()));
            m
        })
        .unwrap();
    }

    #[test]
    fn plan_insert_row_nested_schema_rejects_unknown_leaf_and_traverses_non_object_leaves() {
        use crate::catalog::CatalogRecordWire;
        use crate::schema::{FieldDef, FieldPath, Type};
        use crate::{Catalog, CollectionId};
        use crate::ScalarValue;
        use std::borrow::Cow;

        // Build a catalog with a nested optional field a.b and PK id.
        let mut catalog = Catalog::default();
        catalog
            .apply_record(CatalogRecordWire::CreateCollection {
                collection_id: 1,
                name: "t".to_string(),
                schema_version: 1,
                fields: vec![
                    FieldDef {
                        path: FieldPath(vec![Cow::Borrowed("id")]),
                        ty: Type::String,
                        constraints: vec![],
                    },
                    FieldDef {
                        path: FieldPath(vec![Cow::Borrowed("a"), Cow::Borrowed("b")]),
                        ty: Type::Optional(Box::new(Type::String)),
                        constraints: vec![],
                    },
                ],
                indexes: vec![],
                primary_field: Some("id".to_string()),
            })
            .unwrap();

        // Unknown leaf path: a.c is not in schema.
        let mut row = BTreeMap::new();
        row.insert("id".to_string(), RowValue::String("pk".to_string()));
        row.insert(
            "a".to_string(),
            RowValue::Object(BTreeMap::from([(
                "c".to_string(),
                RowValue::String("oops".to_string()),
            )])),
        );
        let err = super::plan_insert_row(&catalog, CollectionId(1), row).unwrap_err();
        assert!(matches!(
            err,
            DbError::Schema(crate::error::SchemaError::RowUnknownField { .. })
        ));

        // Non-object leaves should be treated as leaves at that path and rejected (a is scalar).
        let mut row2 = BTreeMap::new();
        row2.insert("id".to_string(), RowValue::String("pk2".to_string()));
        row2.insert("a".to_string(), RowValue::String("not_object".to_string()));
        let err = super::plan_insert_row(&catalog, CollectionId(1), row2).unwrap_err();
        assert!(matches!(
            err,
            DbError::Schema(crate::error::SchemaError::RowUnknownField { .. })
        ));

        // Optional nested field absent: provide empty object for a so b is missing but allowed.
        let mut row3 = BTreeMap::new();
        row3.insert("id".to_string(), RowValue::String("pk3".to_string()));
        row3.insert("a".to_string(), RowValue::Object(BTreeMap::new()));
        let (_payload, (_pk_key, full), _idx, pk_scalar) =
            super::plan_insert_row(&catalog, CollectionId(1), row3).unwrap();
        assert_eq!(pk_scalar, ScalarValue::String("pk3".to_string()));
        assert!(full.contains_key("id"));
    }

    #[test]
    fn plan_insert_row_covers_pk_scan_and_pk_validation_error_branches() {
        use crate::catalog::CatalogRecordWire;
        use crate::schema::{FieldDef, FieldPath, Type};
        use crate::{Catalog, CollectionId};
        use std::borrow::Cow;

        // 1) Cover pk_def scan branch where we skip a non-PK top-level field first.
        let mut catalog = Catalog::default();
        catalog
            .apply_record(CatalogRecordWire::CreateCollection {
                collection_id: 1,
                name: "t".to_string(),
                schema_version: 1,
                fields: vec![
                    FieldDef {
                        path: FieldPath(vec![Cow::Borrowed("other")]),
                        ty: Type::String,
                        constraints: vec![],
                    },
                    FieldDef {
                        path: FieldPath(vec![Cow::Borrowed("id")]),
                        ty: Type::String,
                        constraints: vec![],
                    },
                ],
                indexes: vec![],
                primary_field: Some("id".to_string()),
            })
            .unwrap();

        // Row missing PK triggers RowMissingPrimary.
        let mut row_missing_pk = BTreeMap::new();
        row_missing_pk.insert("other".to_string(), RowValue::String("x".to_string()));
        let err = super::plan_insert_row(&catalog, CollectionId(1), row_missing_pk).unwrap_err();
        assert!(matches!(
            err,
            DbError::Schema(crate::error::SchemaError::RowMissingPrimary { .. })
        ));

        // Non-scalar PK value is rejected during type validation.
        let mut row_bad_pk = BTreeMap::new();
        row_bad_pk.insert("id".to_string(), RowValue::Object(BTreeMap::new()));
        row_bad_pk.insert("other".to_string(), RowValue::String("x".to_string()));
        let err = super::plan_insert_row(&catalog, CollectionId(1), row_bad_pk).unwrap_err();
        assert!(matches!(err, DbError::Validation(_)));

        // 2) Cover ensure_pk_type_primitive failing.
        let mut catalog2 = Catalog::default();
        catalog2
            .apply_record(CatalogRecordWire::CreateCollection {
                collection_id: 1,
                name: "t2".to_string(),
                schema_version: 1,
                fields: vec![FieldDef {
                    path: FieldPath(vec![Cow::Borrowed("id")]),
                    ty: Type::Optional(Box::new(Type::String)),
                    constraints: vec![],
                }],
                indexes: vec![],
                primary_field: Some("id".to_string()),
            })
            .unwrap();

        let mut row = BTreeMap::new();
        row.insert("id".to_string(), RowValue::String("pk".to_string()));
        let err = super::plan_insert_row(&catalog2, CollectionId(1), row).unwrap_err();
        assert!(matches!(err, DbError::Validation(_)));
    }

    #[test]
    fn register_collection_rejects_whitespace_only_primary_field() {
        let mut db = Database::open_in_memory().unwrap();
        let err = db
            .register_collection("t", vec![path_field("id")], "   \t")
            .unwrap_err();
        assert!(matches!(
            err,
            DbError::Schema(crate::error::SchemaError::InvalidCollectionName)
        ));
    }

    #[test]
    fn plan_insert_row_skips_index_when_optional_indexed_scalar_is_absent() {
        use crate::catalog::CatalogRecordWire;
        use crate::schema::{FieldDef, FieldPath, IndexDef, IndexKind, Type};
        use crate::{Catalog, CollectionId};
        use std::borrow::Cow;

        let mut catalog = Catalog::default();
        catalog
            .apply_record(CatalogRecordWire::CreateCollection {
                collection_id: 1,
                name: "t".to_string(),
                schema_version: 1,
                fields: vec![
                    FieldDef {
                        path: FieldPath(vec![Cow::Borrowed("id")]),
                        ty: Type::String,
                        constraints: vec![],
                    },
                    FieldDef {
                        path: FieldPath(vec![Cow::Borrowed("email")]),
                        ty: Type::Optional(Box::new(Type::String)),
                        constraints: vec![],
                    },
                ],
                indexes: vec![IndexDef {
                    name: "email_idx".to_string(),
                    path: FieldPath(vec![Cow::Owned("email".to_string())]),
                    kind: IndexKind::NonUnique,
                }],
                primary_field: Some("id".to_string()),
            })
            .unwrap();

        let mut row = BTreeMap::new();
        row.insert("id".to_string(), RowValue::String("pk".to_string()));
        let (_payload, _full, index_entries, _pk) =
            super::plan_insert_row(&catalog, CollectionId(1), row).unwrap();
        assert!(
            index_entries.is_empty(),
            "optional absent scalar should not emit index ops"
        );
    }

    #[test]
    fn row_value_at_path_for_plan_non_object_middle_returns_none() {
        let mut row = BTreeMap::new();
        row.insert(
            "a".to_string(),
            RowValue::Object(BTreeMap::from([(
                "b".to_string(),
                RowValue::String("x".to_string()),
            )])),
        );
        let path = vec![
            Cow::Borrowed("a"),
            Cow::Borrowed("b"),
            Cow::Borrowed("c"),
        ];
        assert!(super::row_value_at_path_for_plan(&row, &path).is_none());
    }

    #[test]
    fn row_value_at_path_for_plan_none_middle_returns_none() {
        let mut row = BTreeMap::new();
        row.insert(
            "a".to_string(),
            RowValue::Object(BTreeMap::from([("b".to_string(), RowValue::None)])),
        );
        let path = vec![
            Cow::Borrowed("a"),
            Cow::Borrowed("b"),
            Cow::Borrowed("c"),
        ];
        assert!(super::row_value_at_path_for_plan(&row, &path).is_none());
    }

    #[test]
    fn row_value_at_path_for_plan_empty_path_returns_none() {
        let row: BTreeMap<String, RowValue> = BTreeMap::new();
        let path: Vec<Cow<'static, str>> = Vec::new();
        assert!(super::row_value_at_path_for_plan(&row, &path).is_none());
    }

    #[test]
    fn plan_insert_row_three_segment_field_runs_nested_prefix_loop() {
        use crate::catalog::CatalogRecordWire;
        use crate::schema::{FieldDef, FieldPath, Type};
        use crate::{Catalog, CollectionId};
        use std::borrow::Cow;

        let mut catalog = Catalog::default();
        catalog
            .apply_record(CatalogRecordWire::CreateCollection {
                collection_id: 1,
                name: "t".to_string(),
                schema_version: 1,
                fields: vec![
                    FieldDef {
                        path: FieldPath(vec![Cow::Borrowed("id")]),
                        ty: Type::String,
                        constraints: vec![],
                    },
                    FieldDef {
                        path: FieldPath(vec![
                            Cow::Borrowed("a"),
                            Cow::Borrowed("b"),
                            Cow::Borrowed("c"),
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
        row.insert("id".to_string(), RowValue::String("p1".to_string()));
        row.insert(
            "a".to_string(),
            RowValue::Object(BTreeMap::from([(
                "b".to_string(),
                RowValue::Object(BTreeMap::from([(
                    "c".to_string(),
                    RowValue::String("z".to_string()),
                )])),
            )])),
        );
        super::plan_insert_row(&catalog, CollectionId(1), row).unwrap();
    }

    #[test]
    fn delete_skips_index_delete_when_optional_indexed_scalar_is_absent() {
        use crate::schema::{FieldPath, IndexDef, IndexKind, Type};

        let mut db = Database::open_in_memory().unwrap();
        let mut email_def = path_field("email");
        email_def.ty = Type::Optional(Box::new(Type::String));
        let indexes = vec![IndexDef {
            name: "email_idx".to_string(),
            path: FieldPath(vec![Cow::Owned("email".to_string())]),
            kind: IndexKind::NonUnique,
        }];
        let (cid, _) = db
            .register_collection_with_indexes(
                "t",
                vec![path_field("id"), email_def],
                indexes,
                "id",
            )
            .unwrap();
        db.insert(
            cid,
            {
                let mut m = BTreeMap::new();
                m.insert("id".to_string(), RowValue::String("k".to_string()));
                m
            },
        )
        .unwrap();
        db.delete(cid, &ScalarValue::String("k".to_string())).unwrap();
    }

    #[test]
    fn query_iter_matches_execute_query_for_indexed_equality() {
        use crate::query::{Predicate, Query};
        use crate::schema::{FieldPath, IndexDef, IndexKind};
        use crate::{RowValue, ScalarValue};

        let mut db = Database::open_in_memory().unwrap();
        let mut year_def = path_field("year");
        year_def.ty = Type::Int64;
        let fields = vec![path_field("title"), year_def];
        let indexes = vec![IndexDef {
            name: "title_idx".to_string(),
            path: FieldPath(vec![std::borrow::Cow::Owned("title".to_string())]),
            kind: IndexKind::NonUnique,
        }];
        let (cid, _) = db
            .register_collection_with_indexes("books", fields, indexes, "title")
            .unwrap();
        db.insert(cid, {
            let mut m = BTreeMap::new();
            m.insert("title".to_string(), RowValue::String("Hello".to_string()));
            m.insert("year".to_string(), RowValue::Int64(2020));
            m
        })
        .unwrap();

        let q = Query {
            collection: cid,
            predicate: Some(Predicate::Eq {
                path: FieldPath(vec![std::borrow::Cow::Owned("title".to_string())]),
                value: ScalarValue::String("Hello".to_string()),
            }),
            limit: None,
            order_by: None,
        };
        let mut from_iter: Vec<_> = db.query_iter(&q).unwrap().map(|r| r.unwrap()).collect();
        let mut from_vec = db.query(&q).unwrap();
        from_iter.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
        from_vec.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
        assert_eq!(from_iter, from_vec);
    }

    #[test]
    fn query_iter_order_by_uses_external_sort_spill_for_large_inputs() {
        use crate::query::{OrderBy, OrderDirection, Query};
        use crate::{RowValue, ScalarValue};

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.typra");
        let mut db = Database::open(&path).unwrap();

        let mut year_def = path_field("year");
        year_def.ty = Type::Int64;
        let fields = vec![path_field("title"), year_def];
        let (cid, _) = db.register_collection("books", fields, "title").unwrap();
        for i in 0..6000i64 {
            db.insert(cid, {
                let mut m = BTreeMap::new();
                m.insert("title".to_string(), RowValue::String(format!("t{i}")));
                m.insert("year".to_string(), RowValue::Int64(i));
                m
            })
            .unwrap();
        }

        let q = Query {
            collection: cid,
            predicate: None,
            order_by: Some(OrderBy {
                path: crate::schema::FieldPath(vec![std::borrow::Cow::Borrowed("year")]),
                direction: OrderDirection::Desc,
            }),
            limit: Some(50),
        };

        let from_vec = db.query(&q).unwrap();
        let from_iter: Vec<_> = db.query_iter(&q).unwrap().map(|r| r.unwrap()).collect();
        assert_eq!(from_iter, from_vec);

        assert_eq!(from_iter[0].get("year"), Some(&RowValue::Int64(5999)));
        assert_eq!(
            from_iter.last().unwrap().get("year"),
            Some(&RowValue::Int64(5950))
        );

        let got = db
            .get(cid, &ScalarValue::String("t123".to_string()))
            .unwrap()
            .unwrap();
        assert_eq!(got.get("year"), Some(&RowValue::Int64(123)));
    }

    #[test]
    fn subset_projection_merges_nested_paths_under_shared_object() {
        use crate::schema::{DbModel, FieldDef, FieldPath, Type};
        use crate::RowValue;
        use std::borrow::Cow;
        struct Sub;
        impl DbModel for Sub {
            fn collection_name() -> &'static str {
                "x"
            }
            fn fields() -> Vec<FieldDef> {
                vec![
                    FieldDef {
                        path: FieldPath(vec![Cow::Borrowed("a"), Cow::Borrowed("b")]),
                        ty: Type::String,
                        constraints: vec![],
                    },
                    FieldDef {
                        path: FieldPath(vec![Cow::Borrowed("a"), Cow::Borrowed("c")]),
                        ty: Type::Int64,
                        constraints: vec![],
                    },
                ]
            }
            fn primary_field() -> &'static str {
                "id"
            }
        }

        let mut row = BTreeMap::new();
        row.insert("id".to_string(), RowValue::String("pk".to_string()));
        let inner = BTreeMap::from([
            ("b".to_string(), RowValue::String("B".to_string())),
            ("c".to_string(), RowValue::Int64(42)),
        ]);
        row.insert("a".to_string(), RowValue::Object(inner));

        let out = super::project_row::<Sub>(row);
        let a = out.get("a").unwrap();
        let RowValue::Object(m) = a else {
            panic!("expected object");
        };
        assert_eq!(m.get("b"), Some(&RowValue::String("B".to_string())));
        assert_eq!(m.get("c"), Some(&RowValue::Int64(42)));
        assert_eq!(out.len(), 1);
    }

    #[test]
    fn checkpoint_roundtrip_replays_only_tail() {
        use crate::schema::{IndexDef, IndexKind};
        use crate::{RowValue, ScalarValue};

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.typra");

        // Create, write state, checkpoint, then append more.
        {
            let mut db = Database::open(&path).unwrap();
            let fields = vec![path_field("title"), path_field("author")];
            let indexes = vec![IndexDef {
                name: "author_idx".to_string(),
                path: crate::schema::FieldPath(vec![std::borrow::Cow::Owned("author".to_string())]),
                kind: IndexKind::NonUnique,
            }];
            let (cid, _) = db
                .register_collection_with_indexes("books", fields, indexes, "title")
                .unwrap();
            db.insert(cid, {
                let mut m = BTreeMap::new();
                m.insert("title".to_string(), RowValue::String("Hello".to_string()));
                m.insert("author".to_string(), RowValue::String("Alice".to_string()));
                m
            })
            .unwrap();
            db.checkpoint().unwrap();
            db.insert(cid, {
                let mut m = BTreeMap::new();
                m.insert("title".to_string(), RowValue::String("World".to_string()));
                m.insert("author".to_string(), RowValue::String("Bob".to_string()));
                m
            })
            .unwrap();
        }

        // Reopen; should load from checkpoint then replay tail insert.
        let db = Database::open(&path).unwrap();
        let cid = db.collection_id_named("books").unwrap();
        let got = db
            .get(cid, &ScalarValue::String("Hello".to_string()))
            .unwrap()
            .unwrap();
        assert_eq!(
            got.get("author"),
            Some(&RowValue::String("Alice".to_string()))
        );
        let got2 = db
            .get(cid, &ScalarValue::String("World".to_string()))
            .unwrap()
            .unwrap();
        assert_eq!(
            got2.get("author"),
            Some(&RowValue::String("Bob".to_string()))
        );
    }

    #[test]
    fn corrupt_checkpoint_falls_back_in_auto_truncate_but_errors_in_strict() {
        use crate::config::{OpenOptions, RecoveryMode};
        use crate::segments::header::SEGMENT_HEADER_LEN;
        use crate::superblock::decode_superblock;
        use crate::RowValue;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.typra");

        // Create DB and checkpoint.
        {
            let mut db = Database::open(&path).unwrap();
            let (cid, _) = db
                .register_collection("books", vec![path_field("title")], "title")
                .unwrap();
            db.insert(cid, {
                let mut m = BTreeMap::new();
                m.insert("title".to_string(), RowValue::String("Hello".to_string()));
                m
            })
            .unwrap();
            db.checkpoint().unwrap();
        }

        // Corrupt one byte inside the checkpoint payload.
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)
            .unwrap();
        let mut store = crate::storage::FileStore::new(file);
        let mut sb_buf = [0u8; SUPERBLOCK_SIZE];
        store
            .read_exact_at(FILE_HEADER_SIZE as u64, &mut sb_buf)
            .unwrap();
        let sb = decode_superblock(&sb_buf).unwrap();
        assert!(sb.checkpoint_offset != 0);
        let corrupt_at = sb.checkpoint_offset + SEGMENT_HEADER_LEN as u64 + 5;
        store.write_all_at(corrupt_at, &[0xff]).unwrap();
        store.sync().unwrap();

        // Strict should error.
        let strict = Database::open_with_options(
            &path,
            OpenOptions {
                recovery: RecoveryMode::Strict,
                ..OpenOptions::default()
            },
        );
        assert!(strict.is_err());

        // AutoTruncate should fall back to replay and still open.
        let auto = Database::open_with_options(
            &path,
            OpenOptions {
                recovery: RecoveryMode::AutoTruncate,
                ..OpenOptions::default()
            },
        )
        .unwrap();
        assert_eq!(auto.collection_names(), vec!["books".to_string()]);
    }

    #[test]
    fn temp_segments_are_ignored_on_reopen() {
        use crate::segments::header::{SegmentHeader, SegmentType};
        use crate::segments::writer::SegmentWriter;
        use crate::{RowValue, ScalarValue};

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("t.typra");

        // Create DB, write state, then append an ephemeral Temp segment.
        {
            let mut db = Database::open(&path).unwrap();
            let (cid, _) = db
                .register_collection("books", vec![path_field("title")], "title")
                .unwrap();
            db.insert(cid, {
                let mut m = BTreeMap::new();
                m.insert("title".to_string(), RowValue::String("Hello".to_string()));
                m
            })
            .unwrap();

            let off = db.store.len().unwrap();
            let mut w = SegmentWriter::new(&mut db.store, off);
            w.append(
                SegmentHeader {
                    segment_type: SegmentType::Temp,
                    payload_len: 0,
                    payload_crc32c: 0,
                },
                b"spill",
            )
            .unwrap();
            db.store.sync().unwrap();
        }

        // Reopen should succeed and ignore the Temp segment.
        let db = Database::open(&path).unwrap();
        let cid = db.collection_id_named("books").unwrap();
        let got = db
            .get(cid, &ScalarValue::String("Hello".to_string()))
            .unwrap()
            .unwrap();
        assert_eq!(
            got.get("title"),
            Some(&RowValue::String("Hello".to_string()))
        );
    }
}
