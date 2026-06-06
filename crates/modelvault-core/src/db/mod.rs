//! Database handle and orchestration.
//!
//! [`Database`] is implemented using internal modules `open` (bootstrap), `replay` (catalog and
//! rows from segments), `read` / `write` / `catalog_ops` / `maintenance` (API surface), and
//! `helpers` (name rules).

mod catalog_ops;
mod file_scan;
mod fs_ops;
mod handle_registry;
mod helpers;
mod maintenance;
mod open;
mod read;
mod recover;
mod replay;
pub(crate) mod row_materialize;
mod row_merge;
pub(crate) mod row_paths;
pub(crate) use row_paths::validate_unknown_fields_for_multiseg_schema;
mod segment_write;
mod write;
mod writer_registry;
pub(crate) use row_materialize::{build_non_pk_values_in_schema_order, row_value_at_path};

pub(crate) use handle_registry::SharedDbHandle;
pub use handle_registry::SharedDbState;

pub use file_scan::{
    read_header_and_superblocks, scan_database_file, scan_database_store, select_superblock,
    DatabaseFileScan, DatabaseScanMode, SEGMENT_REGION_START,
};

use std::collections::{BTreeMap, HashMap};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::catalog::Catalog;
use crate::config::{OpenMode, OpenOptions, OpenRecoveryInfo};
use crate::error::{DbError, FormatError, SchemaError};
use crate::index::IndexState;
use crate::index::{IndexEntry, IndexOp};
use crate::record::{
    encode_record_payload_v2, encode_record_payload_v3, non_pk_defs_in_order, RowValue, ScalarValue,
};
use crate::schema::{CollectionId, FieldDef};
use crate::storage::{FileStore, Store, VecStore};
use crate::validation;

use self::fs_ops::{FsOps, StdFsOps};

#[cfg(test)]
pub(crate) use maintenance::best_effort_fsync_parent_dir;

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
        .ok_or(DbError::Schema(SchemaError::PrimaryFieldNotFound {
            name: pk_name.to_string(),
        }))?;
    let pk_ty = &pk_def.ty;
    validation::ensure_pk_type_primitive(pk_ty)?;
    let mut pk_path = vec![pk_name.to_string()];
    let pk_cell = row
        .get(pk_name)
        .ok_or(DbError::Schema(SchemaError::RowMissingPrimary {
            name: pk_name.to_string(),
        }))?;
    validation::validate_value(&mut pk_path, pk_ty, &pk_def.constraints, pk_cell)?;
    if let Ok(scalar) = pk_cell.clone().into_scalar() {
        validation::ensure_pk_scalar_finite(&scalar)?;
    }
    // Validate unknown fields: for nested schema paths we validate by traversing row objects.
    // For legacy single-segment schemas, keep the existing top-level validation.
    let has_multi_segment_schema = col.fields.iter().any(|f| f.path.0.len() != 1);
    if !has_multi_segment_schema {
        validation::validate_top_level_row(&col.fields, pk_name, &row)?;
    } else {
        validation::validate_multiseg_row(&col.fields, pk_name, &row)?;
    }

    // `pk_cell` is already present (validated above), so remove must succeed.
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
    let non_pk = row_materialize::build_non_pk_values_in_schema_order(&row, &non_pk_defs)?;

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

    let mut full_map: BTreeMap<String, RowValue> = BTreeMap::new();
    full_map.insert(pk_name.to_string(), pk_val);
    for (def, v) in &non_pk {
        let parts: Vec<String> = def.path.0.iter().map(|s| s.as_ref().to_string()).collect();
        if parts.len() == 1 {
            full_map.insert(parts[0].clone(), v.clone());
        } else {
            debug_assert!(parts.len() >= 2);
            row_merge::merge_non_pk_into_full_map(&mut full_map, &parts, v);
        }
    }
    let mut index_entries: Vec<IndexEntry> = Vec::new();
    for idx in &col.indexes {
        let Some(v) = scalar_at_path(&full_map, &idx.path) else {
            if idx.kind == crate::schema::IndexKind::Unique {
                #[cfg(feature = "tracing")]
                tracing::warn!(
                    collection_id = collection_id.0,
                    index = %idx.name,
                    "unique index field absent or null; row is not indexed (SQL NULL semantics)"
                );
            }
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

pub(crate) fn index_deletes_for_existing_row(
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

/// Rebuild in-memory index state from catalog + latest rows (insert semantics only).
pub fn rebuild_indexes_from_latest(
    catalog: &Catalog,
    latest: &LatestMap,
) -> Result<IndexState, DbError> {
    let mut state = IndexState::default();
    for col in catalog.collections() {
        let Some(pk_name) = col.primary_field.as_deref() else {
            continue;
        };
        let Some(pk_def) = col
            .fields
            .iter()
            .find(|f| f.path.0.len() == 1 && f.path.0[0] == pk_name)
        else {
            continue;
        };
        for ((cid, _), row) in latest.iter() {
            if *cid != col.id.0 {
                continue;
            }
            let Ok(pk_scalar) = row
                .get(pk_name)
                .cloned()
                .ok_or(())
                .and_then(|c| c.into_scalar().map_err(|_| ()))
            else {
                continue;
            };
            if !pk_scalar.ty_matches(&pk_def.ty) {
                continue;
            }
            for idx in &col.indexes {
                let Some(v) = scalar_at_path(row, &idx.path) else {
                    continue;
                };
                state.apply(IndexEntry {
                    collection_id: col.id.0,
                    index_name: idx.name.clone(),
                    kind: idx.kind,
                    op: IndexOp::Insert,
                    index_key: v.canonical_key_bytes(),
                    pk_key: pk_scalar.canonical_key_bytes(),
                })?;
            }
        }
    }
    Ok(state)
}

fn index_snapshot(entries: &mut [IndexEntry]) {
    entries.sort_by(|a, b| {
        let kind_key = |k: crate::schema::IndexKind| match k {
            crate::schema::IndexKind::Unique => 0u8,
            crate::schema::IndexKind::NonUnique => 1u8,
        };
        a.collection_id
            .cmp(&b.collection_id)
            .then_with(|| a.index_name.cmp(&b.index_name))
            .then_with(|| kind_key(a.kind).cmp(&kind_key(b.kind)))
            .then_with(|| a.index_key.cmp(&b.index_key))
            .then_with(|| a.pk_key.cmp(&b.pk_key))
    });
}

/// Verify replayed index segments match what row data implies.
pub fn verify_indexes_match_rows(
    catalog: &Catalog,
    latest: &LatestMap,
    indexes: &IndexState,
) -> Result<(), DbError> {
    let expected = rebuild_indexes_from_latest(catalog, latest)?;
    let mut got = indexes.entries_for_checkpoint();
    let mut want = expected.entries_for_checkpoint();
    index_snapshot(&mut got);
    index_snapshot(&mut want);
    if got != want {
        return Err(DbError::Format(FormatError::InvalidCatalogPayload {
            message: "index state does not match row data".into(),
        }));
    }
    Ok(())
}

/// Staged writes while [`Database::transaction`] is executing.
pub(crate) struct TxnStaging {
    pub(crate) txn_id: u64,
    pub(crate) shadow_catalog: Catalog,
    pub(crate) shadow_latest: LatestMap,
    pub(crate) shadow_indexes: IndexState,
    pub(crate) pending: Vec<(crate::segments::header::SegmentType, Vec<u8>)>,
}

/// Opened ModelVault database: generic over a [`Store`] ([`FileStore`] on disk, [`VecStore`] in memory).
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
    /// Present for writable on-disk databases (process-wide single-writer registry).
    #[allow(dead_code)]
    writer_registry: Option<writer_registry::WriterRegistryGuard>,
    /// Shared in-memory mirror for same-process read-only handles.
    shared_mirror: Option<SharedDbHandle>,
    /// When true, reads pull from [`Self::shared_mirror`] before each operation.
    read_only_attached: bool,
    /// Recovery actions taken during the most recent open (truncation, etc.).
    recovery_info: OpenRecoveryInfo,
    /// Covers replace-path record encoding error branches in tests (misaligned validated row maps).
    #[cfg(test)]
    #[doc(hidden)]
    #[allow(clippy::type_complexity)]
    pub(crate) test_poison_planned_replace_row:
        Option<fn(CollectionId, &mut BTreeMap<String, RowValue>)>,
    /// Covers delete Opcode payload encoding `?` by supplying a bogus scalar unrelated to validated `pk`.
    #[cfg(test)]
    #[doc(hidden)]
    pub(crate) test_poison_delete_encode_scalar: Option<fn(ScalarValue) -> ScalarValue>,
}

impl<S: Store> Database<S> {
    pub(crate) fn open_with_store(
        path: PathBuf,
        store: S,
        opts: OpenOptions,
    ) -> Result<Self, DbError> {
        open::open_with_store(path, store, opts)
    }

    /// Test hook: mutate the planned row once on the replace path immediately before Opcode re-encoding.
    #[cfg(test)]
    #[doc(hidden)]
    pub(crate) fn test_arm_replace_encode_poison_once(
        &mut self,
        poison: fn(CollectionId, &mut BTreeMap<String, RowValue>),
    ) {
        self.test_poison_planned_replace_row = Some(poison);
    }

    #[cfg(test)]
    #[doc(hidden)]
    pub(crate) fn test_arm_delete_encode_poison_once(
        &mut self,
        poison: fn(ScalarValue) -> ScalarValue,
    ) {
        self.test_poison_delete_encode_scalar = Some(poison);
    }

    #[cfg(test)]
    #[doc(hidden)]
    pub(crate) fn test_catalog_mut(&mut self) -> &mut Catalog {
        &mut self.catalog
    }

    /// Test helper: overwrite one cell in [`Self::latest`] without validation.
    #[cfg(test)]
    #[doc(hidden)]
    pub(crate) fn test_write_latest_cell_unchecked(
        &mut self,
        collection_id: CollectionId,
        pk: &ScalarValue,
        field: &str,
        value: RowValue,
    ) {
        let pk_key = pk.canonical_key_bytes();
        let row = self
            .latest
            .get_mut(&(collection_id.0, pk_key))
            .expect("test_write_latest_cell_unchecked: unknown row key");
        row.insert(field.to_string(), value);
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
    crate::schema_compat::validate_model_fields_against_catalog(
        col,
        T::primary_field(),
        &T::fields(),
        &T::indexes(),
    )
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

pub(crate) fn row_value_at_path_segments(
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
        if opts.mode == OpenMode::ReadOnly && writer_registry::is_writable_open(&path) {
            if let Some(shared) = handle_registry::get(&path) {
                let state = {
                    let g = shared.read().map_err(|_| {
                        DbError::Io(std::io::Error::other("shared database lock poisoned"))
                    })?;
                    Arc::clone(&*g)
                };
                let db = Self {
                    path: path.clone(),
                    store: FileStore::open_locked(&path, OpenMode::ReadOnly)?,
                    catalog: state.catalog.clone(),
                    segment_start: state.segment_start,
                    format_minor: state.format_minor,
                    latest: state.latest.clone(),
                    indexes: state.indexes.clone(),
                    txn_seq: 0,
                    txn_staging: None,
                    writer_registry: None,
                    shared_mirror: Some(shared),
                    read_only_attached: true,
                    recovery_info: OpenRecoveryInfo::default(),
                    #[cfg(test)]
                    test_poison_planned_replace_row: None,
                    #[cfg(test)]
                    test_poison_delete_encode_scalar: None,
                };
                return Ok(db);
            }
        }
        let store = FileStore::open_locked(&path, opts.mode)?;
        let mut db = Self::open_with_store(path.clone(), store, opts)?;
        if opts.mode == OpenMode::ReadWrite {
            db.writer_registry = Some(writer_registry::WriterRegistryGuard::new(path.clone())?);
            db.shared_mirror = Some(handle_registry::register(
                &path,
                handle_registry::SharedDbState {
                    catalog: db.catalog.clone(),
                    latest: db.latest.clone(),
                    indexes: db.indexes.clone(),
                    segment_start: db.segment_start,
                    format_minor: db.format_minor,
                    generation: 0,
                },
            )?);
        }
        Ok(db)
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
        Self::export_snapshot_to_path_with_fsops(&StdFsOps, dest_path, &self.snapshot_bytes())
    }

    pub(crate) fn export_snapshot_to_path_with_fsops(
        fs: &dyn fs_ops::FsOps,
        dest_path: impl AsRef<Path>,
        bytes: &[u8],
    ) -> Result<(), DbError> {
        fs.write(dest_path.as_ref(), bytes).map_err(DbError::Io)?;
        Ok(())
    }

    /// Open an in-memory database from a snapshot file.
    pub fn open_snapshot_path(path: impl AsRef<Path>) -> Result<Self, DbError> {
        let bytes = StdFsOps.read(path.as_ref()).map_err(DbError::Io)?;
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
