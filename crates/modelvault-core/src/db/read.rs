//! Read paths: queries, catalog views, and collection handles.

use std::marker::PhantomData;
use std::path::Path;
use std::sync::Arc;

use crate::catalog::Catalog;
use crate::config::OpenRecoveryInfo;
use crate::error::{DbError, SchemaError};
use crate::index::IndexState;
use crate::record::{RowValue, ScalarValue};
use crate::schema::CollectionId;
use crate::storage::Store;

use super::{validate_subset_model, Collection, Database, LatestMap};

impl<S: Store> Database<S> {
    pub(crate) fn live_shared_snapshot(
        &self,
    ) -> Result<Arc<super::handle_registry::SharedDbState>, DbError> {
        let shared = self.shared_mirror.as_ref().ok_or_else(|| {
            DbError::Io(std::io::Error::other(
                "read-only attached handle missing shared state",
            ))
        })?;
        let g = shared
            .read()
            .map_err(|_| DbError::Io(std::io::Error::other("shared database lock poisoned")))?;
        Ok(Arc::clone(&*g))
    }

    pub(crate) fn with_live_snapshot<R>(
        &self,
        f: impl FnOnce(&Catalog, &IndexState, &LatestMap) -> R,
    ) -> Result<R, DbError> {
        if self.read_only_attached {
            let state = self.live_shared_snapshot()?;
            return Ok(f(&state.catalog, &state.indexes, &state.latest));
        }
        if let Some(st) = &self.txn_staging {
            Ok(f(&st.shadow_catalog, &st.shadow_indexes, &st.shadow_latest))
        } else {
            Ok(f(&self.catalog, &self.indexes, &self.latest))
        }
    }

    pub(crate) fn catalog_for_read(&self) -> &Catalog {
        if let Some(st) = &self.txn_staging {
            &st.shadow_catalog
        } else {
            &self.catalog
        }
    }

    pub(crate) fn indexes_for_read(&self) -> &IndexState {
        if let Some(st) = &self.txn_staging {
            &st.shadow_indexes
        } else {
            &self.indexes
        }
    }

    pub(crate) fn latest_for_read(&self) -> &LatestMap {
        if let Some(st) = &self.txn_staging {
            &st.shadow_latest
        } else {
            &self.latest
        }
    }

    /// Path passed to [`Database::open`](super::Database::<crate::storage::FileStore>::open), or `":memory:"` for [`crate::storage::VecStore`].
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Recovery metadata from the most recent open (truncation, etc.).
    pub fn recovery_info(&self) -> &OpenRecoveryInfo {
        &self.recovery_info
    }

    /// Rebuild secondary indexes from `latest` rows and compare to replayed index state.
    pub fn verify_index_consistency(&self) -> Result<(), DbError> {
        self.with_live_snapshot(|catalog, indexes, latest| {
            super::verify_indexes_match_rows(catalog, latest, indexes)
        })?
    }

    /// Read-only view of the schema catalog built from `Schema` segments.
    ///
    /// On same-process attached read-only handles, prefer [`Self::collection_names`] and
    /// [`Self::collection_id_named`] for live metadata; this borrows the open-time snapshot.
    pub fn catalog(&self) -> &Catalog {
        self.catalog_for_read()
    }

    /// Clone of the live schema catalog (always current on attached read-only handles).
    pub fn snapshot_catalog(&self) -> Catalog {
        self.with_live_snapshot(|c, _, _| c.clone())
            .expect("live snapshot")
    }

    /// All registered collection names in lexicographic order.
    pub fn collection_names(&self) -> Vec<String> {
        self.with_live_snapshot(|catalog, _, _| catalog.collection_names())
            .expect("live snapshot")
    }

    /// Read-only access to the in-memory secondary index state (rebuilt from `Index` segments).
    ///
    /// On same-process attached read-only handles this returns the index state captured at open;
    /// use [`Self::snapshot_index_state`] for a live clone after writer updates.
    pub fn index_state(&self) -> &IndexState {
        self.indexes_for_read()
    }

    /// Clone of the live secondary index state (always current on attached read-only handles).
    pub fn snapshot_index_state(&self) -> IndexState {
        self.with_live_snapshot(|_, indexes, _| indexes.clone())
            .expect("live snapshot")
    }

    /// Execute a query against the current in-memory snapshot of the database.
    pub fn query(
        &self,
        q: &crate::query::Query,
    ) -> Result<Vec<std::collections::BTreeMap<String, RowValue>>, DbError> {
        self.with_live_snapshot(|catalog, indexes, latest| {
            crate::query::execute_query(catalog, indexes, latest, q)
        })?
    }

    /// Return a human-readable explanation of the chosen plan for `q`.
    pub fn explain_query(&self, q: &crate::query::Query) -> Result<String, DbError> {
        self.with_live_snapshot(|catalog, _, _| crate::query::explain_query(catalog, q))?
    }

    /// Lazy iterator over query rows (same semantics as [`Self::query`]).
    ///
    /// See [`crate::query::QueryRowIter`] — this is the v0.7 pull-based execution boundary, not a
    /// full operator graph.
    pub fn query_iter(
        &self,
        q: &crate::query::Query,
    ) -> Result<crate::query::QueryRowIter<'_>, DbError> {
        if self.read_only_attached {
            let snapshot = self.live_shared_snapshot()?;
            return crate::query::execute_query_iter_owned(snapshot, q, Some(self.path.as_path()));
        }
        crate::query::execute_query_iter_with_spill_path(
            self.catalog_for_read(),
            self.indexes_for_read(),
            self.latest_for_read(),
            q,
            Some(self.path.as_path()),
        )
    }

    /// Typed handle over a registered collection; `T` may be a *subset model*.
    pub fn collection<'a, T: crate::schema::DbModel>(
        &'a self,
    ) -> Result<Collection<'a, S, T>, DbError> {
        let cid = self.collection_id_named(T::collection_name())?;
        self.with_live_snapshot(|catalog, _, _| {
            let col = catalog
                .get(cid)
                .ok_or(DbError::Schema(SchemaError::UnknownCollection {
                    id: cid.0,
                }))?;
            validate_subset_model::<T>(col)?;
            Ok(Collection {
                db: self,
                collection_id: cid,
                _marker: PhantomData,
            })
        })?
    }

    /// Look up [`CollectionId`] by collection name (leading/trailing whitespace trimmed).
    ///
    /// Returns [`SchemaError::UnknownCollectionName`] when the name is not registered.
    pub fn collection_id_named(&self, name: &str) -> Result<CollectionId, DbError> {
        self.with_live_snapshot(|catalog, _, _| {
            catalog
                .lookup_name(name)
                .ok_or(DbError::Schema(SchemaError::UnknownCollectionName {
                    name: name.trim().to_string(),
                }))
        })?
    }

    /// Return the latest stored row for `pk`, or `None` if no insert has been replayed for that key.
    ///
    /// `pk` must match the declared primary field’s [`crate::schema::Type`].
    pub fn get(
        &self,
        collection_id: CollectionId,
        pk: &ScalarValue,
    ) -> Result<Option<std::collections::BTreeMap<String, RowValue>>, DbError> {
        self.with_live_snapshot(|catalog, _, latest| {
            let col = catalog.get(collection_id).ok_or(DbError::Schema(
                SchemaError::UnknownCollection {
                    id: collection_id.0,
                },
            ))?;
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
                return Err(DbError::Schema(SchemaError::PrimaryKeyTypeMismatch {
                    collection_id: collection_id.0,
                }));
            }
            let key = (collection_id.0, pk.canonical_key_bytes());
            Ok(latest.get(&key).cloned())
        })?
    }
}
