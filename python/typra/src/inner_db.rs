//! Dispatches engine calls to either [`typra_core::storage::FileStore`] or [`typra_core::storage::VecStore`].

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use std::collections::BTreeMap;

use typra_core::query::Query;
use typra_core::record::{RowValue, ScalarValue};
use typra_core::schema::{CollectionId, FieldDef, IndexDef, SchemaVersion};
use typra_core::storage::{FileStore, VecStore};
use typra_core::Database as CoreDatabase;

pub(crate) enum InnerDb {
    File(CoreDatabase<FileStore>),
    Mem(CoreDatabase<VecStore>),
}

impl InnerDb {
    pub(crate) fn register_collection_with_indexes(
        &mut self,
        name: &str,
        fields: Vec<FieldDef>,
        indexes: Vec<IndexDef>,
        primary_field: &str,
    ) -> Result<(CollectionId, SchemaVersion), typra_core::DbError> {
        match self {
            InnerDb::File(d) => {
                d.register_collection_with_indexes(name, fields, indexes, primary_field)
            }
            InnerDb::Mem(d) => {
                d.register_collection_with_indexes(name, fields, indexes, primary_field)
            }
        }
    }

    pub(crate) fn collection_names(&self) -> Vec<String> {
        match self {
            InnerDb::File(d) => d.collection_names(),
            InnerDb::Mem(d) => d.collection_names(),
        }
    }

    pub(crate) fn collection_id_named(
        &self,
        name: &str,
    ) -> Result<CollectionId, typra_core::DbError> {
        match self {
            InnerDb::File(d) => d.collection_id_named(name),
            InnerDb::Mem(d) => d.collection_id_named(name),
        }
    }

    pub(crate) fn catalog(&self) -> &typra_core::Catalog {
        match self {
            InnerDb::File(d) => d.catalog(),
            InnerDb::Mem(d) => d.catalog(),
        }
    }

    pub(crate) fn insert(
        &mut self,
        id: CollectionId,
        row: BTreeMap<String, RowValue>,
    ) -> Result<(), typra_core::DbError> {
        match self {
            InnerDb::File(d) => d.insert(id, row),
            InnerDb::Mem(d) => d.insert(id, row),
        }
    }

    pub(crate) fn get(
        &self,
        id: CollectionId,
        pk: &ScalarValue,
    ) -> Result<Option<BTreeMap<String, RowValue>>, typra_core::DbError> {
        match self {
            InnerDb::File(d) => d.get(id, pk),
            InnerDb::Mem(d) => d.get(id, pk),
        }
    }

    pub(crate) fn query(
        &self,
        q: &Query,
    ) -> Result<Vec<BTreeMap<String, RowValue>>, typra_core::DbError> {
        match self {
            InnerDb::File(d) => d.query(q),
            InnerDb::Mem(d) => d.query(q),
        }
    }

    pub(crate) fn explain_query(&self, q: &Query) -> Result<String, typra_core::DbError> {
        match self {
            InnerDb::File(d) => d.explain_query(q),
            InnerDb::Mem(d) => d.explain_query(q),
        }
    }

    pub(crate) fn path_display(&self) -> String {
        match self {
            InnerDb::File(d) => d.path().display().to_string(),
            InnerDb::Mem(d) => d.path().display().to_string(),
        }
    }

    pub(crate) fn snapshot_bytes(&self) -> Result<Vec<u8>, PyErr> {
        match self {
            InnerDb::File(_) => Err(PyValueError::new_err(
                "snapshot_bytes is only supported for in-memory databases",
            )),
            InnerDb::Mem(d) => Ok(d.snapshot_bytes()),
        }
    }

    pub(crate) fn begin_transaction(&mut self) -> Result<(), typra_core::DbError> {
        match self {
            InnerDb::File(d) => d.begin_transaction(),
            InnerDb::Mem(d) => d.begin_transaction(),
        }
    }

    pub(crate) fn commit_transaction(&mut self) -> Result<(), typra_core::DbError> {
        match self {
            InnerDb::File(d) => d.commit_transaction(),
            InnerDb::Mem(d) => d.commit_transaction(),
        }
    }

    pub(crate) fn rollback_transaction(&mut self) {
        match self {
            InnerDb::File(d) => d.rollback_transaction(),
            InnerDb::Mem(d) => d.rollback_transaction(),
        }
    }
}
