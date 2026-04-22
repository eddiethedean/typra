//! Backend selection for the Python [`crate::database::Database`] wrapper (file vs in-memory).

use pyo3::exceptions::PyValueError;
use pyo3::prelude::*;
use std::collections::BTreeMap;

use typra_core::record::ScalarValue;
use typra_core::schema::{CollectionId, FieldDef, SchemaVersion};
use typra_core::storage::{FileStore, VecStore};
use typra_core::Database as CoreDatabase;

pub(crate) enum InnerDb {
    File(CoreDatabase<FileStore>),
    Mem(CoreDatabase<VecStore>),
}

impl InnerDb {
    pub(crate) fn register_collection(
        &mut self,
        name: &str,
        fields: Vec<FieldDef>,
        primary_field: &str,
    ) -> Result<(CollectionId, SchemaVersion), typra_core::DbError> {
        match self {
            InnerDb::File(d) => d.register_collection(name, fields, primary_field),
            InnerDb::Mem(d) => d.register_collection(name, fields, primary_field),
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
        row: BTreeMap<String, ScalarValue>,
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
    ) -> Result<Option<BTreeMap<String, ScalarValue>>, typra_core::DbError> {
        match self {
            InnerDb::File(d) => d.get(id, pk),
            InnerDb::Mem(d) => d.get(id, pk),
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
}
