//! In-memory catalog: maps names to ids, tracks schema versions, and applies replayed records.
//!
//! State must match the sequence of `Schema` segments on disk.

use std::collections::HashMap;

use crate::catalog::codec::{CatalogRecordWire, MAX_COLLECTION_NAME_BYTES};
use crate::error::{DbError, SchemaError};
use crate::schema::{validate_field_defs, CollectionId, FieldDef, IndexDef, SchemaVersion};

/// Snapshot of one registered collection (latest schema version).
#[derive(Debug, Clone, PartialEq)]
pub struct CollectionInfo {
    pub id: CollectionId,
    pub name: String,
    pub current_version: SchemaVersion,
    pub fields: Vec<FieldDef>,
    pub indexes: Vec<IndexDef>,
    /// Single top-level field name for the primary key (`None` for legacy catalog v1 segments).
    pub primary_field: Option<String>,
}

/// Logical catalog: collection names, ids, and current schema version per collection.
#[derive(Debug, Clone)]
pub struct Catalog {
    by_id: HashMap<u32, CollectionInfo>,
    by_name: HashMap<String, CollectionId>,
    /// Next `CollectionId` to assign on `create` (starts at `1` when empty).
    next_id: u32,
}

impl Default for Catalog {
    fn default() -> Self {
        Self {
            by_id: HashMap::new(),
            by_name: HashMap::new(),
            next_id: 1,
        }
    }
}

impl Catalog {
    /// Next collection id that will be assigned (replay must produce sequential creates `1..n`).
    pub fn next_collection_id(&self) -> CollectionId {
        CollectionId(self.next_id)
    }

    pub fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }

    pub fn len(&self) -> usize {
        self.by_id.len()
    }

    pub fn get(&self, id: CollectionId) -> Option<&CollectionInfo> {
        self.by_id.get(&id.0)
    }

    /// Resolve a registered collection by name (trimmed, matching [`Database::register_collection`](crate::db::Database::register_collection)).
    pub fn lookup_name(&self, name: &str) -> Option<CollectionId> {
        self.by_name.get(name.trim()).copied()
    }

    pub fn collection_names(&self) -> Vec<String> {
        let mut names: Vec<String> = self.by_name.keys().cloned().collect();
        names.sort();
        names
    }

    pub fn collections(&self) -> Vec<CollectionInfo> {
        let mut v: Vec<CollectionInfo> = self.by_id.values().cloned().collect();
        v.sort_by_key(|c| c.id.0);
        v
    }

    /// `true` if `name` is a single-segment path on a top-level field.
    pub fn has_top_level_field(fields: &[FieldDef], name: &str) -> bool {
        fields
            .iter()
            .any(|f| f.path.0.len() == 1 && f.path.0[0] == name)
    }

    /// Apply one catalog record (from replay or after a local append).
    pub fn apply_record(&mut self, record: CatalogRecordWire) -> Result<(), DbError> {
        match record {
            CatalogRecordWire::CreateCollection {
                collection_id,
                name,
                schema_version,
                fields,
                indexes,
                primary_field,
            } => self.apply_create(
                collection_id,
                name,
                schema_version,
                fields,
                indexes,
                primary_field,
            ),
            CatalogRecordWire::NewSchemaVersion {
                collection_id,
                schema_version,
                fields,
                indexes,
            } => self.apply_new_version(collection_id, schema_version, fields, indexes),
        }
    }

    fn validate_name(name: &str) -> Result<(), DbError> {
        if name.is_empty() {
            return Err(DbError::Schema(SchemaError::InvalidCollectionName));
        }
        if name.len() > MAX_COLLECTION_NAME_BYTES {
            return Err(DbError::Schema(SchemaError::InvalidCollectionName));
        }
        Ok(())
    }

    fn apply_create(
        &mut self,
        collection_id: u32,
        name: String,
        schema_version: u32,
        fields: Vec<FieldDef>,
        indexes: Vec<IndexDef>,
        primary_field: Option<String>,
    ) -> Result<(), DbError> {
        Self::validate_name(&name)?;
        if schema_version != 1 {
            return Err(DbError::Schema(SchemaError::InvalidSchemaVersion {
                expected: 1,
                got: schema_version,
            }));
        }
        if collection_id != self.next_id {
            return Err(DbError::Schema(SchemaError::UnexpectedCollectionId {
                expected: self.next_id,
                got: collection_id,
            }));
        }
        if self.by_name.contains_key(&name) {
            return Err(DbError::Schema(SchemaError::DuplicateCollectionName {
                name: name.clone(),
            }));
        }
        validate_field_defs(&fields)?;
        if let Some(ref pk) = primary_field {
            if !Catalog::has_top_level_field(&fields, pk) {
                return Err(DbError::Schema(SchemaError::PrimaryFieldNotFound {
                    name: pk.clone(),
                }));
            }
        }
        let id = CollectionId(collection_id);
        let info = CollectionInfo {
            id,
            name: name.clone(),
            current_version: SchemaVersion(1),
            fields,
            indexes,
            primary_field,
        };
        self.by_id.insert(collection_id, info);
        self.by_name.insert(name, id);
        self.next_id = collection_id.saturating_add(1);
        Ok(())
    }

    fn apply_new_version(
        &mut self,
        collection_id: u32,
        schema_version: u32,
        fields: Vec<FieldDef>,
        indexes: Vec<IndexDef>,
    ) -> Result<(), DbError> {
        let col = self.by_id.get_mut(&collection_id).ok_or(DbError::Schema(
            SchemaError::UnknownCollection { id: collection_id },
        ))?;
        let expected = col.current_version.0.saturating_add(1);
        if schema_version != expected {
            return Err(DbError::Schema(SchemaError::InvalidSchemaVersion {
                expected,
                got: schema_version,
            }));
        }
        validate_field_defs(&fields)?;
        if let Some(ref pk) = col.primary_field {
            if !Catalog::has_top_level_field(&fields, pk) {
                return Err(DbError::Schema(SchemaError::PrimaryFieldMissingInSchema {
                    name: pk.clone(),
                }));
            }
        }
        col.current_version = SchemaVersion(schema_version);
        col.fields = fields;
        col.indexes = indexes;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/unit/src_catalog_state_tests.rs"
    ));
}
