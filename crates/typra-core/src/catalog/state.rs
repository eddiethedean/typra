//! In-memory catalog state; must match replay from on-disk schema segments.

use std::collections::HashMap;

use crate::catalog::codec::{CatalogRecordWire, MAX_COLLECTION_NAME_BYTES};
use crate::error::{DbError, SchemaError};
use crate::schema::{CollectionId, FieldDef, SchemaVersion};

/// Snapshot of one registered collection (latest schema version).
#[derive(Debug, Clone, PartialEq)]
pub struct CollectionInfo {
    pub id: CollectionId,
    pub name: String,
    pub current_version: SchemaVersion,
    pub fields: Vec<FieldDef>,
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

    /// Apply one catalog record (from replay or after a local append).
    pub fn apply_record(&mut self, record: CatalogRecordWire) -> Result<(), DbError> {
        match record {
            CatalogRecordWire::CreateCollection {
                collection_id,
                name,
                schema_version,
                fields,
            } => self.apply_create(collection_id, name, schema_version, fields),
            CatalogRecordWire::NewSchemaVersion {
                collection_id,
                schema_version,
                fields,
            } => self.apply_new_version(collection_id, schema_version, fields),
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
        let id = CollectionId(collection_id);
        let info = CollectionInfo {
            id,
            name: name.clone(),
            current_version: SchemaVersion(1),
            fields,
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
        col.current_version = SchemaVersion(schema_version);
        col.fields = fields;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::codec::encode_catalog_payload;
    use crate::schema::{FieldPath, Type};
    use std::borrow::Cow;

    fn path(parts: &[&str]) -> crate::schema::FieldPath {
        FieldPath(parts.iter().map(|s| Cow::Owned(s.to_string())).collect())
    }

    #[test]
    fn apply_create_then_version() {
        let mut c = Catalog::default();
        let w = CatalogRecordWire::CreateCollection {
            collection_id: 1,
            name: "a".to_string(),
            schema_version: 1,
            fields: vec![],
        };
        c.apply_record(w).unwrap();
        assert_eq!(c.next_collection_id(), CollectionId(2));
        let w2 = CatalogRecordWire::NewSchemaVersion {
            collection_id: 1,
            schema_version: 2,
            fields: vec![FieldDef {
                path: path(&["x"]),
                ty: Type::Int64,
            }],
        };
        c.apply_record(w2).unwrap();
        assert_eq!(
            c.get(CollectionId(1)).unwrap().current_version,
            SchemaVersion(2)
        );
    }

    #[test]
    fn duplicate_name_rejected() {
        let mut c = Catalog::default();
        c.apply_record(CatalogRecordWire::CreateCollection {
            collection_id: 1,
            name: "a".to_string(),
            schema_version: 1,
            fields: vec![],
        })
        .unwrap();
        let err = c.apply_record(CatalogRecordWire::CreateCollection {
            collection_id: 2,
            name: "a".to_string(),
            schema_version: 1,
            fields: vec![],
        });
        assert!(matches!(
            err,
            Err(DbError::Schema(SchemaError::DuplicateCollectionName { .. }))
        ));
    }

    #[test]
    fn new_schema_version_cannot_skip_numbers() {
        let mut c = Catalog::default();
        c.apply_record(CatalogRecordWire::CreateCollection {
            collection_id: 1,
            name: "a".to_string(),
            schema_version: 1,
            fields: vec![],
        })
        .unwrap();
        let err = c.apply_record(CatalogRecordWire::NewSchemaVersion {
            collection_id: 1,
            schema_version: 3,
            fields: vec![],
        });
        assert!(matches!(
            err,
            Err(DbError::Schema(SchemaError::InvalidSchemaVersion {
                expected: 2,
                got: 3
            }))
        ));
    }

    #[test]
    fn encode_then_apply_matches() {
        let w = CatalogRecordWire::CreateCollection {
            collection_id: 1,
            name: "b".to_string(),
            schema_version: 1,
            fields: vec![],
        };
        let bytes = encode_catalog_payload(&w);
        let mut c = Catalog::default();
        c.apply_record(crate::catalog::codec::decode_catalog_payload(&bytes).unwrap())
            .unwrap();
        assert_eq!(c.collection_names(), vec!["b".to_string()]);
    }
}
