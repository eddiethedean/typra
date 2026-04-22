//! Database handle and orchestration: [`Database`] composes internal helpers for
//! bootstrap (`open`), scan/replay (`replay`), segment writes (`write`), and naming (`helpers`).

mod helpers;
mod open;
mod replay;
mod write;

use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};

use crate::catalog::{encode_catalog_payload, Catalog, CatalogRecordWire};
use crate::error::{DbError, FormatError, SchemaError};
use crate::record::{encode_record_payload_v1, ScalarValue};
use crate::schema::{CollectionId, FieldDef, SchemaVersion};
use crate::storage::{FileStore, Store, VecStore};

pub(crate) type LatestMap = HashMap<(u32, Vec<u8>), BTreeMap<String, ScalarValue>>;

/// Typra database handle backed by a [`Store`] (file or in-memory).
pub struct Database<S: Store = FileStore> {
    path: PathBuf,
    store: S,
    catalog: Catalog,
    segment_start: u64,
    /// Format minor read from the file header (lazy bumps `3` → `4` → `5`).
    format_minor: u16,
    /// Latest row per `(collection_id, canonical_pk_bytes)` (replay order / last wins).
    latest: LatestMap,
}

impl<S: Store> Database<S> {
    fn open_with_store(path: PathBuf, store: S) -> Result<Self, DbError> {
        open::open_with_store(path, store)
    }

    /// Path passed to [`Database::open`](Database::<FileStore>::open) or `":memory:"` for in-memory stores.
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// In-memory view of the persisted schema catalog.
    pub fn catalog(&self) -> &Catalog {
        &self.catalog
    }

    /// Registered collection names (sorted).
    pub fn collection_names(&self) -> Vec<String> {
        self.catalog.collection_names()
    }

    /// Resolve a registered collection id by name (trimmed).
    pub fn collection_id_named(&self, name: &str) -> Result<CollectionId, DbError> {
        self.catalog
            .lookup_name(name)
            .ok_or(DbError::Schema(SchemaError::UnknownCollectionName {
                name: name.trim().to_string(),
            }))
    }

    /// Register a new collection with schema version `1` and a **top-level** primary key field name.
    pub fn register_collection(
        &mut self,
        name: &str,
        fields: Vec<FieldDef>,
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
        let id = self.catalog.next_collection_id().0;
        let wire = CatalogRecordWire::CreateCollection {
            collection_id: id,
            name: name.clone(),
            schema_version: 1,
            fields,
            primary_field: Some(pk.to_string()),
        };
        let payload = encode_catalog_payload(&wire);
        write::append_schema_segment_and_publish(
            &mut self.store,
            self.segment_start,
            &mut self.format_minor,
            &payload,
        )?;
        self.catalog.apply_record(wire)?;
        Ok((CollectionId(id), SchemaVersion(1)))
    }

    /// Append a new schema version for an existing collection (`current + 1` required).
    pub fn register_schema_version(
        &mut self,
        id: CollectionId,
        fields: Vec<FieldDef>,
    ) -> Result<SchemaVersion, DbError> {
        let current = self
            .catalog
            .get(id)
            .ok_or(DbError::Schema(SchemaError::UnknownCollection { id: id.0 }))?;
        let next_v = current.current_version.0.saturating_add(1);
        let wire = CatalogRecordWire::NewSchemaVersion {
            collection_id: id.0,
            schema_version: next_v,
            fields,
        };
        let payload = encode_catalog_payload(&wire);
        write::append_schema_segment_and_publish(
            &mut self.store,
            self.segment_start,
            &mut self.format_minor,
            &payload,
        )?;
        self.catalog.apply_record(wire)?;
        Ok(SchemaVersion(next_v))
    }

    /// Insert or replace the latest row for `collection_id` (same primary key).
    pub fn insert(
        &mut self,
        collection_id: CollectionId,
        mut row: BTreeMap<String, ScalarValue>,
    ) -> Result<(), DbError> {
        write::ensure_header_v0_5(&mut self.store, &mut self.format_minor)?;
        let (payload, full) = {
            let col = self.catalog.get(collection_id).ok_or(DbError::Schema(
                SchemaError::UnknownCollection {
                    id: collection_id.0,
                },
            ))?;
            for f in &col.fields {
                if f.path.0.len() != 1 {
                    return Err(DbError::NotImplemented);
                }
            }
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
            let pk_val =
                row.remove(pk_name)
                    .ok_or(DbError::Schema(SchemaError::RowMissingPrimary {
                        name: pk_name.to_string(),
                    }))?;
            if !pk_val.ty_matches(pk_ty) {
                return Err(DbError::Format(FormatError::RecordPayloadTypeMismatch));
            }
            let mut non_pk: Vec<(FieldDef, ScalarValue)> = Vec::new();
            for def in &col.fields {
                let seg = def.path.0[0].as_ref();
                if seg == pk_name {
                    continue;
                }
                let v = row
                    .remove(seg)
                    .ok_or(DbError::Schema(SchemaError::RowMissingField {
                        name: seg.to_string(),
                    }))?;
                if !v.ty_matches(&def.ty) {
                    return Err(DbError::Format(FormatError::RecordPayloadTypeMismatch));
                }
                non_pk.push((def.clone(), v));
            }
            if !row.is_empty() {
                let name = row.keys().next().unwrap().clone();
                return Err(DbError::Schema(SchemaError::RowUnknownField { name }));
            }

            let payload = encode_record_payload_v1(
                collection_id.0,
                col.current_version.0,
                &pk_val,
                pk_ty,
                &non_pk,
            )?;
            let mut full_map: BTreeMap<String, ScalarValue> = BTreeMap::new();
            full_map.insert(pk_name.to_string(), pk_val.clone());
            for (def, v) in non_pk {
                full_map.insert(def.path.0[0].to_string(), v);
            }
            let pk_key = pk_val.canonical_key_bytes();
            (payload, (pk_key, full_map))
        };
        write::append_record_segment_and_publish(
            &mut self.store,
            self.segment_start,
            &mut self.format_minor,
            &payload,
        )?;
        self.latest.insert((collection_id.0, full.0), full.1);
        Ok(())
    }

    /// Get the latest row by primary key, or `None` if missing.
    pub fn get(
        &self,
        collection_id: CollectionId,
        pk: &ScalarValue,
    ) -> Result<Option<BTreeMap<String, ScalarValue>>, DbError> {
        let col = self.catalog.get(collection_id).ok_or(DbError::Schema(
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
            return Err(DbError::Format(FormatError::RecordPayloadTypeMismatch));
        }
        let key = (collection_id.0, pk.canonical_key_bytes());
        Ok(self.latest.get(&key).cloned())
    }
}

impl Database<FileStore> {
    /// Open or create a database at `path`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, DbError> {
        let path = path.as_ref().to_path_buf();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;
        let store = FileStore::new(file);
        Self::open_with_store(path, store)
    }
}

impl Database<VecStore> {
    /// Empty in-memory database (same layout as a new file, held in a [`VecStore`]).
    pub fn open_in_memory() -> Result<Self, DbError> {
        Self::open_with_store(PathBuf::from(":memory:"), VecStore::new())
    }

    /// Open from snapshot bytes produced by [`into_snapshot_bytes`](Self::into_snapshot_bytes).
    pub fn from_snapshot_bytes(bytes: Vec<u8>) -> Result<Self, DbError> {
        Self::open_with_store(PathBuf::from(":memory:"), VecStore::from_vec(bytes))
    }

    /// Consume the database and return the raw file image.
    pub fn into_snapshot_bytes(self) -> Vec<u8> {
        self.store.into_inner()
    }

    /// Copy of the full on-disk image (same bytes as [`into_snapshot_bytes`](Self::into_snapshot_bytes)).
    pub fn snapshot_bytes(&self) -> Vec<u8> {
        self.store.as_slice().to_vec()
    }
}

#[cfg(test)]
mod tests {
    use super::Database;
    use crate::db::open;
    use crate::error::FormatError;
    use crate::file_format::{FileHeader, FILE_HEADER_SIZE};
    use crate::schema::{FieldDef, Type};
    use crate::segments::header::{SegmentHeader, SegmentType};
    use crate::segments::writer::SegmentWriter;
    use crate::storage::{FileStore, Store};
    use crate::superblock::{Superblock, SUPERBLOCK_SIZE};
    use crate::DbError;
    use std::borrow::Cow;

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
        }
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
}
