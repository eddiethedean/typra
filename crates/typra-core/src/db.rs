use std::collections::{BTreeMap, HashMap};
use std::path::{Path, PathBuf};

use crate::catalog::{
    decode_catalog_payload, encode_catalog_payload, Catalog, CatalogRecordWire,
    MAX_COLLECTION_NAME_BYTES,
};
use crate::error::{DbError, FormatError, SchemaError};
use crate::file_format::{decode_header, FileHeader, FILE_HEADER_SIZE};
use crate::manifest::decode_manifest_v0;
use crate::publish::append_manifest_and_publish;
use crate::record::{decode_record_payload_v1, encode_record_payload_v1, ScalarValue};
use crate::schema::{CollectionId, FieldDef, SchemaVersion};
use crate::segments::header::{SegmentHeader, SegmentType};
use crate::segments::reader::{read_segment_header_at, read_segment_payload, scan_segments};
use crate::segments::writer::SegmentWriter;
use crate::storage::{FileStore, Store, VecStore};
use crate::superblock::{decode_superblock, Superblock, SUPERBLOCK_SIZE};

type LatestMap = HashMap<(u32, Vec<u8>), BTreeMap<String, ScalarValue>>;

/// Typra database handle backed by a [`Store`](crate::storage::Store) (file or in-memory).
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
    fn open_with_store(path: PathBuf, mut store: S) -> Result<Self, DbError> {
        let len = store.len()?;
        let segment_start = (FILE_HEADER_SIZE + 2 * SUPERBLOCK_SIZE) as u64;

        let format_minor: u16;

        if len == 0 {
            let header = FileHeader::new_v0_5();
            format_minor = header.format_minor;
            store.write_all_at(0, &header.encode())?;
            Self::init_superblocks(&mut store, segment_start)?;
            let _ = append_manifest_and_publish(&mut store, segment_start)?;
            store.sync()?;
        } else if len < FILE_HEADER_SIZE as u64 {
            return Err(DbError::Format(FormatError::TruncatedHeader {
                got: len as usize,
                expected: FILE_HEADER_SIZE,
            }));
        } else {
            let mut buf = [0u8; FILE_HEADER_SIZE];
            store.read_exact_at(0, &mut buf)?;
            let header = decode_header(&buf)?;

            if header.format_minor == 2 {
                if len == FILE_HEADER_SIZE as u64 {
                    let upgraded = FileHeader::new_v0_3();
                    store.write_all_at(0, &upgraded.encode())?;
                    Self::init_superblocks(&mut store, segment_start)?;
                    let _ = append_manifest_and_publish(&mut store, segment_start)?;
                    store.sync()?;
                    format_minor = crate::file_format::FORMAT_MINOR_V3;
                } else {
                    return Err(DbError::Format(FormatError::UnsupportedVersion {
                        major: header.format_major,
                        minor: header.format_minor,
                    }));
                }
            } else if header.format_minor == 3
                || header.format_minor == 4
                || header.format_minor == 5
            {
                if len < segment_start {
                    return Err(DbError::Format(FormatError::TruncatedSuperblock {
                        got: len as usize,
                        expected: segment_start as usize,
                    }));
                }
                let selected = Self::read_and_select_superblock(&mut store)?;
                if selected.manifest_offset != 0 {
                    let _ = Self::read_manifest(&mut store, &selected);
                }
                if len > segment_start {
                    let _ = scan_segments(&mut store, segment_start)?;
                }
                format_minor = header.format_minor;
            } else {
                return Err(DbError::Format(FormatError::UnsupportedVersion {
                    major: header.format_major,
                    minor: header.format_minor,
                }));
            }
        }

        let catalog = if len == 0 {
            Catalog::default()
        } else {
            Self::load_catalog(&mut store, segment_start)?
        };

        let latest = if len == 0 {
            HashMap::new()
        } else {
            Self::load_latest_rows(&mut store, segment_start, &catalog)?
        };

        let db = Self {
            path,
            store,
            catalog,
            segment_start,
            format_minor,
            latest,
        };
        Ok(db)
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
        let name = Self::normalize_collection_name(name)?;
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
        self.append_schema_segment_and_publish(&payload)?;
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
        self.append_schema_segment_and_publish(&payload)?;
        self.catalog.apply_record(wire)?;
        Ok(SchemaVersion(next_v))
    }

    fn normalize_collection_name(name: &str) -> Result<String, DbError> {
        let name = name.trim();
        if name.is_empty() {
            return Err(DbError::Schema(SchemaError::InvalidCollectionName));
        }
        if name.len() > MAX_COLLECTION_NAME_BYTES {
            return Err(DbError::Schema(SchemaError::InvalidCollectionName));
        }
        Ok(name.to_string())
    }

    /// Insert or replace the latest row for `collection_id` (same primary key).
    pub fn insert(
        &mut self,
        collection_id: CollectionId,
        mut row: BTreeMap<String, ScalarValue>,
    ) -> Result<(), DbError> {
        self.ensure_header_v0_5()?;
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
            let mut full: BTreeMap<String, ScalarValue> = BTreeMap::new();
            full.insert(pk_name.to_string(), pk_val.clone());
            for (def, v) in non_pk {
                full.insert(def.path.0[0].to_string(), v);
            }
            let pk_key = pk_val.canonical_key_bytes();
            (payload, (pk_key, full))
        };
        self.append_record_segment_and_publish(&payload)?;
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

    fn append_schema_segment_and_publish(&mut self, payload: &[u8]) -> Result<(), DbError> {
        self.ensure_header_v0_4()?;
        let file_len = self.store.len()?;
        let mut writer = SegmentWriter::new(&mut self.store, file_len.max(self.segment_start));
        writer.append(
            SegmentHeader {
                segment_type: SegmentType::Schema,
                payload_len: 0,
                payload_crc32c: 0,
            },
            payload,
        )?;
        let _ = append_manifest_and_publish(&mut self.store, self.segment_start)?;
        self.store.sync()?;
        Ok(())
    }

    fn append_record_segment_and_publish(&mut self, payload: &[u8]) -> Result<(), DbError> {
        self.ensure_header_v0_5()?;
        let file_len = self.store.len()?;
        let mut writer = SegmentWriter::new(&mut self.store, file_len.max(self.segment_start));
        writer.append(
            SegmentHeader {
                segment_type: SegmentType::Record,
                payload_len: 0,
                payload_crc32c: 0,
            },
            payload,
        )?;
        let _ = append_manifest_and_publish(&mut self.store, self.segment_start)?;
        self.store.sync()?;
        Ok(())
    }

    fn ensure_header_v0_4(&mut self) -> Result<(), DbError> {
        if self.format_minor >= crate::file_format::FORMAT_MINOR_V4 {
            return Ok(());
        }
        let mut buf = [0u8; FILE_HEADER_SIZE];
        self.store.read_exact_at(0, &mut buf)?;
        let mut h = decode_header(&buf)?;
        h.format_minor = crate::file_format::FORMAT_MINOR_V4;
        self.store.write_all_at(0, &h.encode())?;
        self.format_minor = crate::file_format::FORMAT_MINOR_V4;
        self.store.sync()?;
        Ok(())
    }

    fn ensure_header_v0_5(&mut self) -> Result<(), DbError> {
        if self.format_minor >= crate::file_format::FORMAT_MINOR {
            return Ok(());
        }
        let mut buf = [0u8; FILE_HEADER_SIZE];
        self.store.read_exact_at(0, &mut buf)?;
        let mut h = decode_header(&buf)?;
        h.format_minor = crate::file_format::FORMAT_MINOR;
        self.store.write_all_at(0, &h.encode())?;
        self.format_minor = crate::file_format::FORMAT_MINOR;
        self.store.sync()?;
        Ok(())
    }

    fn load_catalog(store: &mut S, segment_start: u64) -> Result<Catalog, DbError> {
        let metas = scan_segments(store, segment_start)?;
        let mut catalog = Catalog::default();
        for meta in metas {
            if meta.header.segment_type != SegmentType::Schema {
                continue;
            }
            let payload = read_segment_payload(store, &meta)?;
            let record = decode_catalog_payload(&payload)?;
            catalog.apply_record(record)?;
        }
        Ok(catalog)
    }

    fn load_latest_rows(
        store: &mut S,
        segment_start: u64,
        catalog: &Catalog,
    ) -> Result<LatestMap, DbError> {
        let metas = scan_segments(store, segment_start)?;
        let mut latest = HashMap::new();
        for meta in metas {
            if meta.header.segment_type != SegmentType::Record {
                continue;
            }
            let payload = read_segment_payload(store, &meta)?;
            if payload.len() < 6 {
                return Err(DbError::Format(FormatError::TruncatedRecordPayload));
            }
            let collection_id =
                u32::from_le_bytes([payload[2], payload[3], payload[4], payload[5]]);
            let col = catalog
                .get(CollectionId(collection_id))
                .ok_or(DbError::Schema(SchemaError::UnknownCollection {
                    id: collection_id,
                }))?;
            let pk_name = match &col.primary_field {
                Some(s) => s.as_str(),
                None => continue,
            };
            for f in &col.fields {
                if f.path.0.len() != 1 {
                    return Err(DbError::NotImplemented);
                }
            }
            let pk_ty = col
                .fields
                .iter()
                .find(|f| f.path.0.len() == 1 && f.path.0[0] == pk_name)
                .map(|f| &f.ty)
                .ok_or(DbError::Schema(SchemaError::PrimaryFieldNotFound {
                    name: pk_name.to_string(),
                }))?;
            let decoded = decode_record_payload_v1(&payload, pk_name, pk_ty, &col.fields)?;
            if decoded.schema_version != col.current_version.0 {
                return Err(DbError::Schema(SchemaError::InvalidSchemaVersion {
                    expected: col.current_version.0,
                    got: decoded.schema_version,
                }));
            }
            let mut full: BTreeMap<String, ScalarValue> = BTreeMap::new();
            full.insert(pk_name.to_string(), decoded.pk.clone());
            for (k, v) in decoded.fields {
                full.insert(k, v);
            }
            latest.insert((collection_id, decoded.pk.canonical_key_bytes()), full);
        }
        Ok(latest)
    }

    fn init_superblocks(store: &mut impl Store, segment_start: u64) -> Result<(), DbError> {
        store.write_all_at(segment_start - 1, &[0u8])?;

        let sb_a = Superblock {
            generation: 1,
            ..Superblock::empty()
        };
        let sb_b = Superblock::empty();
        store.write_all_at(FILE_HEADER_SIZE as u64, &sb_a.encode())?;
        store.write_all_at((FILE_HEADER_SIZE + SUPERBLOCK_SIZE) as u64, &sb_b.encode())?;
        Ok(())
    }

    fn read_and_select_superblock(store: &mut impl Store) -> Result<Superblock, DbError> {
        let mut a = [0u8; SUPERBLOCK_SIZE];
        let mut b = [0u8; SUPERBLOCK_SIZE];
        store.read_exact_at(FILE_HEADER_SIZE as u64, &mut a)?;
        store.read_exact_at((FILE_HEADER_SIZE + SUPERBLOCK_SIZE) as u64, &mut b)?;

        let sa = decode_superblock(&a).ok();
        let sb = decode_superblock(&b).ok();
        match (sa, sb) {
            (Some(sa), Some(sb)) => Ok(if sa.generation >= sb.generation {
                sa
            } else {
                sb
            }),
            (Some(sa), None) => Ok(sa),
            (None, Some(sb)) => Ok(sb),
            (None, None) => Err(DbError::Format(FormatError::BadSuperblockChecksum)),
        }
    }

    fn read_manifest(store: &mut impl Store, sb: &Superblock) -> Result<(), DbError> {
        let (_, header) = read_segment_header_at(store, sb.manifest_offset)?;
        if header.segment_type != SegmentType::Manifest {
            return Err(DbError::Format(FormatError::UnsupportedVersion {
                major: 0,
                minor: 0,
            }));
        }
        let mut payload = vec![0u8; header.payload_len as usize];
        store.read_exact_at(
            sb.manifest_offset + crate::segments::header::SEGMENT_HEADER_LEN as u64,
            &mut payload,
        )?;
        let _m = decode_manifest_v0(&payload)?;
        Ok(())
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
    /// Empty in-memory database (same layout as a new file, held in a [`VecStore`](crate::storage::VecStore)).
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

        let res = Database::<FileStore>::read_and_select_superblock(&mut store);
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
        let res = Database::<FileStore>::read_manifest(&mut store, &sb);
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

        let selected = Database::<FileStore>::read_and_select_superblock(&mut store).unwrap();
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
