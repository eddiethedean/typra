//! Catalog persistence and registration semantics (0.4.0).

use std::borrow::Cow;
use std::fs;

use typra_core::file_format::{decode_header, FileHeader, FILE_HEADER_SIZE};
use typra_core::schema::{FieldDef, Type};
use typra_core::Database;
use typra_core::DbError;
use typra_core::SchemaError;

fn title_field() -> FieldDef {
    FieldDef {
        path: typra_core::schema::FieldPath(vec![Cow::Owned("title".to_string())]),
        ty: Type::String,
    }
}

#[test]
fn duplicate_collection_name_errors() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("d.typra");
    let mut db = Database::open(&path).unwrap();
    db.register_collection("x", vec![title_field()]).unwrap();
    let err = db.register_collection("x", vec![]);
    assert!(matches!(
        err,
        Err(DbError::Schema(SchemaError::DuplicateCollectionName { .. }))
    ));
}

#[test]
fn unknown_collection_id_errors() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("u.typra");
    let mut db = Database::open(&path).unwrap();
    db.register_collection("c", vec![title_field()]).unwrap();
    let err = db.register_schema_version(typra_core::schema::CollectionId(99), vec![]);
    assert!(matches!(
        err,
        Err(DbError::Schema(SchemaError::UnknownCollection { id: 99 }))
    ));
}

#[test]
fn replay_create_a_create_b_then_version_bump_a() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("order.typra");
    {
        let mut db = Database::open(&path).unwrap();
        db.register_collection("a", vec![title_field()]).unwrap();
        db.register_collection("b", vec![]).unwrap();
        assert_eq!(db.catalog().next_collection_id().0, 3);
        db.register_schema_version(typra_core::schema::CollectionId(1), vec![title_field()])
            .unwrap();
    }
    let db = Database::open(&path).unwrap();
    let a = db
        .catalog()
        .get(typra_core::schema::CollectionId(1))
        .unwrap();
    let b = db
        .catalog()
        .get(typra_core::schema::CollectionId(2))
        .unwrap();
    assert_eq!(a.name, "a");
    assert_eq!(a.current_version.0, 2);
    assert_eq!(b.name, "b");
    assert_eq!(b.current_version.0, 1);
    assert_eq!(
        db.collection_names(),
        vec!["a".to_string(), "b".to_string()]
    );
}

#[test]
fn register_schema_version_v2_then_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("v2.typra");
    {
        let mut db = Database::open(&path).unwrap();
        db.register_collection("c", vec![title_field()]).unwrap();
        let v = db
            .register_schema_version(typra_core::schema::CollectionId(1), vec![])
            .unwrap();
        assert_eq!(v.0, 2);
    }
    let db = Database::open(&path).unwrap();
    let c = db
        .catalog()
        .get(typra_core::schema::CollectionId(1))
        .unwrap();
    assert_eq!(c.current_version.0, 2);
}

#[test]
fn lazy_header_bump_from_v0_3_to_v0_4_on_register() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("legacy.typra");

    // v0.2 header-only file → open upgrades to v0.3 layout (see format_upgrade tests).
    let mut header = [0u8; FILE_HEADER_SIZE];
    header[0..4].copy_from_slice(b"TDB0");
    header[4..6].copy_from_slice(&0u16.to_le_bytes());
    header[6..8].copy_from_slice(&2u16.to_le_bytes());
    header[8..12].copy_from_slice(&(FILE_HEADER_SIZE as u32).to_le_bytes());
    fs::write(&path, header).unwrap();

    let mut db = Database::open(&path).unwrap();
    let bytes = fs::read(&path).unwrap();
    let h = decode_header(&bytes[..FILE_HEADER_SIZE]).unwrap();
    assert_eq!(h.format_minor, 3);

    db.register_collection("books", vec![title_field()])
        .unwrap();
    let bytes = fs::read(&path).unwrap();
    let h2 = decode_header(&bytes[..FILE_HEADER_SIZE]).unwrap();
    assert_eq!(h2.format_minor, 4);
}

#[test]
fn corrupt_catalog_payload_errors() {
    use typra_core::publish::append_manifest_and_publish;
    use typra_core::segments::header::{SegmentHeader, SegmentType};
    use typra_core::segments::writer::SegmentWriter;
    use typra_core::storage::{FileStore, Store};
    use typra_core::superblock::{Superblock, SUPERBLOCK_SIZE};

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bad.typra");
    let segment_start = (FILE_HEADER_SIZE + 2 * SUPERBLOCK_SIZE) as u64;

    let f = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&path)
        .unwrap();
    let mut store = FileStore::new(f);
    store
        .write_all_at(0, &FileHeader::new_v0_4().encode())
        .unwrap();
    store.write_all_at(segment_start - 1, &[0u8]).unwrap();
    let sb_a = Superblock {
        generation: 1,
        ..Superblock::empty()
    };
    store
        .write_all_at(
            typra_core::file_format::FILE_HEADER_SIZE as u64,
            &sb_a.encode(),
        )
        .unwrap();
    store
        .write_all_at(
            (typra_core::file_format::FILE_HEADER_SIZE + SUPERBLOCK_SIZE) as u64,
            &Superblock::empty().encode(),
        )
        .unwrap();
    append_manifest_and_publish(&mut store, segment_start).unwrap();
    let file_len = store.len().unwrap();
    let mut w = SegmentWriter::new(&mut store, file_len.max(segment_start));
    w.append(
        SegmentHeader {
            segment_type: SegmentType::Schema,
            payload_len: 0,
            payload_crc32c: 0,
        },
        b"not-a-catalog-payload",
    )
    .unwrap();
    append_manifest_and_publish(&mut store, segment_start).unwrap();
    store.sync().unwrap();
    drop(store);

    let res = Database::open(&path);
    assert!(res.is_err());
}
