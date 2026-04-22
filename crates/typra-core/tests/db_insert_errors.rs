//! `Database::insert` / `get` / `collection_id_named` error paths and header bump.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fs;

use typra_core::error::{DbError, FormatError, SchemaError};
use typra_core::file_format::{decode_header, FILE_HEADER_SIZE, FORMAT_MINOR};
use typra_core::record::{RowValue, ScalarValue};
use typra_core::schema::{FieldDef, FieldPath, Type};
use typra_core::CollectionId;
use typra_core::Database;

fn title() -> FieldDef {
    FieldDef {
        path: FieldPath(vec![Cow::Owned("title".to_string())]),
        ty: Type::String,
        constraints: vec![],
    }
}

fn year() -> FieldDef {
    FieldDef {
        path: FieldPath(vec![Cow::Owned("year".to_string())]),
        ty: Type::Int64,
        constraints: vec![],
    }
}

#[test]
fn collection_id_named_unknown_errors() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open(dir.path().join("x.typra")).unwrap();
    let e = db.collection_id_named("nope").unwrap_err();
    assert!(matches!(
        e,
        DbError::Schema(SchemaError::UnknownCollectionName { name }) if name == "nope"
    ));
}

#[test]
fn insert_row_unknown_field_errors() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = Database::open(dir.path().join("u.typra")).unwrap();
    let (id, _) = db
        .register_collection("b", vec![title(), year()], "title")
        .unwrap();
    let mut row = BTreeMap::new();
    row.insert("title".into(), RowValue::String("t".into()));
    row.insert("year".into(), RowValue::Int64(1));
    row.insert("extra".into(), RowValue::Int64(0));
    let e = db.insert(id, row).unwrap_err();
    assert!(matches!(e, DbError::Validation(_)));
}

#[test]
fn insert_missing_non_pk_field_errors() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = Database::open(dir.path().join("m.typra")).unwrap();
    let (id, _) = db
        .register_collection("b", vec![title(), year()], "title")
        .unwrap();
    let mut row = BTreeMap::new();
    row.insert("title".into(), RowValue::String("t".into()));
    let e = db.insert(id, row).unwrap_err();
    assert!(matches!(e, DbError::Validation(_)));
}

#[test]
fn insert_pk_type_mismatch_errors() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = Database::open(dir.path().join("p.typra")).unwrap();
    let (id, _) = db.register_collection("b", vec![title()], "title").unwrap();
    let mut row = BTreeMap::new();
    row.insert("title".into(), RowValue::Int64(1));
    let e = db.insert(id, row).unwrap_err();
    assert!(matches!(e, DbError::Validation(_)));
}

#[test]
fn get_pk_type_mismatch_errors() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = Database::open(dir.path().join("g.typra")).unwrap();
    let (id, _) = db.register_collection("b", vec![title()], "title").unwrap();
    let e = db.get(id, &ScalarValue::Int64(1)).unwrap_err();
    assert!(matches!(
        e,
        DbError::Format(FormatError::RecordPayloadTypeMismatch)
    ));
}

#[test]
fn insert_nested_path_schema_not_implemented() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = Database::open(dir.path().join("n.typra")).unwrap();
    let nested = FieldDef {
        path: FieldPath(vec![Cow::Owned("a".into()), Cow::Owned("b".into())]),
        ty: Type::String,
        constraints: vec![],
    };
    let (id, _) = db
        .register_collection("x", vec![nested, title()], "title")
        .unwrap();
    let mut row = BTreeMap::new();
    row.insert("title".into(), RowValue::String("t".into()));
    let e = db.insert(id, row).unwrap_err();
    assert!(matches!(e, DbError::NotImplemented));
}

#[test]
fn lazy_header_v4_to_v5_on_first_record_write() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bump.typra");
    let mut header = [0u8; FILE_HEADER_SIZE];
    header[0..4].copy_from_slice(b"TDB0");
    header[4..6].copy_from_slice(&0u16.to_le_bytes());
    header[6..8].copy_from_slice(&2u16.to_le_bytes());
    header[8..12].copy_from_slice(&(FILE_HEADER_SIZE as u32).to_le_bytes());
    fs::write(&path, header).unwrap();

    {
        let mut db = Database::open(&path).unwrap();
        db.register_collection("books", vec![title(), year()], "title")
            .unwrap();
        let bytes = fs::read(&path).unwrap();
        let h = decode_header(&bytes[..FILE_HEADER_SIZE]).unwrap();
        assert_eq!(h.format_minor, 4);

        let mut row = BTreeMap::new();
        row.insert("title".into(), RowValue::String("Rust".into()));
        row.insert("year".into(), RowValue::Int64(2024));
        db.insert(CollectionId(1), row).unwrap();
    }
    let bytes = fs::read(&path).unwrap();
    let h = decode_header(&bytes[..FILE_HEADER_SIZE]).unwrap();
    assert_eq!(h.format_minor, 5);
}

#[test]
fn new_database_starts_at_format_minor_5() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("new.typra");
    let _db = Database::open(&path).unwrap();
    let bytes = fs::read(&path).unwrap();
    let h = decode_header(&bytes[..FILE_HEADER_SIZE]).unwrap();
    assert_eq!(h.format_minor, FORMAT_MINOR);
}
