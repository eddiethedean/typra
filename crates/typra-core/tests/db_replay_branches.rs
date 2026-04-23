//! Additional `Database::open` / replay / registration branches for coverage.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fs::OpenOptions;

use typra_core::error::{DbError, FormatError, SchemaError};
use typra_core::file_format::FileHeader;
use typra_core::record::{RowValue, ScalarValue};
use typra_core::schema::{FieldDef, FieldPath, Type};
use typra_core::segments::header::{SegmentHeader, SegmentType};
use typra_core::segments::writer::SegmentWriter;
use typra_core::storage::{FileStore, Store};
use typra_core::Database;

fn id_field() -> FieldDef {
    FieldDef {
        path: FieldPath(vec![Cow::Owned("id".to_string())]),
        ty: Type::Int64,
        constraints: vec![],
    }
}

fn title_field() -> FieldDef {
    FieldDef {
        path: FieldPath(vec![Cow::Owned("title".to_string())]),
        ty: Type::String,
        constraints: vec![],
    }
}

#[test]
fn open_rejects_unsupported_format_minor() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bad_minor.typra");
    let mut h = FileHeader::new_v0_5();
    h.format_minor = 99;
    std::fs::write(&path, h.encode()).unwrap();
    let e = match Database::open(&path) {
        Err(e) => e,
        Ok(_) => panic!("expected unsupported version"),
    };
    assert!(matches!(
        e,
        DbError::Format(FormatError::UnsupportedVersion {
            major: 0,
            minor: 99
        })
    ));
}

#[test]
fn register_rejects_whitespace_only_primary_field() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = Database::open(dir.path().join("pk.typra")).unwrap();
    let e = db
        .register_collection("c", vec![id_field(), title_field()], "   ")
        .unwrap_err();
    assert!(matches!(
        e,
        DbError::Schema(SchemaError::InvalidCollectionName)
    ));
}

#[test]
fn register_rejects_primary_field_missing_from_schema() {
    let mut db = Database::open_in_memory().unwrap();
    let e = db
        .register_collection("c", vec![title_field()], "id")
        .unwrap_err();
    assert!(matches!(
        e,
        DbError::Schema(SchemaError::PrimaryFieldNotFound { .. })
    ));
}

#[test]
fn insert_rejects_non_pk_type_mismatch() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = Database::open(dir.path().join("ty.typra")).unwrap();
    let (id, _) = db
        .register_collection("c", vec![id_field(), title_field()], "id")
        .unwrap();
    let mut row = BTreeMap::new();
    row.insert("id".into(), RowValue::Int64(1));
    row.insert("title".into(), RowValue::Int64(2));
    let e = db.insert(id, row).unwrap_err();
    assert!(matches!(e, DbError::Validation(_)));
}

#[test]
fn into_snapshot_bytes_roundtrips_like_snapshot_bytes() {
    let mut db = Database::open_in_memory().unwrap();
    let (id, _) = db
        .register_collection("c", vec![id_field(), title_field()], "id")
        .unwrap();
    let mut row = BTreeMap::new();
    row.insert("id".into(), RowValue::Int64(7));
    row.insert("title".into(), RowValue::String("snap".into()));
    db.insert(id, row).unwrap();
    let snap = db.into_snapshot_bytes();
    let db2 = Database::from_snapshot_bytes(snap).unwrap();
    let got = db2.get(id, &ScalarValue::Int64(7)).unwrap().expect("row");
    assert_eq!(
        got.get("title"),
        Some(&RowValue::String("snap".to_string()))
    );
}

#[test]
fn replay_errors_when_record_schema_version_behind_catalog() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ver.typra");
    {
        let mut db = Database::open(&path).unwrap();
        let (id, _) = db
            .register_collection("c", vec![id_field(), title_field()], "id")
            .unwrap();
        let mut row = BTreeMap::new();
        row.insert("id".into(), RowValue::Int64(1));
        row.insert("title".into(), RowValue::String("a".into()));
        db.insert(id, row).unwrap();
        db.register_schema_version(id, vec![id_field(), title_field()])
            .unwrap();
    }
    let e = match Database::open(&path) {
        Err(e) => e,
        Ok(_) => panic!("expected schema version mismatch on replay"),
    };
    assert!(matches!(
        e,
        DbError::Schema(SchemaError::InvalidSchemaVersion {
            expected: 2,
            got: 1
        })
    ));
}

#[test]
fn replay_errors_on_nested_field_paths_in_schema() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("nest.typra");
    {
        let mut db = Database::open(&path).unwrap();
        let (id, _) = db
            .register_collection("c", vec![id_field(), title_field()], "id")
            .unwrap();
        let mut row = BTreeMap::new();
        row.insert("id".into(), RowValue::Int64(1));
        row.insert("title".into(), RowValue::String("a".into()));
        db.insert(id, row).unwrap();
        let nested = FieldDef {
            path: FieldPath(vec![
                Cow::Owned("outer".to_string()),
                Cow::Owned("inner".to_string()),
            ]),
            ty: Type::Int64,
            constraints: vec![],
        };
        db.register_schema_version(id, vec![id_field(), title_field(), nested])
            .unwrap();
    }
    let e = match Database::open(&path) {
        Err(e) => e,
        Ok(_) => panic!("expected NotImplemented for nested paths"),
    };
    assert!(matches!(e, DbError::NotImplemented));
}

#[test]
fn open_rejects_record_segment_with_short_payload_body() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("short_rec.typra");
    {
        let mut db = Database::open(&path).unwrap();
        let (id, _) = db
            .register_collection("c", vec![id_field(), title_field()], "id")
            .unwrap();
        let mut row = BTreeMap::new();
        row.insert("id".into(), RowValue::Int64(1));
        row.insert("title".into(), RowValue::String("a".into()));
        db.insert(id, row).unwrap();
    }
    let file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    let mut store = FileStore::new(file);
    let len = store.len().unwrap();
    let mut w = SegmentWriter::new(&mut store, len);
    // Format minor 6 uses transaction framing; wrap the invalid record bytes in a committed txn.
    let begin = typra_core::txn::encode_txn_payload_v0(999);
    w.append(
        SegmentHeader {
            segment_type: SegmentType::TxnBegin,
            payload_len: 0,
            payload_crc32c: 0,
        },
        begin.as_slice(),
    )
    .unwrap();
    w.append(
        SegmentHeader {
            segment_type: SegmentType::Record,
            payload_len: 0,
            payload_crc32c: 0,
        },
        &[1, 2, 3],
    )
    .unwrap();
    let commit = typra_core::txn::encode_txn_payload_v0(999);
    w.append(
        SegmentHeader {
            segment_type: SegmentType::TxnCommit,
            payload_len: 0,
            payload_crc32c: 0,
        },
        commit.as_slice(),
    )
    .unwrap();
    drop(store);
    let e = match Database::open(&path) {
        Err(e) => e,
        Ok(_) => panic!("expected truncated record payload error"),
    };
    assert!(matches!(
        e,
        DbError::Format(FormatError::TruncatedRecordPayload)
    ));
}
