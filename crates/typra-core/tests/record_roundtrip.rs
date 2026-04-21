//! Insert / get / reopen and in-memory snapshot parity (0.5.0).

use std::borrow::Cow;
use std::collections::BTreeMap;

use typra_core::record::ScalarValue;
use typra_core::schema::{FieldDef, FieldPath, Type};
use typra_core::CollectionId;
use typra_core::Database;

fn title_field() -> FieldDef {
    FieldDef {
        path: FieldPath(vec![Cow::Owned("title".to_string())]),
        ty: Type::String,
    }
}

fn year_field() -> FieldDef {
    FieldDef {
        path: FieldPath(vec![Cow::Owned("year".to_string())]),
        ty: Type::Int64,
    }
}

#[test]
fn insert_get_reopen_disk() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("r.typra");
    {
        let mut db = Database::open(&path).unwrap();
        let (id, _) = db
            .register_collection("books", vec![title_field(), year_field()], "title")
            .unwrap();
        let mut row = BTreeMap::new();
        row.insert("title".to_string(), ScalarValue::String("Rust".to_string()));
        row.insert("year".to_string(), ScalarValue::Int64(2024));
        db.insert(id, row).unwrap();
    }
    let db = Database::open(&path).unwrap();
    let id = CollectionId(1);
    let pk = ScalarValue::String("Rust".to_string());
    let got = db.get(id, &pk).unwrap().expect("row");
    assert_eq!(
        got.get("title"),
        Some(&ScalarValue::String("Rust".to_string()))
    );
    assert_eq!(got.get("year"), Some(&ScalarValue::Int64(2024)));
}

#[test]
fn replay_last_insert_wins() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("w.typra");
    {
        let mut db = Database::open(&path).unwrap();
        let (id, _) = db
            .register_collection("t", vec![title_field(), year_field()], "title")
            .unwrap();
        for y in [1i64, 2, 3] {
            let mut row = BTreeMap::new();
            row.insert("title".to_string(), ScalarValue::String("k".to_string()));
            row.insert("year".to_string(), ScalarValue::Int64(y));
            db.insert(id, row).unwrap();
        }
    }
    let db = Database::open(&path).unwrap();
    let got = db
        .get(CollectionId(1), &ScalarValue::String("k".to_string()))
        .unwrap()
        .expect("row");
    assert_eq!(got.get("year"), Some(&ScalarValue::Int64(3)));
}

#[test]
fn mem_snapshot_roundtrip() {
    let (id, snap) = {
        let mut db = Database::open_in_memory().unwrap();
        let (id, _) = db
            .register_collection("books", vec![title_field(), year_field()], "title")
            .unwrap();
        let mut row = BTreeMap::new();
        row.insert("title".to_string(), ScalarValue::String("Mem".to_string()));
        row.insert("year".to_string(), ScalarValue::Int64(2025));
        db.insert(id, row).unwrap();
        (id, db.snapshot_bytes())
    };
    let db2 = Database::from_snapshot_bytes(snap).unwrap();
    let got = db2
        .get(id, &ScalarValue::String("Mem".to_string()))
        .unwrap()
        .expect("row");
    assert_eq!(
        got.get("title"),
        Some(&ScalarValue::String("Mem".to_string()))
    );
}
