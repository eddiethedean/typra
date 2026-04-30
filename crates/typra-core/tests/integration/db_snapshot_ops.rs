use std::borrow::Cow;
use std::collections::BTreeMap;

use typra_core::schema::{FieldDef, FieldPath, Type};
use typra_core::{Database, RowValue, ScalarValue};

fn field(name: &str, ty: Type) -> FieldDef {
    FieldDef {
        path: FieldPath(vec![Cow::Owned(name.to_string())]),
        ty,
        constraints: vec![],
    }
}

#[test]
fn export_and_restore_snapshot_file_roundtrips() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src.typra");
    let snapshot = dir.path().join("snapshot.typra");
    let restored = dir.path().join("restored.typra");

    // Create a small on-disk DB.
    {
        let mut db = Database::open(&src).unwrap();
        let (cid, _) = db
            .register_collection("books", vec![field("id", Type::Int64)], "id")
            .unwrap();
        db.insert(
            cid,
            BTreeMap::from([("id".to_string(), RowValue::Int64(7))]),
        )
        .unwrap();

        // Writes a checkpoint and copies the DB bytes.
        db.export_snapshot_to_path(&snapshot).unwrap();
    }

    // Ensure `dest_path.exists()` branch is taken by creating the destination file first.
    {
        let _ = Database::open(&restored).unwrap();
    }

    Database::restore_snapshot_to_path(&snapshot, &restored).unwrap();

    let db2 = Database::open(&restored).unwrap();
    let cid = db2.collection_id_named("books").unwrap();
    let got = db2.get(cid, &ScalarValue::Int64(7)).unwrap().unwrap();
    assert_eq!(got.get("id"), Some(&RowValue::Int64(7)));
}

#[test]
fn restore_snapshot_to_new_destination_roundtrips() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src.typra");
    let snapshot = dir.path().join("snapshot.typra");
    let restored = dir.path().join("restored.typra");

    {
        let mut db = Database::open(&src).unwrap();
        let (cid, _) = db
            .register_collection("books", vec![field("id", Type::Int64)], "id")
            .unwrap();
        db.insert(
            cid,
            BTreeMap::from([("id".to_string(), RowValue::Int64(8))]),
        )
        .unwrap();
        db.export_snapshot_to_path(&snapshot).unwrap();
    }

    // Destination does not exist yet.
    Database::restore_snapshot_to_path(&snapshot, &restored).unwrap();

    let db2 = Database::open(&restored).unwrap();
    let cid = db2.collection_id_named("books").unwrap();
    let got = db2.get(cid, &ScalarValue::Int64(8)).unwrap().unwrap();
    assert_eq!(got.get("id"), Some(&RowValue::Int64(8)));
}

#[test]
fn vecstore_export_and_open_snapshot_path_roundtrips() {
    let dir = tempfile::tempdir().unwrap();
    let snapshot = dir.path().join("vec.snap");

    let mut db = Database::open_in_memory().unwrap();
    let (cid, _) = db
        .register_collection("items", vec![field("k", Type::String)], "k")
        .unwrap();
    db.insert(
        cid,
        BTreeMap::from([("k".to_string(), RowValue::String("v".to_string()))]),
    )
    .unwrap();

    db.export_snapshot_to_path(&snapshot).unwrap();

    let db2 = Database::open_snapshot_path(&snapshot).unwrap();
    let cid2 = db2.collection_id_named("items").unwrap();
    let got = db2
        .get(cid2, &ScalarValue::String("v".to_string()))
        .unwrap()
        .unwrap();
    assert_eq!(
        got.get("k"),
        Some(&RowValue::String("v".to_string()))
    );
}

