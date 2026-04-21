//! Edge cases for `Database::register_collection` / names / version chains.

use std::borrow::Cow;

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

fn id_field() -> FieldDef {
    FieldDef {
        path: typra_core::schema::FieldPath(vec![Cow::Owned("id".to_string())]),
        ty: Type::Int64,
    }
}

#[test]
fn register_rejects_empty_name_after_trim() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("e.typra");
    let mut db = Database::open(&path).unwrap();
    let err = db.register_collection("", vec![], "id");
    assert!(matches!(
        err,
        Err(DbError::Schema(SchemaError::InvalidCollectionName))
    ));
    let err = db.register_collection("   ", vec![], "id");
    assert!(matches!(
        err,
        Err(DbError::Schema(SchemaError::InvalidCollectionName))
    ));
}

#[test]
fn register_trims_whitespace_around_name() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("trim.typra");
    let mut db = Database::open(&path).unwrap();
    db.register_collection("  books  ", vec![title_field()], "title")
        .unwrap();
    assert_eq!(db.collection_names(), vec!["books".to_string()]);
}

#[test]
fn register_accepts_max_length_collection_name() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("max.typra");
    let mut db = Database::open(&path).unwrap();
    let name = "a".repeat(1023);
    assert_eq!(name.len(), 1023);
    db.register_collection(&name, vec![title_field()], "title")
        .unwrap();
    assert_eq!(db.collection_names(), vec![name.clone()]);
    drop(db);
    let db = Database::open(&path).unwrap();
    assert_eq!(db.collection_names(), vec![name]);
}

#[test]
fn register_rejects_name_longer_than_max() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("long.typra");
    let mut db = Database::open(&path).unwrap();
    let name = "b".repeat(1024);
    assert_eq!(name.len(), 1024);
    let err = db.register_collection(&name, vec![], "id");
    assert!(matches!(
        err,
        Err(DbError::Schema(SchemaError::InvalidCollectionName))
    ));
}

#[test]
fn schema_version_chain_through_v3_reopens_clean() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("chain.typra");
    {
        let mut db = Database::open(&path).unwrap();
        db.register_collection("t", vec![title_field()], "title")
            .unwrap();
        db.register_schema_version(typra_core::schema::CollectionId(1), vec![title_field()])
            .unwrap();
        db.register_schema_version(typra_core::schema::CollectionId(1), vec![title_field()])
            .unwrap();
        assert_eq!(
            db.catalog()
                .get(typra_core::schema::CollectionId(1))
                .unwrap()
                .current_version
                .0,
            3
        );
    }
    let db = Database::open(&path).unwrap();
    assert_eq!(
        db.catalog()
            .get(typra_core::schema::CollectionId(1))
            .unwrap()
            .current_version
            .0,
        3
    );
}

#[test]
fn multiple_collections_stable_ids_after_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("multi.typra");
    {
        let mut db = Database::open(&path).unwrap();
        db.register_collection("a", vec![id_field()], "id").unwrap();
        db.register_collection("b", vec![id_field()], "id").unwrap();
        db.register_collection("c", vec![id_field()], "id").unwrap();
    }
    let db = Database::open(&path).unwrap();
    assert_eq!(db.collection_names(), vec!["a", "b", "c"]);
    assert!(db
        .catalog()
        .get(typra_core::schema::CollectionId(1))
        .is_some());
    assert!(db
        .catalog()
        .get(typra_core::schema::CollectionId(2))
        .is_some());
    assert!(db
        .catalog()
        .get(typra_core::schema::CollectionId(3))
        .is_some());
}
