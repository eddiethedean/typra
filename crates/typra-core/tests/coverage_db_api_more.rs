use std::borrow::Cow;
use std::collections::BTreeMap;

use typra_core::record::RowValue;
use typra_core::schema::{FieldDef, FieldPath, IndexDef, IndexKind, Type};
use typra_core::{CollectionId, Database, DbError, ScalarValue};

fn field(name: &str, ty: Type) -> FieldDef {
    FieldDef {
        path: FieldPath(vec![Cow::Owned(name.to_string())]),
        ty,
        constraints: vec![],
    }
}

#[test]
fn transaction_rollback_discards_staged_writes() {
    let mut db = Database::open_in_memory().unwrap();
    db.register_collection("books", vec![field("title", Type::String)], "title")
        .unwrap();
    let cid = CollectionId(1);

    db.begin_transaction().unwrap();
    let mut r = BTreeMap::new();
    r.insert("title".into(), RowValue::String("a".into()));
    db.insert(cid, r).unwrap();

    // Read-your-writes inside the txn.
    assert!(db
        .get(cid, &ScalarValue::String("a".into()))
        .unwrap()
        .is_some());

    db.rollback_transaction();

    // Rolled back: no data.
    assert!(db
        .get(cid, &ScalarValue::String("a".into()))
        .unwrap()
        .is_none());
}

#[test]
fn collection_names_and_unknown_collection_id_named_error() {
    let mut db = Database::open_in_memory().unwrap();
    db.register_collection("books", vec![field("title", Type::String)], "title")
        .unwrap();

    assert_eq!(db.collection_names(), vec!["books".to_string()]);
    assert_eq!(db.collection_id_named("books").unwrap(), CollectionId(1));

    let e = db.collection_id_named("nope").unwrap_err();
    assert!(matches!(e, DbError::Schema(_)));
}

#[test]
fn unique_index_violation_returns_error_and_does_not_mutate() {
    let mut db = Database::open_in_memory().unwrap();
    let fields = vec![field("id", Type::Int64), field("email", Type::String)];
    let indexes = vec![IndexDef {
        name: "email_u".to_string(),
        path: FieldPath(vec![Cow::Borrowed("email")]),
        kind: IndexKind::Unique,
    }];
    let (cid, _) = db
        .register_collection_with_indexes("accounts", fields, indexes, "id")
        .unwrap();

    let mut r1 = BTreeMap::new();
    r1.insert("id".into(), RowValue::Int64(1));
    r1.insert("email".into(), RowValue::String("a@example.test".into()));
    db.insert(cid, r1).unwrap();

    let mut r2 = BTreeMap::new();
    r2.insert("id".into(), RowValue::Int64(2));
    r2.insert("email".into(), RowValue::String("a@example.test".into()));
    let e = db.insert(cid, r2).unwrap_err();
    assert!(matches!(e, DbError::Schema(_)));

    // Second insert should not be visible.
    assert!(db.get(cid, &ScalarValue::Int64(2)).unwrap().is_none());
}

#[test]
fn from_snapshot_bytes_rejects_empty_buffer() {
    // An empty snapshot is treated as a new empty DB; a *truncated* header must fail.
    let e = match Database::from_snapshot_bytes(vec![0u8; 1]) {
        Ok(_) => panic!("expected open from truncated snapshot to fail"),
        Err(e) => e,
    };
    assert!(matches!(e, DbError::Format(_)));
}
