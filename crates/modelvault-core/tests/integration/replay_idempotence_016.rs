use modelvault_core::schema::{FieldDef, FieldPath, Type};
use modelvault_core::{Database, RowValue};
use std::borrow::Cow;
use std::collections::BTreeMap;

#[test]
fn replay_idempotence_open_reopen_same_rows() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("reopen.modelvault");
    {
        let mut db = Database::open(&path).unwrap();
        let fields = vec![FieldDef {
            path: FieldPath(vec![Cow::Borrowed("id")]),
            ty: Type::Int64,
            constraints: vec![],
        }];
        let (cid, _) = db
            .register_collection_with_indexes("items", fields, vec![], "id")
            .unwrap();
        db.insert(
            cid,
            BTreeMap::from([("id".to_string(), RowValue::Int64(1))]),
        )
        .unwrap();
        db.insert(
            cid,
            BTreeMap::from([("id".to_string(), RowValue::Int64(2))]),
        )
        .unwrap();
    }
    let db1 = Database::open(&path).unwrap();
    let cid = db1.collection_id_named("items").unwrap();
    assert_eq!(
        db1.query(&modelvault_core::query::Query {
            collection: cid,
            predicate: None,
            order_by: None,
            limit: None,
        })
        .unwrap()
        .len(),
        2
    );
    drop(db1);

    let db2 = Database::open(&path).unwrap();
    let cid = db2.collection_id_named("items").unwrap();
    assert!(db2
        .get(cid, &modelvault_core::ScalarValue::Int64(1))
        .unwrap()
        .is_some());
    assert_eq!(
        db2.query(&modelvault_core::query::Query {
            collection: cid,
            predicate: None,
            order_by: None,
            limit: None,
        })
        .unwrap()
        .len(),
        2
    );
}

#[test]
fn attach_detach_lifecycle_writer_close_then_strict_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("attach.modelvault");
    {
        let mut writer = Database::open(&path).unwrap();
        let fields = vec![FieldDef {
            path: FieldPath(vec![Cow::Borrowed("id")]),
            ty: Type::Int64,
            constraints: vec![],
        }];
        let (cid, _) = writer
            .register_collection_with_indexes("items", fields, vec![], "id")
            .unwrap();
        let reader = Database::open_read_only(&path).unwrap();
        writer
            .insert(
                cid,
                BTreeMap::from([("id".to_string(), RowValue::Int64(7))]),
            )
            .unwrap();
        assert!(reader
            .get(cid, &modelvault_core::ScalarValue::Int64(7))
            .unwrap()
            .is_some());
    }
    let db = Database::open(&path).unwrap();
    let cid = db.collection_id_named("items").unwrap();
    assert!(db
        .get(cid, &modelvault_core::ScalarValue::Int64(7))
        .unwrap()
        .is_some());
}

#[test]
fn checkpoint_then_reopen_preserves_rows() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cp.modelvault");
    let cid = {
        let mut db = Database::open(&path).unwrap();
        let fields = vec![FieldDef {
            path: FieldPath(vec![Cow::Borrowed("id")]),
            ty: Type::Int64,
            constraints: vec![],
        }];
        let (cid, _) = db
            .register_collection_with_indexes("items", fields, vec![], "id")
            .unwrap();
        db.insert(
            cid,
            BTreeMap::from([("id".to_string(), RowValue::Int64(3))]),
        )
        .unwrap();
        db.checkpoint().unwrap();
        cid
    };
    let db = Database::open(&path).unwrap();
    assert!(db
        .get(cid, &modelvault_core::ScalarValue::Int64(3))
        .unwrap()
        .is_some());
}
