use modelvault_core::schema::{FieldDef, FieldPath, IndexDef, IndexKind, Type};
use modelvault_core::{Database, RowValue};
use std::borrow::Cow;
use std::collections::BTreeMap;

#[test]
fn same_process_read_only_sees_writer_inserts() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("fresh.modelvault");
    let mut writer = Database::open(&path).unwrap();
    let fields = vec![FieldDef {
        path: FieldPath(vec![Cow::Borrowed("id")]),
        ty: Type::Int64,
        constraints: vec![],
    }];
    let (cid, _) = writer
        .register_collection_with_indexes("items", fields, vec![], "id")
        .unwrap();
    let mut row = BTreeMap::new();
    row.insert("id".to_string(), RowValue::Int64(1));
    writer.insert(cid, row).unwrap();

    let reader = Database::open_read_only(&path).unwrap();
    let got = reader
        .get(cid, &modelvault_core::ScalarValue::Int64(1))
        .unwrap();
    assert!(got.is_some());
}

#[test]
fn unique_index_absent_field_allows_multiple_rows() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("uniq.modelvault");
    let mut db = Database::open(&path).unwrap();
    let fields = vec![
        FieldDef {
            path: FieldPath(vec![Cow::Borrowed("id")]),
            ty: Type::Int64,
            constraints: vec![],
        },
        FieldDef {
            path: FieldPath(vec![Cow::Borrowed("email")]),
            ty: Type::Optional(Box::new(Type::String)),
            constraints: vec![],
        },
    ];
    let indexes = vec![IndexDef {
        name: "email_u".into(),
        path: FieldPath(vec![Cow::Borrowed("email")]),
        kind: IndexKind::Unique,
    }];
    let (cid, _) = db
        .register_collection_with_indexes("users", fields, indexes, "id")
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
    db.verify_index_consistency().unwrap();
}
