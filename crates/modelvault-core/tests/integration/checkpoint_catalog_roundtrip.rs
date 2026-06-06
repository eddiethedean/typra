use modelvault_core::schema::{FieldDef, FieldPath, Type};
use modelvault_core::{Database, RowValue};
use std::borrow::Cow;
use std::collections::BTreeMap;

#[test]
fn checkpoint_reopen_preserves_bumped_schema_version() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cp.modelvault");
    let mut db = Database::open(&path).unwrap();
    let v1_fields = vec![FieldDef {
        path: FieldPath(vec![Cow::Borrowed("id")]),
        ty: Type::Int64,
        constraints: vec![],
    }];
    let (cid, _) = db
        .register_collection_with_indexes("items", v1_fields, vec![], "id")
        .unwrap();
    db.insert(
        cid,
        BTreeMap::from([("id".to_string(), RowValue::Int64(1))]),
    )
    .unwrap();

    let v2_fields = vec![
        FieldDef {
            path: FieldPath(vec![Cow::Borrowed("id")]),
            ty: Type::Int64,
            constraints: vec![],
        },
        FieldDef {
            path: FieldPath(vec![Cow::Borrowed("note")]),
            ty: Type::Optional(Box::new(Type::String)),
            constraints: vec![],
        },
    ];
    db.register_schema_version_with_indexes(cid, v2_fields, vec![])
        .unwrap();
    db.checkpoint().unwrap();
    drop(db);

    let reopened = Database::open_read_only(&path).unwrap();
    let rid = reopened.collection_id_named("items").unwrap();
    assert_eq!(reopened.catalog().get(rid).unwrap().current_version.0, 2);
    reopened.verify_index_consistency().unwrap();
    assert!(reopened
        .get(cid, &modelvault_core::ScalarValue::Int64(1))
        .unwrap()
        .is_some());
}
