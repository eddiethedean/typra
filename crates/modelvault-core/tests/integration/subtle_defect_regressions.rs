use modelvault_core::query::{OrderBy, OrderDirection, Query};
use modelvault_core::schema::{FieldDef, FieldPath, IndexDef, IndexKind, Type};
use modelvault_core::{Database, RowValue};
use std::borrow::Cow;
use std::collections::BTreeMap;

#[test]
fn attached_ro_sees_post_attach_insert_via_get_query_and_query_iter() {
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

    let reader = Database::open_read_only(&path).unwrap();
    writer
        .insert(
            cid,
            BTreeMap::from([("id".to_string(), RowValue::Int64(42))]),
        )
        .unwrap();

    assert!(reader
        .get(cid, &modelvault_core::ScalarValue::Int64(42))
        .unwrap()
        .is_some());

    let q = Query {
        collection: cid,
        predicate: None,
        order_by: None,
        limit: None,
    };
    assert_eq!(reader.query(&q).unwrap().len(), 1);
    assert_eq!(reader.query_iter(&q).unwrap().count(), 1);
}

#[test]
fn attached_ro_sees_post_attach_register_collection() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("meta.modelvault");
    let mut writer = Database::open(&path).unwrap();
    let fields = vec![FieldDef {
        path: FieldPath(vec![Cow::Borrowed("id")]),
        ty: Type::Int64,
        constraints: vec![],
    }];
    let _ = writer
        .register_collection_with_indexes("first", fields.clone(), vec![], "id")
        .unwrap();

    let reader = Database::open_read_only(&path).unwrap();
    let _ = writer
        .register_collection_with_indexes("second", fields, vec![], "id")
        .unwrap();

    let names = reader.collection_names();
    assert!(names.contains(&"first".to_string()));
    assert!(names.contains(&"second".to_string()));
}

#[test]
fn rebuild_indexes_removes_stale_unique_key() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("idx.modelvault");
    let mut db = Database::open(&path).unwrap();
    let fields = vec![
        FieldDef {
            path: FieldPath(vec![Cow::Borrowed("id")]),
            ty: Type::Int64,
            constraints: vec![],
        },
        FieldDef {
            path: FieldPath(vec![Cow::Borrowed("tag")]),
            ty: Type::String,
            constraints: vec![],
        },
    ];
    let indexes = vec![IndexDef {
        name: "tag_u".into(),
        path: FieldPath(vec![Cow::Borrowed("tag")]),
        kind: IndexKind::Unique,
    }];
    let (cid, _) = db
        .register_collection_with_indexes("items", fields, indexes, "id")
        .unwrap();
    db.insert(
        cid,
        BTreeMap::from([
            ("id".to_string(), RowValue::Int64(1)),
            ("tag".to_string(), RowValue::String("old".into())),
        ]),
    )
    .unwrap();
    db.insert(
        cid,
        BTreeMap::from([
            ("id".to_string(), RowValue::Int64(1)),
            ("tag".to_string(), RowValue::String("new".into())),
        ]),
    )
    .unwrap();
    db.rebuild_indexes_for_collection(cid).unwrap();
    db.verify_index_consistency().unwrap();
    let key = modelvault_core::ScalarValue::String("old".into()).canonical_key_bytes();
    assert!(db
        .index_state()
        .unique_lookup(cid.0, "tag_u", &key)
        .is_none());
}

#[test]
fn query_and_query_iter_order_by_float_neg_zero_parity() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("float.modelvault");
    let mut db = Database::open(&path).unwrap();
    let fields = vec![
        FieldDef {
            path: FieldPath(vec![Cow::Borrowed("id")]),
            ty: Type::Int64,
            constraints: vec![],
        },
        FieldDef {
            path: FieldPath(vec![Cow::Borrowed("score")]),
            ty: Type::Float64,
            constraints: vec![],
        },
    ];
    let (cid, _) = db
        .register_collection_with_indexes("items", fields, vec![], "id")
        .unwrap();
    db.insert(
        cid,
        BTreeMap::from([
            ("id".to_string(), RowValue::Int64(1)),
            ("score".to_string(), RowValue::Float64(-0.0)),
        ]),
    )
    .unwrap();
    db.insert(
        cid,
        BTreeMap::from([
            ("id".to_string(), RowValue::Int64(2)),
            ("score".to_string(), RowValue::Float64(0.0)),
        ]),
    )
    .unwrap();

    let q = Query {
        collection: cid,
        predicate: None,
        order_by: Some(OrderBy {
            path: FieldPath(vec![Cow::Borrowed("score")]),
            direction: OrderDirection::Asc,
        }),
        limit: None,
    };
    let from_query: Vec<_> = db
        .query(&q)
        .unwrap()
        .into_iter()
        .map(|r| r.get("id").cloned().unwrap())
        .collect();
    let from_iter: Vec<_> = db
        .query_iter(&q)
        .unwrap()
        .map(|r| r.unwrap().get("id").cloned().unwrap())
        .collect();
    assert_eq!(from_query, from_iter);
}

#[test]
fn schema_field_reorder_is_breaking() {
    use modelvault_core::schema::SchemaChange;
    use modelvault_core::schema_compat::classify_schema_update;

    let old = vec![
        FieldDef {
            path: FieldPath(vec![Cow::Borrowed("id")]),
            ty: Type::Int64,
            constraints: vec![],
        },
        FieldDef {
            path: FieldPath(vec![Cow::Borrowed("a")]),
            ty: Type::String,
            constraints: vec![],
        },
        FieldDef {
            path: FieldPath(vec![Cow::Borrowed("b")]),
            ty: Type::String,
            constraints: vec![],
        },
    ];
    let new = vec![
        FieldDef {
            path: FieldPath(vec![Cow::Borrowed("id")]),
            ty: Type::Int64,
            constraints: vec![],
        },
        FieldDef {
            path: FieldPath(vec![Cow::Borrowed("b")]),
            ty: Type::String,
            constraints: vec![],
        },
        FieldDef {
            path: FieldPath(vec![Cow::Borrowed("a")]),
            ty: Type::String,
            constraints: vec![],
        },
    ];
    let change = classify_schema_update(&old, &[], &new, &[]).unwrap();
    assert!(matches!(change, SchemaChange::Breaking { .. }));
}

#[test]
fn compact_in_place_keeps_attached_ro_mirror_fresh() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("compact.modelvault");
    let mut writer = Database::open(&path).unwrap();
    let fields = vec![FieldDef {
        path: FieldPath(vec![Cow::Borrowed("id")]),
        ty: Type::Int64,
        constraints: vec![],
    }];
    let (cid, _) = writer
        .register_collection_with_indexes("items", fields, vec![], "id")
        .unwrap();
    writer
        .insert(
            cid,
            BTreeMap::from([("id".to_string(), RowValue::Int64(1))]),
        )
        .unwrap();

    let reader = Database::open_read_only(&path).unwrap();
    writer.compact_in_place().unwrap();
    writer
        .insert(
            cid,
            BTreeMap::from([("id".to_string(), RowValue::Int64(2))]),
        )
        .unwrap();

    assert!(reader
        .get(cid, &modelvault_core::ScalarValue::Int64(2))
        .unwrap()
        .is_some());
}

#[test]
fn autocommit_schema_bump_survives_reopen_with_rows() {
    use modelvault_core::schema::SchemaVersion;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("schema.modelvault");
    let mut db = Database::open(&path).unwrap();
    let fields = vec![
        FieldDef {
            path: FieldPath(vec![Cow::Borrowed("id")]),
            ty: Type::Int64,
            constraints: vec![],
        },
        FieldDef {
            path: FieldPath(vec![Cow::Borrowed("note")]),
            ty: Type::String,
            constraints: vec![],
        },
    ];
    let (cid, _) = db
        .register_collection_with_indexes("items", fields.clone(), vec![], "id")
        .unwrap();
    db.insert(
        cid,
        BTreeMap::from([
            ("id".to_string(), RowValue::Int64(1)),
            ("note".to_string(), RowValue::String("hello".into())),
        ]),
    )
    .unwrap();

    let mut bumped = fields;
    bumped.push(FieldDef {
        path: FieldPath(vec![Cow::Borrowed("extra")]),
        ty: Type::Optional(Box::new(Type::String)),
        constraints: vec![],
    });
    let v2 = db
        .register_schema_version_with_indexes(cid, bumped, vec![])
        .unwrap();
    assert_eq!(v2, SchemaVersion(2));

    drop(db);
    let reopened = Database::open(&path).unwrap();
    let row = reopened
        .get(cid, &modelvault_core::ScalarValue::Int64(1))
        .unwrap()
        .expect("row survives reopen");
    assert_eq!(
        row.get("note").cloned(),
        Some(RowValue::String("hello".into()))
    );
}
