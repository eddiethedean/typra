use std::borrow::Cow;
use std::collections::BTreeMap;

use typra_core::schema::{FieldDef, FieldPath, IndexDef, IndexKind, Type};
use typra_core::{Database, RowValue, ScalarValue};

fn path_field(name: &str, ty: Type) -> FieldDef {
    FieldDef {
        path: FieldPath(vec![Cow::Owned(name.to_string())]),
        ty,
        constraints: vec![],
    }
}

#[test]
fn compact_to_preserves_rows_and_indexes() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("src.typra");
    let compacted = dir.path().join("dst.typra");

    let mut db = Database::open(&path).unwrap();
    let fields = vec![
        path_field("id", Type::Int64),
        path_field("tag", Type::String),
    ];
    let indexes = vec![IndexDef {
        name: "tag_idx".to_string(),
        path: FieldPath(vec![Cow::Owned("tag".to_string())]),
        kind: IndexKind::NonUnique,
    }];
    let (cid, _v) = db
        .register_collection_with_indexes("t", fields, indexes, "id")
        .unwrap();

    for (id, tag) in [(1i64, "a"), (2, "b"), (3, "a")] {
        let mut row = BTreeMap::new();
        row.insert("id".to_string(), RowValue::Int64(id));
        row.insert("tag".to_string(), RowValue::String(tag.to_string()));
        db.insert(cid, row).unwrap();
    }

    // Ensure index lookup works pre-compaction.
    let q = typra_core::query::Query {
        collection: cid,
        predicate: Some(typra_core::query::Predicate::Eq {
            path: FieldPath(vec![Cow::Owned("tag".to_string())]),
            value: ScalarValue::String("a".to_string()),
        }),
        limit: None,
        order_by: None,
    };
    assert_eq!(db.query(&q).unwrap().len(), 2);

    db.compact_to(&compacted).unwrap();

    let db2 = Database::open(&compacted).unwrap();
    assert_eq!(db2.query(&q).unwrap().len(), 2);
    let got = db2.get(cid, &ScalarValue::Int64(2)).unwrap().unwrap();
    assert_eq!(got.get("tag"), Some(&RowValue::String("b".to_string())));
}

#[test]
fn compact_in_place_roundtrips() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("inplace.typra");

    let mut db = Database::open(&path).unwrap();
    let fields = vec![path_field("id", Type::Int64), path_field("x", Type::Int64)];
    db.register_collection("t", fields, "id").unwrap();
    let cid = typra_core::CollectionId(1);
    db.insert(
        cid,
        BTreeMap::from([
            ("id".to_string(), RowValue::Int64(1)),
            ("x".to_string(), RowValue::Int64(10)),
        ]),
    )
    .unwrap();

    db.compact_in_place().unwrap();
    let reopened = Database::open(&path).unwrap();
    let row = reopened.get(cid, &ScalarValue::Int64(1)).unwrap().unwrap();
    assert_eq!(row.get("x"), Some(&RowValue::Int64(10)));
}
