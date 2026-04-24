use std::borrow::Cow;
use std::collections::BTreeMap;

use tempfile::tempdir;
use typra_core::query::{OrderBy, OrderDirection, Predicate, Query};
use typra_core::schema::{CollectionId, FieldDef, FieldPath, Type};
use typra_core::{Database, RowValue, ScalarValue};

fn field(name: &'static str, ty: Type) -> FieldDef {
    FieldDef {
        path: FieldPath(vec![Cow::Borrowed(name)]),
        ty,
        constraints: vec![],
    }
}

fn insert(
    db: &mut Database,
    cid: CollectionId,
    pk_name: &str,
    pk: &str,
    shared: &str,
    value: i64,
) {
    let mut row = BTreeMap::new();
    row.insert(pk_name.to_string(), RowValue::String(pk.to_string()));
    row.insert("shared".to_string(), RowValue::String(shared.to_string()));
    row.insert("value".to_string(), RowValue::Int64(value));
    db.insert(cid, row).unwrap();
}

#[test]
fn two_collections_with_overlapping_field_names_do_not_cross_contaminate() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("t.typra");

    {
        let mut db = Database::open(&path).unwrap();
        let fields_a = vec![
            field("id", Type::String),
            field("shared", Type::String),
            field("value", Type::Int64),
        ];
        let fields_b = vec![
            field("key", Type::String),
            field("shared", Type::String),
            field("value", Type::Int64),
        ];
        let (a, _) = db.register_collection("a", fields_a, "id").unwrap();
        let (b, _) = db.register_collection("b", fields_b, "key").unwrap();

        insert(&mut db, a, "id", "a1", "x", 10);
        insert(&mut db, a, "id", "a2", "y", 20);
        insert(&mut db, b, "key", "b1", "x", 100);
        insert(&mut db, b, "key", "b2", "z", 200);
        db.checkpoint().unwrap();
    }

    let db = Database::open(&path).unwrap();
    let a = db.collection_id_named("a").unwrap();
    let b = db.collection_id_named("b").unwrap();
    assert_ne!(a.0, b.0);

    // `get` should only work with the correct PK type and correct collection id.
    assert!(db
        .get(a, &ScalarValue::String("a1".to_string()))
        .unwrap()
        .is_some());
    assert!(db
        .get(b, &ScalarValue::String("b1".to_string()))
        .unwrap()
        .is_some());

    // Query each collection; rows must not mix.
    let q_a = Query {
        collection: a,
        predicate: Some(Predicate::Eq {
            path: FieldPath(vec![Cow::Borrowed("shared")]),
            value: ScalarValue::String("x".to_string()),
        }),
        limit: None,
        order_by: Some(OrderBy {
            path: FieldPath(vec![Cow::Borrowed("value")]),
            direction: OrderDirection::Asc,
        }),
    };
    let rows_a = db.query(&q_a).unwrap();
    assert_eq!(rows_a.len(), 1);
    assert_eq!(
        rows_a[0].get("value"),
        Some(&RowValue::Int64(10)),
        "collection a should see its own row"
    );

    let q_b = Query {
        collection: b,
        predicate: Some(Predicate::Eq {
            path: FieldPath(vec![Cow::Borrowed("shared")]),
            value: ScalarValue::String("x".to_string()),
        }),
        limit: None,
        order_by: Some(OrderBy {
            path: FieldPath(vec![Cow::Borrowed("value")]),
            direction: OrderDirection::Asc,
        }),
    };
    let rows_b = db.query(&q_b).unwrap();
    assert_eq!(rows_b.len(), 1);
    assert_eq!(
        rows_b[0].get("value"),
        Some(&RowValue::Int64(100)),
        "collection b should see its own row"
    );
}

