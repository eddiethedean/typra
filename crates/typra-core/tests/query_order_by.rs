use std::borrow::Cow;
use std::collections::BTreeMap;

use tempfile::tempdir;
use typra_core::query::{OrderBy, OrderDirection, Predicate, Query};
use typra_core::schema::{FieldDef, FieldPath, IndexDef, IndexKind, Type};
use typra_core::storage::Store;
use typra_core::{Database, RowValue, ScalarValue};

fn field(name: &'static str, ty: Type) -> FieldDef {
    FieldDef {
        path: FieldPath(vec![Cow::Borrowed(name)]),
        ty,
        constraints: vec![],
    }
}

fn insert_book(
    db: &mut Database<impl Store>,
    cid: typra_core::schema::CollectionId,
    title: &str,
    year: i64,
) {
    let mut row = BTreeMap::new();
    row.insert("title".to_string(), RowValue::String(title.to_string()));
    row.insert("year".to_string(), RowValue::Int64(year));
    db.insert(cid, row).unwrap();
}

#[test]
fn order_by_sorts_asc_desc_and_respects_limit_in_memory() {
    let mut db = Database::open_in_memory().unwrap();
    let fields = vec![field("title", Type::String), field("year", Type::Int64)];
    let indexes = vec![IndexDef {
        name: "year_idx".to_string(),
        path: FieldPath(vec![Cow::Borrowed("year")]),
        kind: IndexKind::NonUnique,
    }];
    let (cid, _) = db
        .register_collection_with_indexes("books", fields, indexes, "title")
        .unwrap();

    // Insert out of order, all distinct years to avoid tie semantics.
    insert_book(&mut db, cid, "b1", 2022);
    insert_book(&mut db, cid, "b2", 2019);
    insert_book(&mut db, cid, "b3", 2025);
    insert_book(&mut db, cid, "b4", 2020);
    insert_book(&mut db, cid, "b5", 2021);

    let year_path = FieldPath(vec![Cow::Borrowed("year")]);

    let q_asc = Query {
        collection: cid,
        predicate: None,
        limit: None,
        order_by: Some(OrderBy {
            path: year_path.clone(),
            direction: OrderDirection::Asc,
        }),
    };
    let rows = db.query(&q_asc).unwrap();
    let years: Vec<i64> = rows
        .iter()
        .map(|r| match r.get("year").unwrap() {
            RowValue::Int64(x) => *x,
            _ => panic!("expected int64 year"),
        })
        .collect();
    assert_eq!(years, vec![2019, 2020, 2021, 2022, 2025]);

    let q_desc_limit = Query {
        collection: cid,
        predicate: None,
        limit: Some(2),
        order_by: Some(OrderBy {
            path: year_path,
            direction: OrderDirection::Desc,
        }),
    };
    let rows = db.query(&q_desc_limit).unwrap();
    let years: Vec<i64> = rows
        .iter()
        .map(|r| match r.get("year").unwrap() {
            RowValue::Int64(x) => *x,
            _ => panic!("expected int64 year"),
        })
        .collect();
    assert_eq!(years, vec![2025, 2022]);
}

#[test]
fn order_by_is_correct_after_reopen_on_disk() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("t.typra");

    {
        let mut db = Database::open(&path).unwrap();
        let fields = vec![field("title", Type::String), field("year", Type::Int64)];
        let indexes = vec![IndexDef {
            name: "year_idx".to_string(),
            path: FieldPath(vec![Cow::Borrowed("year")]),
            kind: IndexKind::NonUnique,
        }];
        let (cid, _) = db
            .register_collection_with_indexes("books", fields, indexes, "title")
            .unwrap();

        for (t, y) in [("a", 3), ("b", 1), ("c", 4), ("d", 2)] {
            insert_book(&mut db, cid, t, y);
        }
        db.checkpoint().unwrap();
    }

    let db = Database::open(&path).unwrap();
    let cid = db.collection_id_named("books").unwrap();
    let year_path = FieldPath(vec![Cow::Borrowed("year")]);
    let q = Query {
        collection: cid,
        predicate: Some(Predicate::Gte {
            path: FieldPath(vec![Cow::Borrowed("year")]),
            value: ScalarValue::Int64(2),
        }),
        limit: None,
        order_by: Some(OrderBy {
            path: year_path,
            direction: OrderDirection::Asc,
        }),
    };
    let rows = db.query(&q).unwrap();
    let years: Vec<i64> = rows
        .iter()
        .map(|r| match r.get("year").unwrap() {
            RowValue::Int64(x) => *x,
            _ => panic!("expected int64 year"),
        })
        .collect();
    assert_eq!(years, vec![2, 3, 4]);
}

