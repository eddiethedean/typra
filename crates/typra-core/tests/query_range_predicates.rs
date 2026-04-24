use std::borrow::Cow;
use std::collections::BTreeMap;

use tempfile::tempdir;
use typra_core::query::{OrderBy, OrderDirection, Predicate, Query};
use typra_core::schema::{FieldDef, FieldPath, Type};
use typra_core::storage::Store;
use typra_core::{Database, RowValue, ScalarValue};

fn field(name: &'static str, ty: Type) -> FieldDef {
    FieldDef {
        path: FieldPath(vec![Cow::Borrowed(name)]),
        ty,
        constraints: vec![],
    }
}

fn insert_row(
    db: &mut Database<impl Store>,
    cid: typra_core::schema::CollectionId,
    id: &str,
    year: i64,
) {
    let mut row = BTreeMap::new();
    row.insert("id".to_string(), RowValue::String(id.to_string()));
    row.insert("year".to_string(), RowValue::Int64(year));
    db.insert(cid, row).unwrap();
}

#[test]
fn range_predicates_filter_correctly_and_explain_is_collection_scan() {
    let mut db = Database::open_in_memory().unwrap();
    let fields = vec![field("id", Type::String), field("year", Type::Int64)];
    let (cid, _) = db.register_collection("events", fields, "id").unwrap();

    for y in 0..20i64 {
        insert_row(&mut db, cid, &format!("e{y}"), y);
    }

    let year_path = FieldPath(vec![Cow::Borrowed("year")]);
    let q = Query {
        collection: cid,
        predicate: Some(Predicate::And(vec![
            Predicate::Gte {
                path: year_path.clone(),
                value: ScalarValue::Int64(5),
            },
            Predicate::Lt {
                path: year_path.clone(),
                value: ScalarValue::Int64(12),
            },
        ])),
        limit: None,
        order_by: Some(OrderBy {
            path: year_path.clone(),
            direction: OrderDirection::Asc,
        }),
    };

    let explain = db.explain_query(&q).unwrap();
    assert!(explain.contains("CollectionScan"), "{explain}");
    assert!(explain.contains("Filter"), "{explain}");

    let rows = db.query(&q).unwrap();
    let years: Vec<i64> = rows
        .iter()
        .map(|r| match r.get("year").unwrap() {
            RowValue::Int64(x) => *x,
            _ => panic!("expected int64 year"),
        })
        .collect();
    assert_eq!(years, vec![5, 6, 7, 8, 9, 10, 11]);
}

#[test]
fn range_predicates_survive_reopen_and_match_baseline_filtering() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("t.typra");

    {
        let mut db = Database::open(&path).unwrap();
        let fields = vec![field("id", Type::String), field("year", Type::Int64)];
        let (cid, _) = db.register_collection("events", fields, "id").unwrap();
        for y in [-3i64, -2, -1, 0, 1, 2, 3, 10, 11, 12, 13] {
            insert_row(&mut db, cid, &format!("e{y}"), y);
        }
        db.checkpoint().unwrap();
    }

    let db = Database::open(&path).unwrap();
    let cid = db.collection_id_named("events").unwrap();

    let year_path = FieldPath(vec![Cow::Borrowed("year")]);
    let q = Query {
        collection: cid,
        predicate: Some(Predicate::Or(vec![
            Predicate::Lt {
                path: year_path.clone(),
                value: ScalarValue::Int64(0),
            },
            Predicate::Gte {
                path: year_path.clone(),
                value: ScalarValue::Int64(12),
            },
        ])),
        limit: None,
        order_by: Some(OrderBy {
            path: year_path,
            direction: OrderDirection::Asc,
        }),
    };

    // Baseline in test: filter from a full scan.
    let mut baseline = Vec::new();
    for y in [-3i64, -2, -1, 0, 1, 2, 3, 10, 11, 12, 13] {
        if y < 0 || y >= 12 {
            baseline.push(y);
        }
    }
    baseline.sort();

    let rows = db.query(&q).unwrap();
    let years: Vec<i64> = rows
        .iter()
        .map(|r| match r.get("year").unwrap() {
            RowValue::Int64(x) => *x,
            _ => panic!("expected int64 year"),
        })
        .collect();
    assert_eq!(years, baseline);
}

