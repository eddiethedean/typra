//! Raise coverage for indexed range plans, index range lookups, and planner execution paths.

use std::borrow::Cow;
use std::collections::BTreeMap;

use modelvault_core::file_format::MAX_QUERY_LIMIT;
use modelvault_core::index::{IndexOp, IndexState};
use modelvault_core::query::{OrderBy, OrderDirection, Predicate, Query};
use modelvault_core::schema::{FieldDef, FieldPath, IndexDef, IndexKind, Type};
use modelvault_core::{Database, DbError, RowValue, ScalarValue};

fn field(name: &str, ty: Type) -> FieldDef {
    FieldDef {
        path: FieldPath(vec![Cow::Owned(name.to_string())]),
        ty,
        constraints: vec![],
    }
}

fn path(name: &str) -> FieldPath {
    FieldPath(vec![Cow::Owned(name.to_string())])
}

#[test]
fn indexed_int64_range_plan_explain_query_and_iter() {
    let mut db = Database::open_in_memory().unwrap();
    let indexes = vec![IndexDef {
        name: "year_idx".into(),
        path: path("year"),
        kind: IndexKind::NonUnique,
    }];
    let fields = vec![field("id", Type::String), field("year", Type::Int64), field("tag", Type::String)];
    let (cid, _) = db
        .register_collection_with_indexes("events", fields, indexes, "id")
        .unwrap();

    for (id, year, tag) in [
        ("a", 1i64, "x"),
        ("b", 5, "x"),
        ("c", 7, "y"),
        ("d", 9, "x"),
        ("e", 12, "x"),
    ] {
        db.insert(
            cid,
            BTreeMap::from([
                ("id".into(), RowValue::String(id.into())),
                ("year".into(), RowValue::Int64(year)),
                ("tag".into(), RowValue::String(tag.into())),
            ]),
        )
        .unwrap();
    }

    let pred = Predicate::And(vec![
        Predicate::Gte {
            path: path("year"),
            value: ScalarValue::Int64(5),
        },
        Predicate::Lt {
            path: path("year"),
            value: ScalarValue::Int64(10),
        },
        Predicate::Eq {
            path: path("tag"),
            value: ScalarValue::String("x".into()),
        },
    ]);
    let q = Query {
        collection: cid,
        predicate: Some(pred),
        limit: Some(2),
        order_by: Some(OrderBy {
            path: path("year"),
            direction: OrderDirection::Desc,
        }),
    };

    let explain = db.explain_query(&q).unwrap();
    assert!(explain.contains("IndexRangeLookup"), "{explain}");
    assert!(explain.contains("KeyRange lo"), "{explain}");
    assert!(explain.contains("KeyRange hi"), "{explain}");
    assert!(explain.contains("ResidualFilter"), "{explain}");
    assert!(explain.contains("Limit 2"), "{explain}");
    assert!(explain.contains("OrderBy"), "{explain}");

    let vec_rows = db.query(&q).unwrap();
    assert_eq!(vec_rows.len(), 2);
    let iter_rows: Vec<_> = db.query_iter(&q).unwrap().map(|r| r.unwrap()).collect();
    assert_eq!(iter_rows.len(), 2);
}

#[test]
fn indexed_string_range_on_unique_index() {
    let mut db = Database::open_in_memory().unwrap();
    let indexes = vec![IndexDef {
        name: "sku_idx".into(),
        path: path("sku"),
        kind: IndexKind::Unique,
    }];
    let fields = vec![field("title", Type::String), field("sku", Type::String)];
    let (cid, _) = db
        .register_collection_with_indexes("items", fields, indexes, "title")
        .unwrap();

    for (title, sku) in [("a", "S01"), ("b", "S05"), ("c", "S09")] {
        db.insert(
            cid,
            BTreeMap::from([
                ("title".into(), RowValue::String(title.into())),
                ("sku".into(), RowValue::String(sku.into())),
            ]),
        )
        .unwrap();
    }

    let q = Query {
        collection: cid,
        predicate: Some(Predicate::And(vec![
            Predicate::Gt {
                path: path("sku"),
                value: ScalarValue::String("S02".into()),
            },
            Predicate::Lte {
                path: path("sku"),
                value: ScalarValue::String("S08".into()),
            },
        ])),
        limit: None,
        order_by: None,
    };
    let explain = db.explain_query(&q).unwrap();
    assert!(explain.contains("IndexRangeLookup"), "{explain}");
    assert_eq!(db.query(&q).unwrap().len(), 1);
}

#[test]
fn attached_ro_index_range_query_iter_with_order_by() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("ro_range.modelvault");
    let mut writer = Database::open(&db_path).unwrap();
    let indexes = vec![IndexDef {
        name: "score_idx".into(),
        path: path("score"),
        kind: IndexKind::NonUnique,
    }];
    let fields = vec![field("id", Type::String), field("score", Type::Int64)];
    let (cid, _) = writer
        .register_collection_with_indexes("games", fields, indexes, "id")
        .unwrap();
    for (id, score) in [("a", 10i64), ("b", 20), ("c", 30)] {
        writer
            .insert(
                cid,
                BTreeMap::from([
                    ("id".into(), RowValue::String(id.into())),
                    ("score".into(), RowValue::Int64(score)),
                ]),
            )
            .unwrap();
    }

    let reader = Database::open_read_only(&db_path).unwrap();
    let q = Query {
        collection: cid,
        predicate: Some(Predicate::Gte {
            path: path("score"),
            value: ScalarValue::Int64(15),
        }),
        limit: None,
        order_by: Some(OrderBy {
            path: path("score"),
            direction: OrderDirection::Asc,
        }),
    };
    let rows: Vec<_> = reader.query_iter(&q).unwrap().map(|r| r.unwrap()).collect();
    assert_eq!(rows.len(), 2);
}

#[test]
fn query_limit_above_max_errors() {
    let db = Database::open_in_memory().unwrap();
    let q = Query {
        collection: modelvault_core::schema::CollectionId(1),
        predicate: None,
        limit: Some(MAX_QUERY_LIMIT + 1),
        order_by: None,
    };
    let err = db.query(&q).unwrap_err();
    assert!(matches!(err, DbError::Query(_)));
    assert!(format!("{err}").contains("exceeds maximum"));
}

#[test]
fn index_state_range_lookup_edges_and_decode_hints() {
    let mut indexes = IndexState::default();
    let collection_id = 7u32;
    let index_name = "k_idx";

    assert!(indexes
        .non_unique_range_lookup(collection_id, "missing", None, true, None, true)
        .is_empty());
    assert!(indexes
        .unique_range_lookup(collection_id, "missing", None, true, None, true)
        .is_empty());

    indexes
        .apply(modelvault_core::index::IndexEntry {
            collection_id,
            index_name: index_name.to_string(),
            kind: IndexKind::Unique,
            op: IndexOp::Insert,
            index_key: ScalarValue::Int64(5).canonical_key_bytes(),
            pk_key: b"pk5".to_vec(),
        })
        .unwrap();
    let unique = indexes.unique_range_lookup(
        collection_id,
        index_name,
        Some(&ScalarValue::Int64(4)),
        true,
        Some(&ScalarValue::Int64(6)),
        false,
    );
    assert_eq!(unique.len(), 1);
    assert_eq!(unique[0], b"pk5");

    indexes
        .apply(modelvault_core::index::IndexEntry {
            collection_id,
            index_name: "ts_idx".to_string(),
            kind: IndexKind::Unique,
            op: IndexOp::Insert,
            index_key: ScalarValue::Timestamp(9).canonical_key_bytes(),
            pk_key: b"pk9".to_vec(),
        })
        .unwrap();
    let ts = indexes.unique_range_lookup(
        collection_id,
        "ts_idx",
        Some(&ScalarValue::Timestamp(8)),
        true,
        Some(&ScalarValue::Timestamp(10)),
        true,
    );
    assert_eq!(ts.len(), 1);

    indexes
        .apply(modelvault_core::index::IndexEntry {
            collection_id,
            index_name: "f_idx".to_string(),
            kind: IndexKind::NonUnique,
            op: IndexOp::Insert,
            index_key: ScalarValue::Float64(1.25).canonical_key_bytes(),
            pk_key: b"pkf".to_vec(),
        })
        .unwrap();
    let floats = indexes.non_unique_range_lookup(
        collection_id,
        "f_idx",
        Some(&ScalarValue::Float64(1.0)),
        true,
        Some(&ScalarValue::Float64(2.0)),
        true,
    );
    assert_eq!(floats.len(), 1);

    indexes
        .apply(modelvault_core::index::IndexEntry {
            collection_id,
            index_name: "s_idx".to_string(),
            kind: IndexKind::NonUnique,
            op: IndexOp::Insert,
            index_key: b"beta".to_vec(),
            pk_key: b"pks".to_vec(),
        })
        .unwrap();
    let strings = indexes.non_unique_range_lookup(
        collection_id,
        "s_idx",
        Some(&ScalarValue::String("alpha".into())),
        false,
        Some(&ScalarValue::String("gamma".into())),
        true,
    );
    assert_eq!(strings.len(), 1);
}

#[test]
fn range_query_iter_without_order_by_uses_index_range_source() {
    let mut db = Database::open_in_memory().unwrap();
    let indexes = vec![IndexDef {
        name: "year_idx".into(),
        path: path("year"),
        kind: IndexKind::NonUnique,
    }];
    let fields = vec![field("id", Type::String), field("year", Type::Int64)];
    let (cid, _) = db
        .register_collection_with_indexes("events", fields, indexes, "id")
        .unwrap();
    for (id, year) in [("a", 1i64), ("b", 5), ("c", 9)] {
        db.insert(
            cid,
            BTreeMap::from([
                ("id".into(), RowValue::String(id.into())),
                ("year".into(), RowValue::Int64(year)),
            ]),
        )
        .unwrap();
    }

    let q = Query {
        collection: cid,
        predicate: Some(Predicate::And(vec![
            Predicate::Gte {
                path: path("year"),
                value: ScalarValue::Int64(4),
            },
            Predicate::Lt {
                path: path("year"),
                value: ScalarValue::Int64(10),
            },
        ])),
        limit: None,
        order_by: None,
    };
    let rows: Vec<_> = db.query_iter(&q).unwrap().map(|r| r.unwrap()).collect();
    assert_eq!(rows.len(), 2);
}

#[test]
fn attached_ro_index_lookup_and_range_iter_without_order_by() {
    let dir = tempfile::tempdir().unwrap();
    let db_path = dir.path().join("ro_owned.modelvault");
    let mut writer = Database::open(&db_path).unwrap();
    let indexes = vec![
        IndexDef {
            name: "sku_idx".into(),
            path: path("sku"),
            kind: IndexKind::Unique,
        },
        IndexDef {
            name: "cat_idx".into(),
            path: path("category"),
            kind: IndexKind::NonUnique,
        },
    ];
    let fields = vec![
        field("title", Type::String),
        field("sku", Type::String),
        field("category", Type::String),
        field("price", Type::Int64),
    ];
    let (cid, _) = writer
        .register_collection_with_indexes("items", fields, indexes, "title")
        .unwrap();
    for (title, sku, cat, price) in [
        ("a", "S01", "tools", 10i64),
        ("b", "S02", "tools", 20),
        ("c", "S03", "media", 30),
    ] {
        writer
            .insert(
                cid,
                BTreeMap::from([
                    ("title".into(), RowValue::String(title.into())),
                    ("sku".into(), RowValue::String(sku.into())),
                    ("category".into(), RowValue::String(cat.into())),
                    ("price".into(), RowValue::Int64(price)),
                ]),
            )
            .unwrap();
    }

    let reader = Database::open_read_only(&db_path).unwrap();

    let eq_unique = Query {
        collection: cid,
        predicate: Some(Predicate::Eq {
            path: path("sku"),
            value: ScalarValue::String("S02".into()),
        }),
        limit: None,
        order_by: None,
    };
    assert_eq!(reader.query_iter(&eq_unique).unwrap().count(), 1);

    let eq_non_unique = Query {
        collection: cid,
        predicate: Some(Predicate::Eq {
            path: path("category"),
            value: ScalarValue::String("tools".into()),
        }),
        limit: None,
        order_by: None,
    };
    assert_eq!(reader.query_iter(&eq_non_unique).unwrap().count(), 2);

    let range = Query {
        collection: cid,
        predicate: Some(Predicate::And(vec![
            Predicate::Gte {
                path: path("price"),
                value: ScalarValue::Int64(15),
            },
            Predicate::Lt {
                path: path("price"),
                value: ScalarValue::Int64(35),
            },
        ])),
        limit: None,
        order_by: None,
    };
    // price is not indexed: attached RO collection scan path still streams via OwnedScanSource.
    assert_eq!(reader.query_iter(&range).unwrap().count(), 2);

    let cat_eq = Query {
        collection: cid,
        predicate: Some(Predicate::Eq {
            path: path("category"),
            value: ScalarValue::String("media".into()),
        }),
        limit: None,
        order_by: None,
    };
    assert_eq!(reader.query_iter(&cat_eq).unwrap().count(), 1);
}
