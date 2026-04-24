//! Exercises `query::planner` branches and `index` codec / `IndexState` edges for practical coverage.

use std::borrow::Cow;
use std::collections::BTreeMap;

use typra_core::db::Database;
use typra_core::error::{DbError, SchemaError};
use typra_core::index::{
    decode_index_payload, encode_index_payload, IndexEntry, IndexState, INDEX_PAYLOAD_VERSION_V1,
};
use typra_core::query::{Predicate, Query};
use typra_core::schema::{CollectionId, FieldDef, FieldPath, IndexDef, IndexKind, Type};
use typra_core::{RowValue, ScalarValue};

fn path_field(name: &str, ty: Type) -> FieldDef {
    FieldDef {
        path: FieldPath(vec![Cow::Owned(name.to_string())]),
        ty,
        constraints: vec![],
    }
}

#[test]
fn query_unknown_collection_errors() {
    let db = Database::open_in_memory().unwrap();
    let q = Query {
        collection: CollectionId(99),
        predicate: None,
        limit: None,
        order_by: None,
    };
    assert!(matches!(
        db.explain_query(&q),
        Err(DbError::Schema(SchemaError::UnknownCollection { id: 99 }))
    ));
    assert!(matches!(
        db.query(&q),
        Err(DbError::Schema(SchemaError::UnknownCollection { id: 99 }))
    ));
    assert!(matches!(
        db.query_iter(&q),
        Err(DbError::Schema(SchemaError::UnknownCollection { id: 99 }))
    ));
}

#[test]
fn query_full_collection_scan_no_predicate() {
    let mut db = Database::open_in_memory().unwrap();
    let fields = vec![
        path_field("title", Type::String),
        path_field("year", Type::Int64),
    ];
    let (cid, _) = db.register_collection("books", fields, "title").unwrap();
    db.insert(
        cid,
        BTreeMap::from([
            ("title".into(), RowValue::String("A".into())),
            ("year".into(), RowValue::Int64(1)),
        ]),
    )
    .unwrap();
    db.insert(
        cid,
        BTreeMap::from([
            ("title".into(), RowValue::String("B".into())),
            ("year".into(), RowValue::Int64(2)),
        ]),
    )
    .unwrap();

    let q = Query {
        collection: cid,
        predicate: None,
        limit: None,
        order_by: None,
    };
    let explain = db.explain_query(&q).unwrap();
    assert!(explain.contains("CollectionScan"));
    assert!(!explain.contains("Filter"));
    let mut rows = db.query(&q).unwrap();
    rows.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
    assert_eq!(rows.len(), 2);

    let mut iter_rows: Vec<_> = db.query_iter(&q).unwrap().map(|r| r.unwrap()).collect();
    iter_rows.sort_by(|a, b| format!("{a:?}").cmp(&format!("{b:?}")));
    assert_eq!(iter_rows, rows);
}

#[test]
fn query_collection_scan_with_limit_and_non_indexed_predicate() {
    let mut db = Database::open_in_memory().unwrap();
    let fields = vec![
        path_field("title", Type::String),
        path_field("year", Type::Int64),
    ];
    let indexes = vec![IndexDef {
        name: "title_idx".into(),
        path: FieldPath(vec![Cow::Owned("title".into())]),
        kind: IndexKind::NonUnique,
    }];
    let (cid, _) = db
        .register_collection_with_indexes("books", fields, indexes, "title")
        .unwrap();
    for (t, y) in [("A", 1i64), ("B", 2), ("C", 3)] {
        db.insert(
            cid,
            BTreeMap::from([
                ("title".into(), RowValue::String(t.into())),
                ("year".into(), RowValue::Int64(y)),
            ]),
        )
        .unwrap();
    }

    let q = Query {
        collection: cid,
        predicate: Some(Predicate::Eq {
            path: FieldPath(vec![Cow::Owned("year".into())]),
            value: ScalarValue::Int64(2),
        }),
        limit: Some(1),
        order_by: None,
    };
    let explain = db.explain_query(&q).unwrap();
    assert!(explain.contains("CollectionScan"));
    assert!(explain.contains("Filter"));
    assert!(explain.contains("Limit 1"));
    assert_eq!(db.query(&q).unwrap().len(), 1);
    assert_eq!(db.query_iter(&q).unwrap().count(), 1);
}

#[test]
fn query_and_prefers_unique_index_over_non_unique() {
    let mut db = Database::open_in_memory().unwrap();
    let fields = vec![
        path_field("title", Type::String),
        path_field("status", Type::String),
        path_field("sku", Type::String),
    ];
    let indexes = vec![
        IndexDef {
            name: "status_idx".into(),
            path: FieldPath(vec![Cow::Owned("status".into())]),
            kind: IndexKind::NonUnique,
        },
        IndexDef {
            name: "sku_idx".into(),
            path: FieldPath(vec![Cow::Owned("sku".into())]),
            kind: IndexKind::Unique,
        },
    ];
    let (cid, _) = db
        .register_collection_with_indexes("items", fields, indexes, "title")
        .unwrap();
    db.insert(
        cid,
        BTreeMap::from([
            ("title".into(), RowValue::String("t1".into())),
            ("status".into(), RowValue::String("open".into())),
            ("sku".into(), RowValue::String("S1".into())),
        ]),
    )
    .unwrap();

    let pred = Predicate::And(vec![
        Predicate::Eq {
            path: FieldPath(vec![Cow::Owned("status".into())]),
            value: ScalarValue::String("open".into()),
        },
        Predicate::Eq {
            path: FieldPath(vec![Cow::Owned("sku".into())]),
            value: ScalarValue::String("S1".into()),
        },
    ]);
    let q = Query {
        collection: cid,
        predicate: Some(pred),
        limit: None,
        order_by: None,
    };
    let explain = db.explain_query(&q).unwrap();
    assert!(explain.contains("sku_idx"));
    assert!(
        explain.contains("kind=Unique"),
        "expected unique index in plan, got:\n{explain}"
    );
    assert!(explain.contains("ResidualFilter"));
    let rows = db.query(&q).unwrap();
    assert_eq!(rows.len(), 1);
}

#[test]
fn query_indexed_non_unique_respects_limit_and_iter_matches() {
    let mut db = Database::open_in_memory().unwrap();
    let fields = vec![
        path_field("title", Type::String),
        path_field("tag", Type::String),
    ];
    let indexes = vec![IndexDef {
        name: "tag_idx".into(),
        path: FieldPath(vec![Cow::Owned("tag".into())]),
        kind: IndexKind::NonUnique,
    }];
    let (cid, _) = db
        .register_collection_with_indexes("books", fields, indexes, "title")
        .unwrap();
    for t in ["a", "b", "c"] {
        db.insert(
            cid,
            BTreeMap::from([
                ("title".into(), RowValue::String(t.into())),
                ("tag".into(), RowValue::String("x".into())),
            ]),
        )
        .unwrap();
    }

    let q = Query {
        collection: cid,
        predicate: Some(Predicate::Eq {
            path: FieldPath(vec![Cow::Owned("tag".into())]),
            value: ScalarValue::String("x".into()),
        }),
        limit: Some(2),
        order_by: None,
    };
    let vec_rows = db.query(&q).unwrap();
    assert_eq!(vec_rows.len(), 2);
    let iter_rows: Vec<_> = db.query_iter(&q).unwrap().map(|r| r.unwrap()).collect();
    assert_eq!(iter_rows.len(), 2);
}

#[test]
fn query_unique_index_miss_returns_empty() {
    let mut db = Database::open_in_memory().unwrap();
    let fields = vec![
        path_field("title", Type::String),
        path_field("sku", Type::String),
    ];
    let indexes = vec![IndexDef {
        name: "sku_idx".into(),
        path: FieldPath(vec![Cow::Owned("sku".into())]),
        kind: IndexKind::Unique,
    }];
    let (cid, _) = db
        .register_collection_with_indexes("items", fields, indexes, "title")
        .unwrap();
    db.insert(
        cid,
        BTreeMap::from([
            ("title".into(), RowValue::String("t1".into())),
            ("sku".into(), RowValue::String("S1".into())),
        ]),
    )
    .unwrap();

    let q = Query {
        collection: cid,
        predicate: Some(Predicate::Eq {
            path: FieldPath(vec![Cow::Owned("sku".into())]),
            value: ScalarValue::String("missing".into()),
        }),
        limit: None,
        order_by: None,
    };
    assert!(db.query(&q).unwrap().is_empty());
    assert_eq!(db.query_iter(&q).unwrap().count(), 0);
}

#[test]
fn query_residual_and_with_two_conjuncts_after_index_pick() {
    let mut db = Database::open_in_memory().unwrap();
    let fields = vec![
        path_field("title", Type::String),
        path_field("year", Type::Int64),
        path_field("qty", Type::Int64),
    ];
    let indexes = vec![IndexDef {
        name: "title_idx".into(),
        path: FieldPath(vec![Cow::Owned("title".into())]),
        kind: IndexKind::NonUnique,
    }];
    let (cid, _) = db
        .register_collection_with_indexes("books", fields, indexes, "title")
        .unwrap();
    db.insert(
        cid,
        BTreeMap::from([
            ("title".into(), RowValue::String("t1".into())),
            ("year".into(), RowValue::Int64(10)),
            ("qty".into(), RowValue::Int64(100)),
        ]),
    )
    .unwrap();

    let pred = Predicate::And(vec![
        Predicate::Eq {
            path: FieldPath(vec![Cow::Owned("title".into())]),
            value: ScalarValue::String("t1".into()),
        },
        Predicate::Eq {
            path: FieldPath(vec![Cow::Owned("year".into())]),
            value: ScalarValue::Int64(10),
        },
        Predicate::Eq {
            path: FieldPath(vec![Cow::Owned("qty".into())]),
            value: ScalarValue::Int64(100),
        },
    ]);
    let q = Query {
        collection: cid,
        predicate: Some(pred),
        limit: None,
        order_by: None,
    };
    let explain = db.explain_query(&q).unwrap();
    assert!(explain.contains("title_idx"));
    assert!(explain.contains("ResidualFilter"));
    assert_eq!(db.query(&q).unwrap().len(), 1);
}

#[test]
fn query_iter_unique_index_residual_filters_row() {
    let mut db = Database::open_in_memory().unwrap();
    let fields = vec![
        path_field("title", Type::String),
        path_field("sku", Type::String),
        path_field("qty", Type::Int64),
    ];
    let indexes = vec![IndexDef {
        name: "sku_idx".into(),
        path: FieldPath(vec![Cow::Owned("sku".into())]),
        kind: IndexKind::Unique,
    }];
    let (cid, _) = db
        .register_collection_with_indexes("items", fields, indexes, "title")
        .unwrap();
    db.insert(
        cid,
        BTreeMap::from([
            ("title".into(), RowValue::String("t1".into())),
            ("sku".into(), RowValue::String("S1".into())),
            ("qty".into(), RowValue::Int64(5)),
        ]),
    )
    .unwrap();

    let pred = Predicate::And(vec![
        Predicate::Eq {
            path: FieldPath(vec![Cow::Owned("sku".into())]),
            value: ScalarValue::String("S1".into()),
        },
        Predicate::Eq {
            path: FieldPath(vec![Cow::Owned("qty".into())]),
            value: ScalarValue::Int64(99),
        },
    ]);
    let q = Query {
        collection: cid,
        predicate: Some(pred),
        limit: None,
        order_by: None,
    };
    assert!(db.query(&q).unwrap().is_empty());
    assert_eq!(db.query_iter(&q).unwrap().count(), 0);
}

#[test]
fn index_state_unique_violation_and_idempotent_reapply() {
    let mut st = IndexState::default();
    let e = IndexEntry {
        collection_id: 1,
        index_name: "u".into(),
        kind: IndexKind::Unique,
        op: typra_core::index::IndexOp::Insert,
        index_key: b"k".to_vec(),
        pk_key: b"p1".to_vec(),
    };
    st.apply(e.clone()).unwrap();
    st.apply(e.clone()).unwrap();
    let clash = IndexEntry {
        collection_id: 1,
        index_name: "u".into(),
        kind: IndexKind::Unique,
        op: typra_core::index::IndexOp::Insert,
        index_key: b"k".to_vec(),
        pk_key: b"p2".to_vec(),
    };
    assert!(matches!(
        st.apply(clash),
        Err(DbError::Schema(SchemaError::UniqueIndexViolation))
    ));
}

#[test]
fn decode_index_payload_rejects_bad_version_kind_trailing() {
    let mut bad_ver = encode_index_payload(&[]);
    bad_ver[0] = 0xff;
    bad_ver[1] = 0xff;
    assert!(decode_index_payload(&bad_ver).is_err());

    let entry = IndexEntry {
        collection_id: 1,
        index_name: "n".into(),
        kind: IndexKind::Unique,
        op: typra_core::index::IndexOp::Insert,
        index_key: vec![1],
        pk_key: vec![2],
    };
    let mut buf = encode_index_payload(std::slice::from_ref(&entry));
    // ver(2) + n(4) + cid(4) + kind(1) + op(1) + ...
    let kind_pos = 2 + 4 + 4;
    buf[kind_pos] = 99;
    assert!(decode_index_payload(&buf).is_err());

    let mut trail = encode_index_payload(&[entry]);
    trail.push(0);
    assert!(decode_index_payload(&trail).is_err());
}

#[test]
fn decode_index_payload_rejects_invalid_utf8_name() {
    let mut buf = Vec::new();
    buf.extend_from_slice(&INDEX_PAYLOAD_VERSION_V1.to_le_bytes());
    buf.extend_from_slice(&1u32.to_le_bytes());
    buf.extend_from_slice(&1u32.to_le_bytes());
    buf.push(1u8);
    buf.extend_from_slice(&1u32.to_le_bytes());
    buf.push(0xff); // invalid UTF-8 as sole code unit
    buf.extend_from_slice(&0u32.to_le_bytes());
    buf.extend_from_slice(&0u32.to_le_bytes());
    let e = decode_index_payload(&buf).unwrap_err();
    assert!(matches!(e, DbError::Format(_)));
}

#[test]
fn decode_index_payload_rejects_empty_name() {
    // version + count=1 + collection_id + kind + empty string len 0 (invalid)
    let mut buf = Vec::new();
    buf.extend_from_slice(&INDEX_PAYLOAD_VERSION_V1.to_le_bytes());
    buf.extend_from_slice(&1u32.to_le_bytes());
    buf.extend_from_slice(&1u32.to_le_bytes());
    buf.push(1u8); // unique
    buf.extend_from_slice(&0u32.to_le_bytes()); // empty name
    buf.extend_from_slice(&0u32.to_le_bytes());
    buf.extend_from_slice(&0u32.to_le_bytes());
    assert!(decode_index_payload(&buf).is_err());
}
