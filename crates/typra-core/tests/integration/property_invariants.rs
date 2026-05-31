use std::borrow::Cow;
use std::collections::BTreeMap;

use proptest::prelude::*;

use typra_core::query::{Predicate, Query};
use typra_core::schema::{FieldDef, FieldPath, IndexDef, IndexKind, Type};
use typra_core::{Database, RowValue, ScalarValue};

fn fp1(name: &'static str) -> FieldPath {
    FieldPath(vec![Cow::Borrowed(name)])
}

fn book_fields() -> Vec<FieldDef> {
    vec![
        FieldDef {
            path: fp1("id"),
            ty: Type::Int64,
            constraints: vec![],
        },
        FieldDef {
            path: fp1("x"),
            ty: Type::Int64,
            constraints: vec![],
        },
    ]
}

fn row(id: i64, x: i64) -> BTreeMap<String, RowValue> {
    BTreeMap::from([
        ("id".to_string(), RowValue::Int64(id)),
        ("x".to_string(), RowValue::Int64(x)),
    ])
}

fn indexed_fields() -> Vec<FieldDef> {
    vec![
        FieldDef {
            path: fp1("id"),
            ty: Type::Int64,
            constraints: vec![],
        },
        FieldDef {
            path: fp1("tag"),
            ty: Type::Int64,
            constraints: vec![],
        },
    ]
}

proptest! {
    // Basic safety invariant: snapshot roundtrip preserves visible state.
    #[test]
    fn snapshot_roundtrip_preserves_rows(ops in proptest::collection::vec((any::<i64>(), any::<i64>()), 0..200)) {
        let mut db = Database::open_in_memory().unwrap();
        let (cid, _) = db.register_collection("books", book_fields(), "id").unwrap();
        for (id, x) in &ops {
            db.insert(cid, row(*id, *x)).unwrap();
        }

        let snap = db.snapshot_bytes();
        let db2 = Database::from_snapshot_bytes(snap).unwrap();

        for (id, x) in &ops {
            let got = db2.get(cid, &ScalarValue::Int64(*id)).unwrap();
            prop_assert_eq!(got, Some(row(*id, *x)));
        }
    }

    // Replay idempotence: snapshot → reopen → row-equivalent state.
    #[test]
    fn snapshot_reopen_preserves_row_count(ops in proptest::collection::vec((any::<i64>(), any::<i64>()), 0..100)) {
        let mut db = Database::open_in_memory().unwrap();
        let (cid, _) = db.register_collection("books", book_fields(), "id").unwrap();
        for (id, x) in &ops {
            db.insert(cid, row(*id, *x)).unwrap();
        }
        let snap = db.snapshot_bytes();
        let db2 = Database::from_snapshot_bytes(snap.clone()).unwrap();
        let db3 = Database::from_snapshot_bytes(snap).unwrap();
        prop_assert_eq!(db2.collection_names(), db3.collection_names());
        for (id, x) in &ops {
            let a = db2.get(cid, &ScalarValue::Int64(*id)).unwrap();
            let b = db3.get(cid, &ScalarValue::Int64(*id)).unwrap();
            prop_assert_eq!(a, Some(row(*id, *x)));
            prop_assert_eq!(b, Some(row(*id, *x)));
        }
    }

    // Indexed equality returns the same rows as manual filtering over inserted data.
    #[test]
    fn indexed_equality_matches_manual_filter(
        ops in proptest::collection::vec((any::<i64>(), any::<i64>()), 0..80),
        query_tag in any::<i64>(),
    ) {
        let mut db = Database::open_in_memory().unwrap();
        let indexes = vec![IndexDef {
            name: "tag_idx".to_string(),
            path: fp1("tag"),
            kind: IndexKind::NonUnique,
        }];
        let (cid, _) = db
            .register_collection_with_indexes("t", indexed_fields(), indexes, "id")
            .unwrap();
        for (id, tag) in &ops {
            let mut r = BTreeMap::new();
            r.insert("id".into(), RowValue::Int64(*id));
            r.insert("tag".into(), RowValue::Int64(*tag));
            db.insert(cid, r).unwrap();
        }

        let q = Query {
            collection: cid,
            predicate: Some(Predicate::Eq {
                path: fp1("tag"),
                value: ScalarValue::Int64(query_tag),
            }),
            limit: None,
            order_by: None,
        };
        let got = db.query(&q).unwrap();
        let explain = db.explain_query(&q).unwrap();
        prop_assert!(explain.contains("IndexLookup"));

        let expected_count = ops.iter().filter(|(_, tag)| *tag == query_tag).count();
        prop_assert_eq!(got.len(), expected_count);
    }

    // Unique index: duplicate tag values always rejected on insert.
    #[test]
    fn unique_index_rejects_duplicate_tags(
        id1 in any::<i64>(),
        id2 in any::<i64>(),
        tag in any::<i64>(),
    ) {
        prop_assume!(id1 != id2);
        let mut db = Database::open_in_memory().unwrap();
        let indexes = vec![IndexDef {
            name: "tag_u".to_string(),
            path: fp1("tag"),
            kind: IndexKind::Unique,
        }];
        let (cid, _) = db
            .register_collection_with_indexes("t", indexed_fields(), indexes, "id")
            .unwrap();
        let mut r1 = BTreeMap::new();
        r1.insert("id".into(), RowValue::Int64(id1));
        r1.insert("tag".into(), RowValue::Int64(tag));
        db.insert(cid, r1).unwrap();
        let mut r2 = BTreeMap::new();
        r2.insert("id".into(), RowValue::Int64(id2));
        r2.insert("tag".into(), RowValue::Int64(tag));
        prop_assert!(db.insert(cid, r2).is_err());
    }

    // Basic hardening invariant: SQL parsing should never panic on arbitrary input.
    #[test]
    fn sql_parse_select_never_panics(s in ".*") {
        let _ = typra_core::sql::parse_select(&s);
    }
}
