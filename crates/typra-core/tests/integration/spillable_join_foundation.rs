use std::borrow::Cow;
use std::collections::BTreeMap;

use typra_core::schema::{FieldDef, FieldPath, Type};
use typra_core::spill::TempSpillFile;
use typra_core::{Database, RowValue};

fn fp(name: &'static str) -> FieldPath {
    FieldPath(vec![Cow::Borrowed(name)])
}

#[test]
fn spillable_hash_join_match_count_i64_forced_spill_matches_baseline() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.typra");
    let mut db = Database::open(&path).unwrap();

    let fields = vec![
        FieldDef {
            path: fp("id"),
            ty: Type::Int64,
            constraints: vec![],
        },
        FieldDef {
            path: fp("k"),
            ty: Type::Int64,
            constraints: vec![],
        },
    ];
    let (cid_l, _) = db.register_collection("l", fields.clone(), "id").unwrap();
    let (cid_r, _) = db.register_collection("r", fields, "id").unwrap();

    // Left: 2000 rows across 31 keys.
    for i in 0..2000i64 {
        db.insert(
            cid_l,
            BTreeMap::from([
                ("id".to_string(), RowValue::Int64(i)),
                ("k".to_string(), RowValue::Int64(i % 31)),
            ]),
        )
        .unwrap();
    }
    // Right: 1500 rows across 31 keys.
    for i in 0..1500i64 {
        db.insert(
            cid_r,
            BTreeMap::from([
                ("id".to_string(), RowValue::Int64(i)),
                ("k".to_string(), RowValue::Int64((i * 3) % 31)),
            ]),
        )
        .unwrap();
    }

    let ql = typra_core::query::Query {
        collection: cid_l,
        predicate: None,
        limit: None,
        order_by: None,
    };
    let qr = typra_core::query::Query {
        collection: cid_r,
        predicate: None,
        limit: None,
        order_by: None,
    };

    let left_rows = db.query_iter(&ql).unwrap();
    let right_rows = db.query_iter(&qr).unwrap();

    // Spill store is the same DB file, using Temp segments.
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(false)
        .open(&path)
        .unwrap();
    let mut spill = TempSpillFile::new(typra_core::storage::FileStore::new(file)).unwrap();

    let got = typra_core::query::spillable_hash_join_match_count_i64(
        left_rows,
        right_rows,
        &fp("k"),
        &fp("k"),
        4, // tiny budget -> spill
        Some(&mut spill),
    )
    .unwrap();

    // Baseline (no spill): same function with huge budget, but must re-run iterators.
    let left2 = db.query_iter(&ql).unwrap();
    let right2 = db.query_iter(&qr).unwrap();
    let baseline = typra_core::query::spillable_hash_join_match_count_i64::<
        _,
        _,
        typra_core::storage::FileStore,
    >(left2, right2, &fp("k"), &fp("k"), 10_000, None)
    .unwrap();

    assert_eq!(got, baseline);
    assert!(got > 0);
}
