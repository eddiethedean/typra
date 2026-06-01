use std::borrow::Cow;
use std::collections::BTreeMap;

use modelvault_core::schema::{FieldDef, FieldPath, Type};
use modelvault_core::spill::TempSpillFile;
use modelvault_core::{Database, RowValue, ScalarValue};

fn fp(name: &'static str) -> FieldPath {
    FieldPath(vec![Cow::Borrowed(name)])
}

#[test]
fn spillable_group_count_sum_i64_forced_spill_matches_in_memory() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.modelvault");
    let mut db = Database::open(&path).unwrap();

    let fields = vec![
        FieldDef {
            path: fp("id"),
            ty: Type::Int64,
            constraints: vec![],
        },
        FieldDef {
            path: fp("g"),
            ty: Type::Int64,
            constraints: vec![],
        },
        FieldDef {
            path: fp("v"),
            ty: Type::Int64,
            constraints: vec![],
        },
    ];
    let (cid, _) = db.register_collection("t", fields, "id").unwrap();

    // Lots of groups; force frequent spills with a tiny budget.
    for i in 0..5000i64 {
        let g = i % 97;
        let v = (i % 7) - 3;
        db.insert(
            cid,
            BTreeMap::from([
                ("id".to_string(), RowValue::Int64(i)),
                ("g".to_string(), RowValue::Int64(g)),
                ("v".to_string(), RowValue::Int64(v)),
            ]),
        )
        .unwrap();
    }

    // Stream rows via query_iter (scan).
    let q = modelvault_core::query::Query {
        collection: cid,
        predicate: None,
        limit: None,
        order_by: None,
    };
    let rows = db.query_iter(&q).unwrap();

    // Spill store is the same DB file, using Temp segments.
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(false)
        .open(&path)
        .unwrap();
    let mut spill = TempSpillFile::new(modelvault_core::storage::FileStore::new(file)).unwrap();

    let got = modelvault_core::query::spillable_group_count_sum_i64(
        rows,
        &fp("g"),
        &fp("v"),
        8, // tiny group budget => forced spilling
        Some(&mut spill),
    )
    .unwrap();

    // In-memory baseline.
    let rows2 = db.query_iter(&q).unwrap();
    let baseline = modelvault_core::query::spillable_group_count_sum_i64::<
        _,
        modelvault_core::storage::FileStore,
    >(rows2, &fp("g"), &fp("v"), 10_000, None)
    .unwrap();

    assert_eq!(got, baseline);

    // spot check one group
    let g = 0i64;
    let mut count = 0u64;
    let mut sum = 0i64;
    for i in 0..5000i64 {
        if i % 97 == g {
            count += 1;
            sum = sum.wrapping_add((i % 7) - 3);
        }
    }
    assert!(got
        .iter()
        .any(|(kg, c, s)| *kg == g && *c == count && *s == sum));

    // make sure we can still read a row (spill segments are ignored)
    let r = db.get(cid, &ScalarValue::Int64(1)).unwrap().unwrap();
    assert_eq!(r.get("id"), Some(&RowValue::Int64(1)));
}
