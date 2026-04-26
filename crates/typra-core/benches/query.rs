//! Compare primary-key `get`, indexed equality query, and full collection scan.
//!
//! Run: `cargo bench -p typra-core --bench query`
//!
//! Expect indexed lookup to outperform a full scan at larger `N` (subject to hash-map iteration
//! order and measurement noise).

use std::collections::BTreeMap;

use criterion::{criterion_group, criterion_main, Criterion};
use std::borrow::Cow;
use typra_core::query::{Predicate, Query};
use typra_core::record::RowValue;
use typra_core::schema::{FieldDef, FieldPath, IndexDef, IndexKind, Type};
use typra_core::{Database, ScalarValue};

fn path_field(name: &str, ty: Type) -> FieldDef {
    FieldDef {
        path: FieldPath(vec![Cow::Owned(name.to_string())]),
        ty,
        constraints: vec![],
    }
}

fn bench_query_paths(c: &mut Criterion) {
    const N: usize = 10_000;
    let mut db = Database::open_in_memory().unwrap();
    let fields = vec![
        path_field("id", Type::Int64),
        path_field("tag", Type::String),
    ];
    let indexes = vec![IndexDef {
        name: "tag_idx".to_string(),
        path: FieldPath(vec![Cow::Owned("tag".to_string())]),
        kind: IndexKind::NonUnique,
    }];
    let (cid, _) = db
        .register_collection_with_indexes("t", fields, indexes, "id")
        .unwrap();

    for i in 0..N {
        let mut m = BTreeMap::new();
        m.insert("id".to_string(), RowValue::Int64(i as i64));
        let tag = if i == N / 2 { "needle" } else { "other" };
        m.insert("tag".to_string(), RowValue::String(tag.to_string()));
        db.insert(cid, m).unwrap();
    }

    let pk = ScalarValue::Int64((N / 2) as i64);
    c.bench_function("get_pk", |b| {
        b.iter(|| std::hint::black_box(db.get(cid, std::hint::black_box(&pk)).unwrap()))
    });

    let q_indexed = Query {
        collection: cid,
        predicate: Some(Predicate::Eq {
            path: FieldPath(vec![Cow::Owned("tag".to_string())]),
            value: ScalarValue::String("needle".to_string()),
        }),
        limit: None,
        order_by: None,
    };
    c.bench_function("query_indexed_eq", |b| {
        b.iter(|| std::hint::black_box(db.query(std::hint::black_box(&q_indexed)).unwrap()))
    });

    let q_scan = Query {
        collection: cid,
        predicate: Some(Predicate::Eq {
            path: FieldPath(vec![Cow::Owned("id".to_string())]),
            value: ScalarValue::Int64((N / 2) as i64),
        }),
        limit: None,
        order_by: None,
    };
    c.bench_function("query_scan_eq_on_non_indexed_field", |b| {
        b.iter(|| std::hint::black_box(db.query(std::hint::black_box(&q_scan)).unwrap()))
    });
}

criterion_group!(benches, bench_query_paths);
criterion_main!(benches);
