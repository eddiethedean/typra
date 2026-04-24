//! End-to-end-ish microbenchmarks for common workflows.
//!
//! Run: `cargo bench -p typra-core --bench workflows`

use std::collections::BTreeMap;

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use std::borrow::Cow;

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

fn bench_txn_batch_insert(c: &mut Criterion) {
    const N: i64 = 5_000;
    c.bench_function("txn_batch_insert_5k", |b| {
        b.iter(|| {
            let mut db = Database::open_in_memory().unwrap();
            let (cid, _) = db
                .register_collection(
                    "t",
                    vec![
                        path_field("id", Type::Int64),
                        path_field("tag", Type::String),
                    ],
                    "id",
                )
                .unwrap();
            db.transaction(|tx| {
                for i in 0..N {
                    let mut row = BTreeMap::new();
                    row.insert("id".to_string(), RowValue::Int64(i));
                    row.insert("tag".to_string(), RowValue::String("x".to_string()));
                    tx.insert(cid, row)?;
                }
                Ok::<(), typra_core::DbError>(())
            })
            .unwrap();
            black_box(db.collection_names());
        })
    });
}

fn bench_checkpointed_open(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("cp_open.typra");

    // Setup once: create DB, write some data, checkpoint.
    {
        let mut db = Database::open(&path).unwrap();
        let (cid, _) = db
            .register_collection("t", vec![path_field("id", Type::Int64)], "id")
            .unwrap();
        for i in 0..10_000i64 {
            let mut row = BTreeMap::new();
            row.insert("id".to_string(), RowValue::Int64(i));
            db.insert(cid, row).unwrap();
        }
        db.checkpoint().unwrap();
    }

    c.bench_function("open_checkpointed_10k", |b| {
        b.iter(|| black_box(Database::open(black_box(&path)).unwrap()))
    });
}

fn bench_compaction_in_place(c: &mut Criterion) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("compact.typra");
    {
        let mut db = Database::open(&path).unwrap();
        let fields = vec![
            path_field("id", Type::Int64),
            path_field("tag", Type::String),
        ];
        let indexes = vec![IndexDef {
            name: "tag_idx".to_string(),
            path: FieldPath(vec![Cow::Borrowed("tag")]),
            kind: IndexKind::NonUnique,
        }];
        let (cid, _) = db
            .register_collection_with_indexes("t", fields, indexes, "id")
            .unwrap();
        for i in 0..20_000i64 {
            let mut row = BTreeMap::new();
            row.insert("id".to_string(), RowValue::Int64(i));
            row.insert("tag".to_string(), RowValue::String("x".to_string()));
            db.insert(cid, row).unwrap();
        }
        db.checkpoint().unwrap();
    }

    c.bench_function("compact_in_place_20k", |b| {
        b.iter(|| {
            let mut db = Database::open(black_box(&path)).unwrap();
            db.compact_in_place().unwrap();
        })
    });
}

fn bench_indexed_query_on_disk(c: &mut Criterion) {
    use typra_core::query::{Predicate, Query};

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("idx.typra");
    let cid;
    {
        let mut db = Database::open(&path).unwrap();
        let fields = vec![
            path_field("id", Type::Int64),
            path_field("tag", Type::String),
        ];
        let indexes = vec![IndexDef {
            name: "tag_idx".to_string(),
            path: FieldPath(vec![Cow::Borrowed("tag")]),
            kind: IndexKind::NonUnique,
        }];
        let (id, _) = db
            .register_collection_with_indexes("t", fields, indexes, "id")
            .unwrap();
        cid = id;
        for i in 0..50_000i64 {
            let mut row = BTreeMap::new();
            row.insert("id".to_string(), RowValue::Int64(i));
            let tag = if i == 25_000 { "needle" } else { "other" };
            row.insert("tag".to_string(), RowValue::String(tag.to_string()));
            db.insert(cid, row).unwrap();
        }
        db.checkpoint().unwrap();
    }
    let db = Database::open(&path).unwrap();

    let q = Query {
        collection: cid,
        predicate: Some(Predicate::Eq {
            path: FieldPath(vec![Cow::Borrowed("tag")]),
            value: ScalarValue::String("needle".to_string()),
        }),
        limit: None,
        order_by: None,
    };
    c.bench_function("indexed_eq_on_disk_50k", |b| {
        b.iter(|| black_box(db.query(black_box(&q)).unwrap()))
    });
}

criterion_group!(
    benches,
    bench_txn_batch_insert,
    bench_checkpointed_open,
    bench_compaction_in_place,
    bench_indexed_query_on_disk
);
criterion_main!(benches);
