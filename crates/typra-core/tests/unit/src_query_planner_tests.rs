use std::borrow::Cow;
use std::collections::BTreeMap;

use crate::catalog::Catalog;
use crate::catalog::CatalogRecordWire;
use crate::db::LatestMap;
use crate::index::IndexState;
use crate::query::ast::{OrderBy, OrderDirection, Predicate, Query};
use crate::query::operators::RowSource;
use crate::record::RowValue;
use crate::schema::{CollectionId, FieldDef, FieldPath, IndexDef, IndexKind, Type};
use crate::{DbError, ScalarValue};

fn field(name: &str, ty: Type) -> FieldDef {
    FieldDef {
        path: FieldPath(vec![Cow::Owned(name.to_string())]),
        ty,
        constraints: vec![],
    }
}

#[test]
fn query_row_iter_vec_state_advances_and_ends() {
    let mut catalog = Catalog::default();
    let indexes = IndexState::default();
    let latest = LatestMap::default();

    catalog
        .apply_record(CatalogRecordWire::CreateCollection {
            collection_id: 1,
            name: "t".to_string(),
            schema_version: 1,
            fields: vec![field("id", Type::Int64)],
            indexes: vec![],
            primary_field: Some("id".to_string()),
        })
        .unwrap();

    // With order_by set, execute_query_iter returns Vec state (materialized).
    let q = Query {
        collection: CollectionId(1),
        predicate: None,
        limit: None,
        order_by: Some(OrderBy {
            path: FieldPath(vec![Cow::Borrowed("id")]),
            direction: OrderDirection::Asc,
        }),
    };

    let mut it = super::execute_query_iter(&catalog, &indexes, &latest, &q).unwrap();
    assert!(it.next().is_none());
    assert!(it.next().is_none());
}

#[test]
fn index_unique_source_done_and_residual_paths() {
    let mut latest = LatestMap::default();
    latest.insert(
        (1, b"pk".to_vec()),
        BTreeMap::from([("x".to_string(), RowValue::Int64(1))]),
    );

    // Residual predicate fails => yields None.
    let mut s = super::IndexUniqueSource {
        latest: &latest,
        collection_id: 1,
        pk: Some(b"pk".to_vec()),
        residual: Some(Predicate::Eq {
            path: FieldPath(vec![Cow::Borrowed("x")]),
            value: ScalarValue::Int64(2),
        }),
        done: false,
    };
    assert!(s.next_key().is_none());

    // done=true => yields None
    let mut s2 = super::IndexUniqueSource {
        latest: &latest,
        collection_id: 1,
        pk: Some(b"pk".to_vec()),
        residual: None,
        done: true,
    };
    assert!(s2.next_key().is_none());
}

#[test]
fn index_non_unique_source_skips_missing_rows_and_returns_none_at_end() {
    let mut latest = LatestMap::default();
    latest.insert(
        (1, b"present".to_vec()),
        BTreeMap::from([("x".to_string(), RowValue::Int64(1))]),
    );

    let mut s = super::IndexNonUniqueSource {
        latest: &latest,
        collection_id: 1,
        pks: vec![b"missing".to_vec(), b"present".to_vec()].into_iter(),
        residual: None,
    };
    let rk = s.next_key().unwrap().unwrap();
    assert_eq!(rk.0 .0, 1);
    assert_eq!(rk.1, b"present".to_vec());
    assert!(s.next_key().is_none());
}

struct OneMissingThenEnd {
    done: bool,
    cid: CollectionId,
    pk: Vec<u8>,
}

impl RowSource for OneMissingThenEnd {
    fn next_key(&mut self) -> Option<Result<super::RowKey, DbError>> {
        if self.done {
            None
        } else {
            self.done = true;
            Some(Ok((self.cid, self.pk.clone())))
        }
    }
}

#[test]
fn query_row_iter_source_skips_missing_rows_until_source_exhausted() {
    let latest = LatestMap::default();
    let mut it = super::QueryRowIter {
        state: super::QueryRowIterState::Source {
            latest: &latest,
            source: Box::new(OneMissingThenEnd {
                done: false,
                cid: CollectionId(1),
                pk: b"nope".to_vec(),
            }),
        },
    };
    assert!(it.next().is_none());
}

#[test]
fn execute_query_iter_with_spill_path_branches_vec_and_fallbacks() {
    let mut cat = Catalog::default();
    let idx = IndexState::default();

    let fields = vec![field("id", Type::Int64), field("x", Type::Int64)];
    let indexes = vec![IndexDef {
        name: "x_u".to_string(),
        path: FieldPath(vec![Cow::Borrowed("x")]),
        kind: IndexKind::Unique,
    }];
    cat.apply_record(CatalogRecordWire::CreateCollection {
        collection_id: 1,
        name: "t".to_string(),
        schema_version: 1,
        fields,
        indexes,
        primary_field: Some("id".to_string()),
    })
    .unwrap();

    // Latest contains one row.
    let mut latest = LatestMap::default();
    latest.insert(
        (1, b"pk".to_vec()),
        BTreeMap::from([
            ("id".to_string(), RowValue::Int64(1)),
            ("x".to_string(), RowValue::Int64(1)),
        ]),
    );

    // q.order_by None => dispatches to execute_query_iter (Source state), but no index entries
    // so it's a scan.
    let q0 = Query {
        collection: CollectionId(1),
        predicate: None,
        limit: None,
        order_by: None,
    };
    let _ = super::execute_query_iter_with_spill_path(&cat, &idx, &latest, &q0, None).unwrap();

    // order_by Some but db_path None => Vec fallback.
    let q1 = Query {
        order_by: Some(OrderBy {
            path: FieldPath(vec![Cow::Borrowed("x")]),
            direction: OrderDirection::Asc,
        }),
        ..q0.clone()
    };
    let mut it = super::execute_query_iter_with_spill_path(&cat, &idx, &latest, &q1, None).unwrap();
    assert!(it.next().is_some());
}

#[test]
fn scalar_sort_key_bytes_covers_negative_float_uuid_timestamp() {
    // Negative float takes the `bits = !bits` arm.
    let a = super::scalar_sort_key_bytes(&ScalarValue::Float64(-1.0));
    let b = super::scalar_sort_key_bytes(&ScalarValue::Float64(1.0));
    assert_ne!(a, b);

    let uuid = [7u8; 16];
    let _ = super::scalar_sort_key_bytes(&ScalarValue::Uuid(uuid));
    let _ = super::scalar_sort_key_bytes(&ScalarValue::Timestamp(-5));
    let _ = super::scalar_sort_key_bytes(&ScalarValue::String("hi".into()));
}

#[test]
fn apply_order_by_and_limit_and_scalar_partial_cmp_mismatch() {
    let mut rows = vec![
        BTreeMap::from([("x".to_string(), RowValue::Int64(2))]),
        BTreeMap::from([("x".to_string(), RowValue::Int64(1))]),
        BTreeMap::new(), // missing => None ordering branch
    ];
    let ob = OrderBy {
        path: FieldPath(vec![Cow::Borrowed("x")]),
        direction: OrderDirection::Asc,
    };
    super::apply_order_by_and_limit(&mut rows, Some(&ob), Some(2));
    assert_eq!(rows.len(), 2);

    // Mismatched types => None.
    assert!(super::scalar_partial_cmp(&ScalarValue::Int64(1), &ScalarValue::String("s".into()))
        .is_none());
}

#[test]
fn run_reader_next_item_returns_none_on_truncated_buffers() {
    let mut rr = super::RunReader::new(vec![0u8; 2]);
    assert!(rr.next_item().is_none());
}

struct ErrSource {
    emitted: bool,
}

impl RowSource for ErrSource {
    fn next_key(&mut self) -> Option<Result<super::RowKey, DbError>> {
        if self.emitted {
            None
        } else {
            self.emitted = true;
            Some(Err(DbError::Io(std::io::Error::other("boom"))))
        }
    }
}

#[test]
fn query_row_iter_source_propagates_row_source_error() {
    let latest = LatestMap::default();
    let mut it = super::QueryRowIter {
        state: super::QueryRowIterState::Source {
            latest: &latest,
            source: Box::new(ErrSource { emitted: false }),
        },
    };
    let got = it.next().unwrap();
    assert!(matches!(got, Err(DbError::Io(_))));
    assert!(it.next().is_none());
}

#[test]
fn index_unique_source_success_then_done() {
    let mut latest = LatestMap::default();
    latest.insert(
        (1, b"pk".to_vec()),
        BTreeMap::from([("x".to_string(), RowValue::Int64(1))]),
    );
    let mut s = super::IndexUniqueSource {
        latest: &latest,
        collection_id: 1,
        pk: Some(b"pk".to_vec()),
        residual: Some(Predicate::Eq {
            path: FieldPath(vec![Cow::Borrowed("x")]),
            value: ScalarValue::Int64(1),
        }),
        done: false,
    };
    assert!(s.next_key().unwrap().is_ok());
    assert!(s.next_key().is_none());
}

#[test]
fn index_non_unique_source_residual_filter_skips_row() {
    let mut latest = LatestMap::default();
    latest.insert(
        (1, b"present".to_vec()),
        BTreeMap::from([("x".to_string(), RowValue::Int64(1))]),
    );
    let mut s = super::IndexNonUniqueSource {
        latest: &latest,
        collection_id: 1,
        pks: vec![b"present".to_vec()].into_iter(),
        residual: Some(Predicate::Eq {
            path: FieldPath(vec![Cow::Borrowed("x")]),
            value: ScalarValue::Int64(2),
        }),
    };
    assert!(s.next_key().is_none());
}

#[test]
fn index_non_unique_source_residual_filter_continues_then_returns_next_match() {
    let mut latest = LatestMap::default();
    latest.insert(
        (1, b"a".to_vec()),
        BTreeMap::from([("x".to_string(), RowValue::Int64(1))]),
    );
    latest.insert(
        (1, b"b".to_vec()),
        BTreeMap::from([("x".to_string(), RowValue::Int64(2))]),
    );
    let mut s = super::IndexNonUniqueSource {
        latest: &latest,
        collection_id: 1,
        pks: vec![b"a".to_vec(), b"b".to_vec()].into_iter(),
        residual: Some(Predicate::Eq {
            path: FieldPath(vec![Cow::Borrowed("x")]),
            value: ScalarValue::Int64(2),
        }),
    };
    let rk = s.next_key().unwrap().unwrap();
    assert_eq!(rk.1, b"b".to_vec());
}

#[test]
fn execute_query_non_unique_index_lookup_hits_loop_body() {
    let mut cat = Catalog::default();
    cat.apply_record(CatalogRecordWire::CreateCollection {
        collection_id: 1,
        name: "t".to_string(),
        schema_version: 1,
        fields: vec![field("id", Type::Int64), field("x", Type::Int64)],
        indexes: vec![IndexDef {
            name: "x_n".to_string(),
            path: FieldPath(vec![Cow::Borrowed("x")]),
            kind: IndexKind::NonUnique,
        }],
        primary_field: Some("id".to_string()),
    })
    .unwrap();

    let mut latest = LatestMap::default();
    latest.insert(
        (1, b"pk".to_vec()),
        BTreeMap::from([
            ("id".to_string(), RowValue::Int64(1)),
            ("x".to_string(), RowValue::Int64(7)),
        ]),
    );

    let mut idx = IndexState::default();
    idx.apply(crate::index::IndexEntry {
        collection_id: 1,
        index_name: "x_n".to_string(),
        kind: IndexKind::NonUnique,
        op: crate::index::IndexOp::Insert,
        index_key: ScalarValue::Int64(7).canonical_key_bytes(),
        pk_key: b"pk".to_vec(),
    })
    .unwrap();
    assert!(idx
        .non_unique_lookup(1, "x_n", &ScalarValue::Int64(7).canonical_key_bytes())
        .is_some());

    let q = Query {
        collection: CollectionId(1),
        predicate: Some(Predicate::Eq {
            path: FieldPath(vec![Cow::Borrowed("x")]),
            value: ScalarValue::Int64(7),
        }),
        limit: None,
        order_by: None,
    };
    let out = super::execute_query(&cat, &idx, &latest, &q).unwrap();
    assert_eq!(out.len(), 1);
}

#[test]
fn execute_query_iter_with_spill_path_index_lookup_unique_and_nonunique() {
    let dir = tempfile::tempdir().unwrap();
    let spill_path = dir.path().join("db.typra");
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&spill_path)
        .unwrap();

    let mut cat = Catalog::default();
    cat.apply_record(CatalogRecordWire::CreateCollection {
        collection_id: 1,
        name: "t".to_string(),
        schema_version: 1,
        fields: vec![field("id", Type::Int64), field("x", Type::Int64)],
        indexes: vec![
            IndexDef {
                name: "x_u".to_string(),
                path: FieldPath(vec![Cow::Borrowed("x")]),
                kind: IndexKind::Unique,
            },
            IndexDef {
                name: "x_n".to_string(),
                path: FieldPath(vec![Cow::Borrowed("x")]),
                kind: IndexKind::NonUnique,
            },
        ],
        primary_field: Some("id".to_string()),
    })
    .unwrap();

    let mut latest = LatestMap::default();
    latest.insert(
        (1, b"pk".to_vec()),
        BTreeMap::from([
            ("id".to_string(), RowValue::Int64(1)),
            ("x".to_string(), RowValue::Int64(7)),
        ]),
    );

    let mut idx = IndexState::default();
    idx.apply(crate::index::IndexEntry {
        collection_id: 1,
        index_name: "x_u".to_string(),
        kind: IndexKind::Unique,
        op: crate::index::IndexOp::Insert,
        index_key: ScalarValue::Int64(7).canonical_key_bytes(),
        pk_key: b"pk".to_vec(),
    })
    .unwrap();
    idx.apply(crate::index::IndexEntry {
        collection_id: 1,
        index_name: "x_n".to_string(),
        kind: IndexKind::NonUnique,
        op: crate::index::IndexOp::Insert,
        index_key: ScalarValue::Int64(7).canonical_key_bytes(),
        pk_key: b"pk".to_vec(),
    })
    .unwrap();

    let base_q = Query {
        collection: CollectionId(1),
        predicate: Some(Predicate::Eq {
            path: FieldPath(vec![Cow::Borrowed("x")]),
            value: ScalarValue::Int64(7),
        }),
        limit: None,
        order_by: Some(OrderBy {
            path: FieldPath(vec![Cow::Borrowed("x")]),
            direction: OrderDirection::Asc,
        }),
    };

    let mut it_u =
        super::execute_query_iter_with_spill_path(&cat, &idx, &latest, &base_q, Some(&spill_path))
            .unwrap();
    assert!(it_u.next().is_some());
    drop(it_u);

    // Force a non-unique plan by dropping the unique index from the catalog.
    cat.apply_record(CatalogRecordWire::NewSchemaVersion {
        collection_id: 1,
        schema_version: 2,
        fields: vec![field("id", Type::Int64), field("x", Type::Int64)],
        indexes: vec![IndexDef {
            name: "x_n".to_string(),
            path: FieldPath(vec![Cow::Borrowed("x")]),
            kind: IndexKind::NonUnique,
        }],
    })
    .unwrap();

    let mut it_n =
        super::execute_query_iter_with_spill_path(&cat, &idx, &latest, &base_q, Some(&spill_path))
            .unwrap();
    assert!(it_n.next().is_some());
}

#[test]
fn remove_used_predicate_and_and_cases() {
    let used = Predicate::Eq {
        path: FieldPath(vec![Cow::Borrowed("x")]),
        value: ScalarValue::Int64(1),
    };
    assert!(super::remove_used_predicate(used.clone(), used.clone()).is_none());

    let p2 = Predicate::Eq {
        path: FieldPath(vec![Cow::Borrowed("y")]),
        value: ScalarValue::Int64(2),
    };
    let and = Predicate::And(vec![used.clone(), p2.clone()]);
    let out = super::remove_used_predicate(and, used).unwrap();
    assert_eq!(out, p2);

    // And with only the used predicate => empty residual.
    let and2 = Predicate::And(vec![p2.clone()]);
    assert!(super::remove_used_predicate(and2, p2).is_none());

    // Non-AND predicates remain as residual when a different predicate is used.
    let base = Predicate::Eq {
        path: FieldPath(vec![Cow::Borrowed("x")]),
        value: ScalarValue::Int64(1),
    };
    let other = Predicate::Eq {
        path: FieldPath(vec![Cow::Borrowed("y")]),
        value: ScalarValue::Int64(2),
    };
    assert_eq!(super::remove_used_predicate(base.clone(), other), Some(base));
}

#[test]
fn eval_predicate_comparisons_cover_all_arms() {
    let row = BTreeMap::from([("x".to_string(), RowValue::Int64(2))]);
    let path = FieldPath(vec![Cow::Borrowed("x")]);
    assert!(super::eval_predicate(
        &row,
        &Predicate::Lt {
            path: path.clone(),
            value: ScalarValue::Int64(3)
        }
    ));
    assert!(super::eval_predicate(
        &row,
        &Predicate::Lte {
            path: path.clone(),
            value: ScalarValue::Int64(2)
        }
    ));
    assert!(super::eval_predicate(
        &row,
        &Predicate::Gt {
            path: path.clone(),
            value: ScalarValue::Int64(1)
        }
    ));
    assert!(super::eval_predicate(
        &row,
        &Predicate::Gte {
            path: path.clone(),
            value: ScalarValue::Int64(2)
        }
    ));
    assert!(super::eval_predicate(
        &row,
        &Predicate::Or(vec![
            Predicate::Eq {
                path: path.clone(),
                value: ScalarValue::Int64(0)
            },
            Predicate::Eq {
                path: path.clone(),
                value: ScalarValue::Int64(2)
            }
        ])
    ));
}

#[test]
fn apply_order_by_and_limit_none_none_ordering_cases() {
    let mut rows = vec![BTreeMap::new(), BTreeMap::new()];
    let ob = OrderBy {
        path: FieldPath(vec![Cow::Borrowed("x")]),
        direction: OrderDirection::Asc,
    };
    super::apply_order_by_and_limit(&mut rows, Some(&ob), None);
    assert_eq!(rows.len(), 2);
}

#[test]
fn scalar_partial_cmp_covers_all_variants() {
    assert!(super::scalar_partial_cmp(&ScalarValue::Bool(false), &ScalarValue::Bool(true)).is_some());
    assert!(super::scalar_partial_cmp(&ScalarValue::Int64(1), &ScalarValue::Int64(2)).is_some());
    assert!(super::scalar_partial_cmp(&ScalarValue::Uint64(1), &ScalarValue::Uint64(2)).is_some());
    assert!(super::scalar_partial_cmp(&ScalarValue::Float64(1.0), &ScalarValue::Float64(2.0)).is_some());
    assert!(
        super::scalar_partial_cmp(&ScalarValue::String("a".into()), &ScalarValue::String("b".into()))
            .is_some()
    );
    assert!(super::scalar_partial_cmp(&ScalarValue::Bytes(vec![1]), &ScalarValue::Bytes(vec![2])).is_some());
    assert!(super::scalar_partial_cmp(&ScalarValue::Uuid([0u8;16]), &ScalarValue::Uuid([1u8;16])).is_some());
    assert!(super::scalar_partial_cmp(&ScalarValue::Timestamp(1), &ScalarValue::Timestamp(2)).is_some());
}

#[test]
fn heap_item_equality_is_exercised() {
    let a = super::HeapItem {
        run_idx: 0,
        none_flag: 0,
        sort_key: vec![1, 2],
        pk: vec![9],
        dir: OrderDirection::Asc,
    };
    let b = super::HeapItem {
        run_idx: 1,
        none_flag: 0,
        sort_key: vec![1, 2],
        pk: vec![9],
        dir: OrderDirection::Asc,
    };
    assert!(a == b);
}

struct KeyIter {
    cid: CollectionId,
    keys: std::vec::IntoIter<Vec<u8>>,
}

impl RowSource for KeyIter {
    fn next_key(&mut self) -> Option<Result<super::RowKey, DbError>> {
        let pk = self.keys.next()?;
        Some(Ok((self.cid, pk)))
    }
}

#[test]
fn external_sort_source_spills_multiple_runs_and_merges() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("spill.typra");
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&path)
        .unwrap();
    let store = crate::storage::FileStore::new(file);
    let spill = crate::spill::TempSpillFile::new(store).unwrap();

    let mut latest = LatestMap::default();
    let mut keys = Vec::new();
    for i in 0..4097u32 {
        let pk = i.to_le_bytes().to_vec();
        keys.push(pk.clone());
        let mut row = BTreeMap::new();
        // Make some rows missing the sort key to exercise none_flag ordering.
        if i % 100 != 0 {
            row.insert("v".to_string(), RowValue::Int64(i as i64));
        }
        latest.insert((1, pk), row);
    }
    let input: Box<dyn RowSource> = Box::new(KeyIter {
        cid: CollectionId(1),
        keys: keys.into_iter().collect::<Vec<_>>().into_iter(),
    });
    let ob = OrderBy {
        path: FieldPath(vec![Cow::Borrowed("v")]),
        direction: OrderDirection::Asc,
    };
    let mut src = super::ExternalSortSource::new(spill, &latest, input, 1, ob).unwrap();

    // Pull a few keys; this drives heap pop + refill and run-reader decoding.
    let mut seen = 0usize;
    while let Some(rk) = src.next_key() {
        rk.unwrap();
        seen += 1;
        if seen > 20 {
            break;
        }
    }
    assert!(seen > 0);
}

#[test]
fn apply_order_by_some_none_branch_is_hit() {
    let mut rows = vec![
        BTreeMap::from([("x".to_string(), RowValue::Int64(1))]),
        BTreeMap::new(),
    ];
    let ob = OrderBy {
        path: FieldPath(vec![Cow::Borrowed("x")]),
        direction: OrderDirection::Asc,
    };
    super::apply_order_by_and_limit(&mut rows, Some(&ob), None);
    assert_eq!(rows.len(), 2);
}

#[test]
fn apply_order_by_hits_some_none_and_none_some_cases() {
    // Craft three rows so the sorting closure compares Some vs None in both directions.
    let mut rows = vec![
        BTreeMap::from([("x".to_string(), RowValue::Int64(1))]),
        BTreeMap::new(),
        BTreeMap::from([("x".to_string(), RowValue::Int64(0))]),
    ];
    let ob = OrderBy {
        path: FieldPath(vec![Cow::Borrowed("x")]),
        direction: OrderDirection::Asc,
    };
    super::apply_order_by_and_limit(&mut rows, Some(&ob), None);
    assert_eq!(rows.len(), 3);
}

