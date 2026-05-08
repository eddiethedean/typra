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
fn execute_query_non_unique_index_lookup_missing_key_returns_empty() {
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
        .non_unique_lookup(1, "x_n", &ScalarValue::Int64(99).canonical_key_bytes())
        .is_none());

    let q = Query {
        collection: CollectionId(1),
        predicate: Some(Predicate::Eq {
            path: FieldPath(vec![Cow::Borrowed("x")]),
            value: ScalarValue::Int64(99),
        }),
        limit: None,
        order_by: None,
    };
    assert!(super::execute_query(&cat, &idx, &latest, &q).unwrap().is_empty());
}

#[test]
fn execute_query_non_unique_index_lookup_limit_exceeds_matches_no_early_break() {
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
        (1, b"p1".to_vec()),
        BTreeMap::from([
            ("id".to_string(), RowValue::Int64(1)),
            ("x".to_string(), RowValue::Int64(7)),
        ]),
    );
    latest.insert(
        (1, b"p2".to_vec()),
        BTreeMap::from([
            ("id".to_string(), RowValue::Int64(2)),
            ("x".to_string(), RowValue::Int64(7)),
        ]),
    );

    let mut idx = IndexState::default();
    for pk in [b"p1".as_slice(), b"p2".as_slice()] {
        idx.apply(crate::index::IndexEntry {
            collection_id: 1,
            index_name: "x_n".to_string(),
            kind: IndexKind::NonUnique,
            op: crate::index::IndexOp::Insert,
            index_key: ScalarValue::Int64(7).canonical_key_bytes(),
            pk_key: pk.to_vec(),
        })
        .unwrap();
    }

    let q = Query {
        collection: CollectionId(1),
        predicate: Some(Predicate::Eq {
            path: FieldPath(vec![Cow::Borrowed("x")]),
            value: ScalarValue::Int64(7),
        }),
        limit: Some(10),
        order_by: None,
    };
    let out = super::execute_query(&cat, &idx, &latest, &q).unwrap();
    assert_eq!(out.len(), 2);
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
fn execute_query_iter_with_spill_path_collection_scan_orders() {
    let dir = tempfile::tempdir().unwrap();
    let spill_path = dir.path().join("spill.typra");
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
        indexes: vec![],
        primary_field: Some("id".to_string()),
    })
    .unwrap();

    let mut latest = LatestMap::default();
    latest.insert(
        (1, b"a".to_vec()),
        BTreeMap::from([
            ("id".to_string(), RowValue::Int64(2)),
            ("x".to_string(), RowValue::Int64(2)),
        ]),
    );
    latest.insert(
        (1, b"b".to_vec()),
        BTreeMap::from([
            ("id".to_string(), RowValue::Int64(1)),
            ("x".to_string(), RowValue::Int64(1)),
        ]),
    );

    let idx = IndexState::default();
    let q = Query {
        collection: CollectionId(1),
        predicate: None,
        limit: None,
        order_by: Some(OrderBy {
            path: FieldPath(vec![Cow::Borrowed("x")]),
            direction: OrderDirection::Asc,
        }),
    };

    let mut it =
        super::execute_query_iter_with_spill_path(&cat, &idx, &latest, &q, Some(&spill_path))
            .unwrap();
    assert!(it.next().is_some());
    assert!(it.next().is_some());
    assert!(it.next().is_none());
}

#[test]
fn sorted_query_spill_store_open_hook_propagates_errors() {
    use std::io;

    struct ClearHookGuard;
    impl Drop for ClearHookGuard {
        fn drop(&mut self) {
            super::test_set_sorted_query_spill_store_open_hook(None);
        }
    }
    let _clear = ClearHookGuard;

    let dir = tempfile::tempdir().unwrap();
    let spill_path = dir.path().join("hook_spill.typra");
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&spill_path)
        .unwrap();

    super::test_set_sorted_query_spill_store_open_hook(Some(Box::new(|_| {
        Err(DbError::Io(io::Error::other(
            "sorted spill store open hook deliberate failure",
        )))
    })));

    let mut cat = Catalog::default();
    cat.apply_record(CatalogRecordWire::CreateCollection {
        collection_id: 1,
        name: "t".to_string(),
        schema_version: 1,
        fields: vec![field("id", Type::Int64), field("x", Type::Int64)],
        indexes: vec![],
        primary_field: Some("id".to_string()),
    })
    .unwrap();

    let mut latest = LatestMap::default();
    latest.insert(
        (1, b"a".to_vec()),
        BTreeMap::from([
            ("id".to_string(), RowValue::Int64(1)),
            ("x".to_string(), RowValue::Int64(1)),
        ]),
    );

    let idx = IndexState::default();
    let q = Query {
        collection: CollectionId(1),
        predicate: None,
        limit: None,
        order_by: Some(OrderBy {
            path: FieldPath(vec![Cow::Borrowed("x")]),
            direction: OrderDirection::Asc,
        }),
    };

    let result = super::execute_query_iter_with_spill_path(&cat, &idx, &latest, &q, Some(&spill_path));
    match result {
        Ok(_) => panic!("expected spill open hook Io error"),
        Err(err) => assert!(
            matches!(err, DbError::Io(_)),
            "expected hook Io from sorted spill setup, got {err:?}"
        ),
    }
}

#[test]
fn sorted_query_spill_temp_spill_file_new_surfaces_store_len_error_via_override_hook() {
    super::test_set_sorted_query_spill_store_override_hook(Some(Box::new(|_path| {
        Ok(super::SortedQuerySpillStore::FailLen)
    })));

    let mut cat = Catalog::default();
    cat.apply_record(CatalogRecordWire::CreateCollection {
        collection_id: 1,
        name: "t".to_string(),
        schema_version: 1,
        fields: vec![field("id", Type::Int64), field("x", Type::Int64)],
        indexes: vec![],
        primary_field: Some("id".to_string()),
    })
    .unwrap();

    let indexes = IndexState::default();
    let latest = LatestMap::default();
    let q = Query {
        collection: CollectionId(1),
        predicate: None,
        limit: None,
        order_by: Some(OrderBy {
            path: FieldPath(vec![Cow::Borrowed("x")]),
            direction: OrderDirection::Asc,
        }),
    };

    let dir = tempfile::tempdir().unwrap();
    let spill_path = dir.path().join("spill.typra");
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&spill_path)
        .unwrap();

    let err = match super::execute_query_iter_with_spill_path(
        &cat,
        &indexes,
        &latest,
        &q,
        Some(&spill_path),
    ) {
        Ok(_) => panic!("expected Err"),
        Err(e) => e,
    };
    super::test_set_sorted_query_spill_store_override_hook(None);

    match err {
        DbError::Io(_) => {}
        other => panic!("expected Io, got {other:?}"),
    }
}

#[test]
fn sorted_query_spill_store_override_fail_len_covers_store_trait_methods() {
    use crate::storage::Store;

    let mut s = super::SortedQuerySpillStore::FailLen;

    // len
    assert!(s.len().is_err());

    // read
    let mut buf = [0u8; 8];
    assert!(s.read_exact_at(0, &mut buf).is_err());

    // write
    assert!(s.write_all_at(0, b"hi").is_err());

    // sync/truncate are ok for the synthetic store
    s.sync().unwrap();
    s.truncate(0).unwrap();
}

#[test]
fn sorted_query_spill_file_store_write_budget_propagates_during_external_sort() {
    use std::cell::Cell;
    use std::rc::Rc;

    struct ClearHookGuard;
    impl Drop for ClearHookGuard {
        fn drop(&mut self) {
            super::test_set_sorted_query_spill_store_open_hook(None);
        }
    }
    let _clear = ClearHookGuard;

    let dir = tempfile::tempdir().unwrap();
    let spill_path = dir.path().join("budget_spill.typra");

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
        indexes: vec![],
        primary_field: Some("id".to_string()),
    })
    .unwrap();

    let mut latest = LatestMap::default();
    latest.insert(
        (1, b"a".to_vec()),
        BTreeMap::from([
            ("id".to_string(), RowValue::Int64(2)),
            ("x".to_string(), RowValue::Int64(2)),
        ]),
    );
    latest.insert(
        (1, b"b".to_vec()),
        BTreeMap::from([
            ("id".to_string(), RowValue::Int64(1)),
            ("x".to_string(), RowValue::Int64(1)),
        ]),
    );

    let idx = IndexState::default();
    let q = Query {
        collection: CollectionId(1),
        predicate: None,
        limit: None,
        order_by: Some(OrderBy {
            path: FieldPath(vec![Cow::Borrowed("x")]),
            direction: OrderDirection::Asc,
        }),
    };

    let calibration_counter = Rc::new(Cell::new(0usize));
    let cc = calibration_counter.clone();
    super::test_set_sorted_query_spill_store_open_hook(Some(Box::new(move |path| {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(path)
            .map_err(DbError::Io)?;
        Ok(crate::storage::FileStore::new_for_test(
            file,
            Some(cc.clone()),
            None,
        ))
    })));

    super::execute_query_iter_with_spill_path(&cat, &idx, &latest, &q, Some(&spill_path)).unwrap();

    let w = calibration_counter.get();
    assert!(
        w > 0,
        "calibration expects non-zero FileStore writes during sorted spill setup"
    );

    super::test_set_sorted_query_spill_store_open_hook(None);

    std::fs::OpenOptions::new()
        .write(true)
        .truncate(true)
        .open(&spill_path)
        .unwrap();

    let budget_remaining = Rc::new(Cell::new(w.saturating_sub(1)));
    let br = budget_remaining.clone();
    super::test_set_sorted_query_spill_store_open_hook(Some(Box::new(move |path| {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(path)
            .map_err(DbError::Io)?;
        Ok(crate::storage::FileStore::new_for_test(file, None, Some(br.clone())))
    })));

    let budget_result =
        super::execute_query_iter_with_spill_path(&cat, &idx, &latest, &q, Some(&spill_path));
    match budget_result {
        Ok(_) => panic!("expected Io once FileStore write budget is exhausted mid external sort"),
        Err(err) => assert!(
            matches!(err, DbError::Io(_)),
            "expected budget Io propagation, got {err:?}"
        ),
    }
}

#[test]
fn sorted_query_spill_file_path_handles_empty_sorted_run_when_snapshot_empty() {
    let dir = tempfile::tempdir().unwrap();
    let spill_path = dir.path().join("empty_sorted_run.typra");
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
        indexes: vec![],
        primary_field: Some("id".to_string()),
    })
    .unwrap();

    let idx = IndexState::default();
    let latest_empty = LatestMap::default();

    let q = Query {
        collection: CollectionId(1),
        predicate: None,
        limit: None,
        order_by: Some(OrderBy {
            path: FieldPath(vec![Cow::Borrowed("x")]),
            direction: OrderDirection::Asc,
        }),
    };

    let mut spill_it = super::execute_query_iter_with_spill_path(
        &cat,
        &idx,
        &latest_empty,
        &q,
        Some(&spill_path),
    )
    .unwrap();
    assert!(
        spill_it.next().is_none(),
        "empty snapshot should spill-sort without accumulating sort batches"
    );
}

#[test]
fn index_unique_source_returns_none_when_pk_row_missing() {
    let latest = LatestMap::default();
    let mut src = super::IndexUniqueSource {
        latest: &latest,
        collection_id: 1,
        pk: Some(b"k".to_vec()),
        residual: None,
        done: false,
    };
    assert!(src.next_key().is_none());
}

#[test]
fn execute_query_iter_order_by_propagates_unknown_collection_error() {
    let catalog = Catalog::default();
    let indexes = IndexState::default();
    let latest = LatestMap::default();
    let q = Query {
        collection: CollectionId(1),
        predicate: None,
        limit: None,
        order_by: Some(OrderBy {
            path: FieldPath(vec![Cow::Borrowed("x")]),
            direction: OrderDirection::Asc,
        }),
    };

    assert!(super::execute_query_iter(&catalog, &indexes, &latest, &q).is_err());
}

#[test]
fn execute_query_unique_index_lookup_with_missing_pk_does_not_push_row() {
    let mut cat = Catalog::default();
    cat.apply_record(CatalogRecordWire::CreateCollection {
        collection_id: 1,
        name: "t".to_string(),
        schema_version: 1,
        fields: vec![field("id", Type::Int64), field("x", Type::Int64)],
        indexes: vec![IndexDef {
            name: "x_u".to_string(),
            path: FieldPath(vec![Cow::Borrowed("x")]),
            kind: IndexKind::Unique,
        }],
        primary_field: Some("id".to_string()),
    })
    .unwrap();

    let latest = LatestMap::default(); // pk row intentionally missing
    let mut idx = IndexState::default();
    idx.apply(crate::index::IndexEntry {
        collection_id: 1,
        index_name: "x_u".to_string(),
        kind: IndexKind::Unique,
        op: crate::index::IndexOp::Insert,
        index_key: ScalarValue::Int64(7).canonical_key_bytes(),
        pk_key: b"pk_missing".to_vec(),
    })
    .unwrap();

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
    assert!(out.is_empty());
}

#[test]
fn sorted_query_spill_path_some_propagates_unknown_collection_error() {
    let dir = tempfile::tempdir().unwrap();
    let spill_path = dir.path().join("spill.typra");
    std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(&spill_path)
        .unwrap();

    let catalog = Catalog::default(); // collection missing
    let indexes = IndexState::default();
    let latest = LatestMap::default();
    let q = Query {
        collection: CollectionId(1),
        predicate: None,
        limit: None,
        order_by: Some(OrderBy {
            path: FieldPath(vec![Cow::Borrowed("x")]),
            direction: OrderDirection::Asc,
        }),
    };

    assert!(
        super::execute_query_iter_with_spill_path(&catalog, &indexes, &latest, &q, Some(&spill_path))
            .is_err()
    );
}

#[test]
fn sorted_query_spill_open_missing_file_maps_to_io_error() {
    let mut cat = Catalog::default();
    cat.apply_record(CatalogRecordWire::CreateCollection {
        collection_id: 1,
        name: "t".to_string(),
        schema_version: 1,
        fields: vec![field("id", Type::Int64), field("x", Type::Int64)],
        indexes: vec![],
        primary_field: Some("id".to_string()),
    })
    .unwrap();

    let indexes = IndexState::default();
    let latest = LatestMap::default();
    let q = Query {
        collection: CollectionId(1),
        predicate: None,
        limit: None,
        order_by: Some(OrderBy {
            path: FieldPath(vec![Cow::Borrowed("x")]),
            direction: OrderDirection::Asc,
        }),
    };

    let dir = tempfile::tempdir().unwrap();
    let missing = dir.path().join("does_not_exist.typra");
    let err = match super::execute_query_iter_with_spill_path(
        &cat,
        &indexes,
        &latest,
        &q,
        Some(&missing),
    )
    {
        Ok(_) => panic!("expected Err"),
        Err(e) => e,
    };

    match err {
        DbError::Io(_) => {}
        other => panic!("expected Io, got {other:?}"),
    }
}

#[test]
fn sorted_query_spill_db_path_none_propagates_execute_query_error() {
    let catalog = Catalog::default();
    let indexes = IndexState::default();
    let latest = LatestMap::default();
    let q = Query {
        collection: CollectionId(1),
        predicate: None,
        limit: None,
        order_by: Some(OrderBy {
            path: FieldPath(vec![Cow::Borrowed("x")]),
            direction: OrderDirection::Asc,
        }),
    };

    assert!(super::execute_query_iter_with_spill_path(&catalog, &indexes, &latest, &q, None).is_err());
}

#[test]
fn sort_item_for_returns_none_when_row_is_missing() {
    let latest = LatestMap::default();
    let key: super::RowKey = (CollectionId(1), b"k".to_vec());
    let order_by = OrderBy {
        path: FieldPath(vec![Cow::Borrowed("x")]),
        direction: OrderDirection::Asc,
    };
    assert!(super::sort_item_for(&latest, &key, &order_by).is_none());
}

#[test]
fn scalar_sort_key_bytes_bool_covers_true_and_false_branches() {
    let t = super::scalar_sort_key_bytes(&ScalarValue::Bool(true));
    let f = super::scalar_sort_key_bytes(&ScalarValue::Bool(false));
    assert_eq!(t, vec![0, 1]);
    assert_eq!(f, vec![0, 0]);
}

#[test]
fn run_reader_next_item_returns_none_when_key_slice_truncated() {
    let mut buf = vec![0u8]; // none_flag
    buf.extend_from_slice(&(5u32).to_le_bytes()); // key_len=5, but only 2 bytes follow
    buf.extend_from_slice(b"ab");
    let mut rr = super::RunReader::new(buf);
    assert!(rr.next_item().is_none());
}

#[test]
fn run_reader_next_item_returns_none_when_pk_len_truncated() {
    let mut buf = vec![0u8]; // none_flag
    buf.extend_from_slice(&(2u32).to_le_bytes()); // key_len=2
    buf.extend_from_slice(b"ab"); // key bytes ok; missing pk_len u32
    let mut rr = super::RunReader::new(buf);
    assert!(rr.next_item().is_none());
}

#[test]
fn run_reader_next_item_returns_none_when_pk_slice_truncated() {
    let mut buf = vec![0u8]; // none_flag
    buf.extend_from_slice(&(1u32).to_le_bytes()); // key_len=1
    buf.extend_from_slice(b"a"); // key ok
    buf.extend_from_slice(&(3u32).to_le_bytes()); // pk_len=3, but only 1 byte follow
    buf.extend_from_slice(b"b");
    let mut rr = super::RunReader::new(buf);
    assert!(rr.next_item().is_none());
}

#[test]
fn external_sort_source_new_propagates_input_key_error() {
    #[derive(Default)]
    struct ErrSource {
        returned: bool,
    }
    impl super::RowSource for ErrSource {
        fn next_key(&mut self) -> Option<Result<super::RowKey, DbError>> {
            if self.returned {
                None
            } else {
                self.returned = true;
                Some(Err(DbError::Io(std::io::Error::other("boom"))))
            }
        }
    }

    let spill = crate::spill::TempSpillFile::new(crate::storage::VecStore::new()).unwrap();
    let latest = LatestMap::default();
    let order_by = OrderBy {
        path: FieldPath(vec![Cow::Borrowed("x")]),
        direction: OrderDirection::Asc,
    };
    let err = match super::ExternalSortSource::new(
        spill,
        &latest,
        Box::new(ErrSource::default()),
        1,
        order_by,
    )
    {
        Ok(_) => panic!("expected Err"),
        Err(e) => e,
    };
    match err {
        DbError::Io(_) => {}
        other => panic!("expected Io, got {other:?}"),
    }
}

#[test]
fn external_sort_source_new_surfaces_flush_error_on_run_limit() {
    use crate::storage::{Store, VecStore};

    #[derive(Default)]
    struct KeySource {
        i: usize,
    }
    impl super::RowSource for KeySource {
        fn next_key(&mut self) -> Option<Result<super::RowKey, DbError>> {
            const RUN_KEYS: usize = 2048;
            if self.i >= RUN_KEYS {
                return None;
            }
            let pk = self.i.to_le_bytes().to_vec();
            self.i += 1;
            Some(Ok((CollectionId(1), pk)))
        }
    }

    #[derive(Default)]
    struct FailWriteStore {
        inner: VecStore,
    }
    impl Store for FailWriteStore {
        fn len(&self) -> Result<u64, DbError> {
            self.inner.len()
        }
        fn read_exact_at(&mut self, offset: u64, buf: &mut [u8]) -> Result<(), DbError> {
            self.inner.read_exact_at(offset, buf)
        }
        fn write_all_at(&mut self, _offset: u64, _buf: &[u8]) -> Result<(), DbError> {
            Err(DbError::Io(std::io::Error::other("write failed")))
        }
        fn sync(&mut self) -> Result<(), DbError> {
            self.inner.sync()
        }
        fn truncate(&mut self, len: u64) -> Result<(), DbError> {
            self.inner.truncate(len)
        }
    }

    let mut latest = LatestMap::default();
    for i in 0..2048usize {
        let pk = i.to_le_bytes().to_vec();
        latest.insert(
            (1, pk),
            BTreeMap::from([("x".to_string(), RowValue::Int64(i as i64))]),
        );
    }

    let spill = crate::spill::TempSpillFile::new(FailWriteStore::default()).unwrap();
    let order_by = OrderBy {
        path: FieldPath(vec![Cow::Borrowed("x")]),
        direction: OrderDirection::Asc,
    };

    assert!(
        super::ExternalSortSource::new(spill, &latest, Box::new(KeySource::default()), 1, order_by)
            .is_err()
    );
}

#[test]
fn external_sort_source_new_skips_sort_item_when_row_missing() {
    use crate::storage::VecStore;

    #[derive(Default)]
    struct OneKey {
        done: bool,
    }
    impl super::RowSource for OneKey {
        fn next_key(&mut self) -> Option<Result<super::RowKey, DbError>> {
            if self.done {
                None
            } else {
                self.done = true;
                Some(Ok((CollectionId(1), b"missing".to_vec())))
            }
        }
    }

    let spill = crate::spill::TempSpillFile::new(VecStore::new()).unwrap();
    let latest = LatestMap::default(); // row missing => sort_item_for returns None
    let order_by = OrderBy {
        path: FieldPath(vec![Cow::Borrowed("x")]),
        direction: OrderDirection::Asc,
    };

    let mut src =
        super::ExternalSortSource::new(spill, &latest, Box::new(OneKey::default()), 1, order_by)
            .unwrap();
    assert!(src.next_key().is_none());
}

#[test]
fn external_sort_source_new_tolerates_corrupt_run_payload_by_skipping_seed_item() {
    use crate::storage::{Store, VecStore};

    #[derive(Default)]
    struct OneKey {
        done: bool,
    }
    impl super::RowSource for OneKey {
        fn next_key(&mut self) -> Option<Result<super::RowKey, DbError>> {
            if self.done {
                None
            } else {
                self.done = true;
                Some(Ok((CollectionId(1), b"k".to_vec())))
            }
        }
    }

    #[derive(Default)]
    struct CorruptReadStore {
        inner: VecStore,
    }
    impl Store for CorruptReadStore {
        fn len(&self) -> Result<u64, DbError> {
            self.inner.len()
        }
        fn read_exact_at(&mut self, offset: u64, buf: &mut [u8]) -> Result<(), DbError> {
            self.inner.read_exact_at(offset, buf)?;
            // Corrupt the encoded key_len so RunReader::next_item() returns None.
            if buf.len() >= 5 {
                buf[1..5].copy_from_slice(&u32::MAX.to_le_bytes());
            }
            Ok(())
        }
        fn write_all_at(&mut self, offset: u64, data: &[u8]) -> Result<(), DbError> {
            self.inner.write_all_at(offset, data)
        }
        fn sync(&mut self) -> Result<(), DbError> {
            self.inner.sync()
        }
        fn truncate(&mut self, len: u64) -> Result<(), DbError> {
            self.inner.truncate(len)
        }
    }

    let mut latest = LatestMap::default();
    latest.insert(
        (1, b"k".to_vec()),
        BTreeMap::from([("x".to_string(), RowValue::Int64(1))]),
    );

    let spill = crate::spill::TempSpillFile::new(CorruptReadStore::default()).unwrap();
    let order_by = OrderBy {
        path: FieldPath(vec![Cow::Borrowed("x")]),
        direction: OrderDirection::Asc,
    };

    let mut src =
        super::ExternalSortSource::new(spill, &latest, Box::new(OneKey::default()), 1, order_by)
            .unwrap();
    assert!(src.next_key().is_none());
}

#[test]
fn external_sort_source_new_surfaces_read_temp_payload_error() {
    use crate::storage::{Store, VecStore};

    #[derive(Default)]
    struct SingleKeySource {
        done: bool,
    }
    impl super::RowSource for SingleKeySource {
        fn next_key(&mut self) -> Option<Result<super::RowKey, DbError>> {
            if self.done {
                None
            } else {
                self.done = true;
                Some(Ok((CollectionId(1), b"k".to_vec())))
            }
        }
    }

    #[derive(Default)]
    struct FailReadStore {
        inner: VecStore,
    }
    impl Store for FailReadStore {
        fn len(&self) -> Result<u64, DbError> {
            self.inner.len()
        }
        fn read_exact_at(&mut self, _offset: u64, _buf: &mut [u8]) -> Result<(), DbError> {
            Err(DbError::Io(std::io::Error::other("read failed")))
        }
        fn write_all_at(&mut self, offset: u64, buf: &[u8]) -> Result<(), DbError> {
            self.inner.write_all_at(offset, buf)
        }
        fn sync(&mut self) -> Result<(), DbError> {
            self.inner.sync()
        }
        fn truncate(&mut self, len: u64) -> Result<(), DbError> {
            self.inner.truncate(len)
        }
    }

    let mut latest = LatestMap::default();
    latest.insert(
        (1, b"k".to_vec()),
        BTreeMap::from([("x".to_string(), RowValue::Int64(1))]),
    );

    let spill = crate::spill::TempSpillFile::new(FailReadStore::default()).unwrap();
    let order_by = OrderBy {
        path: FieldPath(vec![Cow::Borrowed("x")]),
        direction: OrderDirection::Asc,
    };

    let err = match super::ExternalSortSource::new(
        spill,
        &latest,
        Box::new(SingleKeySource::default()),
        1,
        order_by,
    )
    {
        Ok(_) => panic!("expected Err"),
        Err(e) => e,
    };
    match err {
        DbError::Io(_) => {}
        other => panic!("expected Io, got {other:?}"),
    }
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
fn external_sort_flushes_residual_run_without_full_batches() {
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
    // Few enough keys never to exceed `RUN_KEYS`, so sorting flushes exactly one residual run.
    for i in 0..100u32 {
        let pk = i.to_le_bytes().to_vec();
        keys.push(pk.clone());
        let mut row = BTreeMap::new();
        row.insert("v".to_string(), RowValue::Int64(i as i64));
        latest.insert((1, pk), row);
    }
    let input: Box<dyn RowSource> = Box::new(KeyIter {
        cid: CollectionId(1),
        keys: keys.into_iter(),
    });
    let ob = OrderBy {
        path: FieldPath(vec![Cow::Borrowed("v")]),
        direction: OrderDirection::Asc,
    };
    let mut src = super::ExternalSortSource::new(spill, &latest, input, 1, ob).unwrap();
    for _ in 0..5 {
        assert!(src.next_key().unwrap().is_ok());
    }
}

#[test]
fn external_sort_propagates_spill_segment_write_budget_exhaustion() {
    use std::cell::Cell;
    use std::io;
    use std::rc::Rc;

    struct CountWrites {
        n: Rc<Cell<usize>>,
        inner: crate::storage::VecStore,
    }

    impl crate::storage::Store for CountWrites {
        fn len(&self) -> Result<u64, DbError> {
            self.inner.len()
        }

        fn read_exact_at(&mut self, offset: u64, buf: &mut [u8]) -> Result<(), DbError> {
            self.inner.read_exact_at(offset, buf)
        }

        fn write_all_at(&mut self, offset: u64, buf: &[u8]) -> Result<(), DbError> {
            self.n.set(self.n.get().saturating_add(1));
            self.inner.write_all_at(offset, buf)
        }

        fn sync(&mut self) -> Result<(), DbError> {
            self.inner.sync()
        }

        fn truncate(&mut self, len: u64) -> Result<(), DbError> {
            self.inner.truncate(len)
        }
    }

    struct BudgetWrites {
        remaining: Cell<usize>,
        inner: crate::storage::VecStore,
    }

    impl crate::storage::Store for BudgetWrites {
        fn len(&self) -> Result<u64, DbError> {
            self.inner.len()
        }

        fn read_exact_at(&mut self, offset: u64, buf: &mut [u8]) -> Result<(), DbError> {
            self.inner.read_exact_at(offset, buf)
        }

        fn write_all_at(&mut self, offset: u64, buf: &[u8]) -> Result<(), DbError> {
            let r = self.remaining.get();
            if r == 0 {
                return Err(DbError::Io(io::Error::other(
                    "spill append write budget exhausted",
                )));
            }
            self.remaining.set(r.saturating_sub(1));
            self.inner.write_all_at(offset, buf)
        }

        fn sync(&mut self) -> Result<(), DbError> {
            self.inner.sync()
        }

        fn truncate(&mut self, len: u64) -> Result<(), DbError> {
            self.inner.truncate(len)
        }
    }

    let mut latest = LatestMap::default();
    latest.insert(
        (1, b"k".to_vec()),
        BTreeMap::from([
            ("id".to_string(), RowValue::Int64(1)),
            ("x".to_string(), RowValue::Int64(7)),
        ]),
    );

    let order_by = OrderBy {
        path: FieldPath(vec![Cow::Borrowed("x")]),
        direction: OrderDirection::Asc,
    };

    let wc = Rc::new(Cell::new(0usize));
    {
        let spill = crate::spill::TempSpillFile::new(CountWrites {
            n: wc.clone(),
            inner: crate::storage::VecStore::new(),
        })
        .unwrap();
        let scan: Box<dyn RowSource> = Box::new(super::ScanSource {
            it: latest.iter(),
            collection_id: 1,
            predicate: None,
        });
        let _sorted = super::ExternalSortSource::new(
            spill,
            &latest,
            scan,
            1,
            order_by.clone(),
        )
        .unwrap();
    }

    let w = wc.get();
    assert!(
        w > 0,
        "expected spilled temp segments to involve at least one store write_all_at batch"
    );

    let spill_b = crate::spill::TempSpillFile::new(BudgetWrites {
        remaining: Cell::new(w.saturating_sub(1)),
        inner: crate::storage::VecStore::new(),
    })
    .unwrap();
    let scan_b: Box<dyn RowSource> = Box::new(super::ScanSource {
        it: latest.iter(),
        collection_id: 1,
        predicate: None,
    });
    match super::ExternalSortSource::new(spill_b, &latest, scan_b, 1, order_by) {
        Ok(_) => panic!("expected ExternalSortSource::new to fail when spill append budget exhausted"),
        Err(err) => assert!(
            matches!(err, DbError::Io(_)),
            "expected Io propagating append_temp_segment/store budget exhaustion, got {err:?}"
        ),
    }
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

