//! Multi-write transactions and nested-transaction errors.

use std::borrow::Cow;
use std::collections::BTreeMap;

use typra_core::query::{Predicate, Query};
use typra_core::record::RowValue;
use typra_core::schema::{FieldDef, FieldPath, IndexDef, IndexKind, Type};
use typra_core::storage::Store;
use typra_core::{CollectionId, Database, OpenOptions, RecoveryMode};

fn title_field() -> FieldDef {
    FieldDef {
        path: FieldPath(vec![Cow::Owned("title".to_string())]),
        ty: Type::String,
        constraints: vec![],
    }
}

fn id_field() -> FieldDef {
    FieldDef {
        path: FieldPath(vec![Cow::Owned("id".to_string())]),
        ty: Type::Int64,
        constraints: vec![],
    }
}

fn sku_field() -> FieldDef {
    FieldDef {
        path: FieldPath(vec![Cow::Owned("sku".to_string())]),
        ty: Type::String,
        constraints: vec![],
    }
}

fn qty_field() -> FieldDef {
    FieldDef {
        path: FieldPath(vec![Cow::Owned("qty".to_string())]),
        ty: Type::Int64,
        constraints: vec![],
    }
}

fn status_field() -> FieldDef {
    FieldDef {
        path: FieldPath(vec![Cow::Owned("status".to_string())]),
        ty: Type::String,
        constraints: vec![],
    }
}

#[test]
fn transaction_inserts_visible_after_commit_and_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("txn.typra");
    {
        let mut db = Database::open(&path).unwrap();
        db.register_collection("books", vec![title_field()], "title")
            .unwrap();
        let id = CollectionId(1);
        db.transaction(|d| {
            let mut r = BTreeMap::new();
            r.insert("title".into(), RowValue::String("a".into()));
            d.insert(id, r)?;
            let mut r2 = BTreeMap::new();
            r2.insert("title".into(), RowValue::String("b".into()));
            d.insert(id, r2)?;
            Ok::<(), typra_core::DbError>(())
        })
        .unwrap();
    }
    let db = Database::open(&path).unwrap();
    let id = CollectionId(1);
    assert!(db
        .get(id, &typra_core::ScalarValue::String("a".into()))
        .unwrap()
        .is_some());
    assert!(db
        .get(id, &typra_core::ScalarValue::String("b".into()))
        .unwrap()
        .is_some());
}

#[test]
fn nested_transaction_errors() {
    let mut db = Database::open_in_memory().unwrap();
    db.register_collection("books", vec![title_field()], "title")
        .unwrap();
    let err = db
        .transaction(|d| {
            d.begin_transaction()?;
            Ok(())
        })
        .unwrap_err();
    assert!(matches!(
        err,
        typra_core::DbError::Transaction(typra_core::TransactionError::NestedTransaction)
    ));
}

#[test]
fn open_strict_rejects_uncommitted_txn_tail_and_autotruncate_recovers() {
    use std::fs::OpenOptions as FsOpenOptions;
    use typra_core::segments::header::{SegmentHeader, SegmentType};
    use typra_core::segments::writer::SegmentWriter;
    use typra_core::storage::FileStore;
    use typra_core::superblock::SUPERBLOCK_SIZE;
    use typra_core::{DbError, FormatError};

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("unclean_txn.typra");
    {
        // Create a valid DB then append a BEGIN + invalid RECORD without COMMIT.
        let mut db = Database::open(&path).unwrap();
        db.register_collection("books", vec![title_field()], "title")
            .unwrap();
        drop(db);

        let file = FsOpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        let mut store = FileStore::new(file);
        let len = store.len().unwrap();
        let mut w = SegmentWriter::new(&mut store, len);
        let begin = typra_core::txn::encode_txn_payload_v0(42);
        w.append(
            SegmentHeader {
                segment_type: SegmentType::TxnBegin,
                payload_len: 0,
                payload_crc32c: 0,
            },
            begin.as_slice(),
        )
        .unwrap();
        // Minimal invalid record payload (too short for replay).
        w.append(
            SegmentHeader {
                segment_type: SegmentType::Record,
                payload_len: 0,
                payload_crc32c: 0,
            },
            &[1, 2, 3],
        )
        .unwrap();
        store.sync().unwrap();
    }

    // Strict mode refuses to mutate and errors with UncleanLogTail.
    let strict = OpenOptions {
        recovery: RecoveryMode::Strict,
        ..OpenOptions::default()
    };
    let e = match Database::open_with_options(&path, strict) {
        Ok(_) => panic!("expected strict open to fail"),
        Err(e) => e,
    };
    assert!(matches!(
        e,
        DbError::Format(FormatError::UncleanLogTail { .. })
    ));

    // AutoTruncate opens and truncates away the incomplete txn tail.
    let auto = OpenOptions {
        recovery: RecoveryMode::AutoTruncate,
        ..OpenOptions::default()
    };
    let _ = Database::open_with_options(&path, auto).unwrap();

    // Reopen again (default opts) should succeed; file should be clean.
    let _ = Database::open(&path).unwrap();

    let _ = SUPERBLOCK_SIZE; // keep import used (future assertions may use it)
}

#[test]
fn open_strict_rejects_torn_segment_tail_and_autotruncate_recovers() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("torn_seg.typra");
    {
        let mut db = Database::open(&path).unwrap();
        db.register_collection("books", vec![title_field()], "title")
            .unwrap();
    }
    // Tear the tail by truncating the file mid-segment header/payload.
    let bytes = std::fs::read(&path).unwrap();
    assert!(bytes.len() > 10);
    std::fs::write(&path, &bytes[..bytes.len() - 7]).unwrap();

    let strict = OpenOptions {
        recovery: RecoveryMode::Strict,
        ..OpenOptions::default()
    };
    let e = match Database::open_with_options(&path, strict) {
        Ok(_) => panic!("expected strict open to fail"),
        Err(e) => e,
    };
    // Strict open must refuse to proceed on a torn tail. The exact error may be either the strict
    // recovery error (preferred) or a lower-level format error if the tear violates invariants
    // before recovery logic can compute a safe prefix.
    assert!(matches!(
        e,
        typra_core::DbError::Format(_) | typra_core::DbError::Io(_)
    ));

    let auto = OpenOptions {
        recovery: RecoveryMode::AutoTruncate,
        ..OpenOptions::default()
    };
    let _ = Database::open_with_options(&path, auto).unwrap();
    let _ = Database::open(&path).unwrap();
}

#[test]
fn unique_index_violation_inside_transaction_rolls_back_all_writes() {
    let mut db = Database::open_in_memory().unwrap();
    let fields = vec![id_field(), sku_field()];
    let indexes = vec![IndexDef {
        name: "sku_u".to_string(),
        path: FieldPath(vec![Cow::Borrowed("sku")]),
        kind: IndexKind::Unique,
    }];
    let (cid, _) = db
        .register_collection_with_indexes("items", fields, indexes, "id")
        .unwrap();

    let res = db.transaction(|d| {
        let mut r1 = BTreeMap::new();
        r1.insert("id".into(), RowValue::Int64(1));
        r1.insert("sku".into(), RowValue::String("X".into()));
        d.insert(cid, r1)?;

        let mut r2 = BTreeMap::new();
        r2.insert("id".into(), RowValue::Int64(2));
        r2.insert("sku".into(), RowValue::String("X".into()));
        d.insert(cid, r2)?;
        Ok::<(), typra_core::DbError>(())
    });
    assert!(matches!(
        res,
        Err(typra_core::DbError::Schema(
            typra_core::SchemaError::UniqueIndexViolation
        ))
    ));

    // No rows should be committed.
    assert!(db
        .get(cid, &typra_core::ScalarValue::Int64(1))
        .unwrap()
        .is_none());
    assert!(db
        .get(cid, &typra_core::ScalarValue::Int64(2))
        .unwrap()
        .is_none());
}

#[test]
fn realistic_disk_workflow_transaction_indexed_query_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("realistic.typra");
    {
        let mut db = Database::open(&path).unwrap();
        let fields = vec![id_field(), sku_field(), qty_field(), status_field()];
        let indexes = vec![
            IndexDef {
                name: "sku_idx".to_string(),
                path: FieldPath(vec![Cow::Borrowed("sku")]),
                kind: IndexKind::NonUnique,
            },
            IndexDef {
                name: "status_idx".to_string(),
                path: FieldPath(vec![Cow::Borrowed("status")]),
                kind: IndexKind::NonUnique,
            },
        ];
        let (cid, _) = db
            .register_collection_with_indexes("order_lines", fields, indexes, "id")
            .unwrap();

        db.transaction(|d| {
            for (id, sku, qty, st) in [
                (1, "SKU-A", 2, "open"),
                (2, "SKU-B", 1, "shipped"),
                (3, "SKU-A", 4, "open"),
            ] {
                let mut row = BTreeMap::new();
                row.insert("id".into(), RowValue::Int64(id));
                row.insert("sku".into(), RowValue::String(sku.into()));
                row.insert("qty".into(), RowValue::Int64(qty));
                row.insert("status".into(), RowValue::String(st.into()));
                d.insert(cid, row)?;
            }
            Ok::<(), typra_core::DbError>(())
        })
        .unwrap();

        let q = Query {
            collection: cid,
            predicate: Some(Predicate::And(vec![
                Predicate::Eq {
                    path: FieldPath(vec![Cow::Borrowed("status")]),
                    value: typra_core::ScalarValue::String("open".into()),
                },
                Predicate::Eq {
                    path: FieldPath(vec![Cow::Borrowed("sku")]),
                    value: typra_core::ScalarValue::String("SKU-A".into()),
                },
            ])),
            limit: Some(10),
            order_by: None,
        };
        let explain = db.explain_query(&q).unwrap();
        assert!(explain.contains("IndexLookup"));
        let rows = db.query(&q).unwrap();
        assert_eq!(rows.len(), 2);
    }

    // Reopen and verify one row.
    let db2 = Database::open(&path).unwrap();
    let got = db2
        .get(CollectionId(1), &typra_core::ScalarValue::Int64(1))
        .unwrap()
        .expect("row");
    assert_eq!(
        got.get("qty"),
        Some(&RowValue::Int64(2)),
        "reopened row has expected qty"
    );
}
