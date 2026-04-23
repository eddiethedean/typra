//! Multi-write transactions and nested-transaction errors.

use std::borrow::Cow;
use std::collections::BTreeMap;

use typra_core::record::RowValue;
use typra_core::schema::{FieldDef, FieldPath, Type};
use typra_core::storage::Store;
use typra_core::{CollectionId, Database, OpenOptions, RecoveryMode};

fn title_field() -> FieldDef {
    FieldDef {
        path: FieldPath(vec![Cow::Owned("title".to_string())]),
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
    assert!(db.get(id, &typra_core::ScalarValue::String("a".into())).unwrap().is_some());
    assert!(db.get(id, &typra_core::ScalarValue::String("b".into())).unwrap().is_some());
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
    use typra_core::segments::header::{SegmentHeader, SegmentType};
    use typra_core::segments::writer::SegmentWriter;
    use typra_core::storage::FileStore;
    use typra_core::superblock::SUPERBLOCK_SIZE;
    use typra_core::{DbError, FormatError};
    use std::fs::OpenOptions as FsOpenOptions;

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
    };
    let _ = Database::open_with_options(&path, auto).unwrap();

    // Reopen again (default opts) should succeed; file should be clean.
    let _ = Database::open(&path).unwrap();

    let _ = SUPERBLOCK_SIZE; // keep import used (future assertions may use it)
}
