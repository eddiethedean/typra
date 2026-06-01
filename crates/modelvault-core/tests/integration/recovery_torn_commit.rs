use std::borrow::Cow;
use std::collections::BTreeMap;

use modelvault_core::config::{OpenOptions, RecoveryMode};
use modelvault_core::error::{DbError, FormatError};
use modelvault_core::schema::{FieldDef, FieldPath, Type};
use modelvault_core::{Database, RowValue, ScalarValue};
use std::io::Seek;
use tempfile::tempdir;

fn field(name: &'static str, ty: Type) -> FieldDef {
    FieldDef {
        path: FieldPath(vec![Cow::Borrowed(name)]),
        ty,
        constraints: vec![],
    }
}

#[test]
fn strict_rejects_trailing_garbage_and_autotruncate_recovers() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("t.modelvault");

    // Create a db and commit at least one txn.
    {
        let mut db = Database::open(&path).unwrap();
        let fields = vec![field("id", Type::String), field("v", Type::Int64)];
        let (cid, _) = db.register_collection("t", fields, "id").unwrap();
        db.transaction(|tdb| {
            let mut row = BTreeMap::new();
            row.insert("id".to_string(), RowValue::String("k1".to_string()));
            row.insert("v".to_string(), RowValue::Int64(1));
            tdb.insert(cid, row)?;
            Ok(())
        })
        .unwrap();
    }

    // Append trailing bytes to simulate a torn/partial segment header write.
    {
        use std::io::Write;
        let mut f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(&path)
            .unwrap();
        f.seek(std::io::SeekFrom::End(0)).unwrap();
        f.write_all(b"garbage").unwrap();
        f.sync_all().unwrap();
    }

    // Strict should fail.
    let strict = Database::open_with_options(
        &path,
        OpenOptions {
            recovery: RecoveryMode::Strict,
            ..OpenOptions::default()
        },
    );
    assert!(strict.is_err());

    // Read-only open should also fail (it uses Strict recovery).
    let ro = Database::open_read_only(&path);
    assert!(matches!(
        ro,
        Err(DbError::Format(FormatError::UncleanLogTail { .. }))
    ));

    // AutoTruncate should open and preserve committed state.
    let db = Database::open_with_options(
        &path,
        OpenOptions {
            recovery: RecoveryMode::AutoTruncate,
            ..OpenOptions::default()
        },
    )
    .unwrap();
    let cid = db.collection_id_named("t").unwrap();
    let got = db.get(cid, &ScalarValue::String("k1".to_string())).unwrap();
    assert!(got.is_some());
}

#[test]
fn autotruncate_recovers_from_orphan_txn_commit() {
    use modelvault_core::segments::header::{SegmentHeader, SegmentType};
    use modelvault_core::segments::writer::SegmentWriter;
    use modelvault_core::storage::{FileStore, Store};
    use modelvault_core::txn::encode_txn_payload_v0;
    use std::fs::OpenOptions as FsOpenOptions;

    let dir = tempdir().unwrap();
    let path = dir.path().join("orphan.modelvault");

    {
        let mut db = Database::open(&path).unwrap();
        let fields = vec![field("id", Type::String), field("v", Type::Int64)];
        let (cid, _) = db.register_collection("t", fields, "id").unwrap();
        db.transaction(|tdb| {
            let mut row = BTreeMap::new();
            row.insert("id".to_string(), RowValue::String("k1".to_string()));
            row.insert("v".to_string(), RowValue::Int64(1));
            tdb.insert(cid, row)?;
            Ok(())
        })
        .unwrap();
    }

    let len_before = std::fs::metadata(&path).unwrap().len();
    {
        let file = FsOpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        let mut store = FileStore::new(file);
        let len = store.len().unwrap();
        let mut w = SegmentWriter::new(&mut store, len);
        let payload = encode_txn_payload_v0(99);
        w.append(
            SegmentHeader {
                segment_type: SegmentType::TxnCommit,
                payload_len: 0,
                payload_crc32c: 0,
            },
            &payload,
        )
        .unwrap();
        store.sync().unwrap();
    }
    assert!(std::fs::metadata(&path).unwrap().len() > len_before);

    let strict = Database::open_with_options(
        &path,
        OpenOptions {
            recovery: RecoveryMode::Strict,
            ..OpenOptions::default()
        },
    );
    assert!(strict.is_err());

    let db = Database::open_with_options(
        &path,
        OpenOptions {
            recovery: RecoveryMode::AutoTruncate,
            ..OpenOptions::default()
        },
    )
    .unwrap();
    let cid = db.collection_id_named("t").unwrap();
    let got = db.get(cid, &ScalarValue::String("k1".to_string())).unwrap();
    assert!(got.is_some());
}
