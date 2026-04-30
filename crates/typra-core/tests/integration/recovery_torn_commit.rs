use std::borrow::Cow;
use std::collections::BTreeMap;

use std::io::Seek;
use tempfile::tempdir;
use typra_core::config::{OpenOptions, RecoveryMode};
use typra_core::error::{DbError, FormatError};
use typra_core::schema::{FieldDef, FieldPath, Type};
use typra_core::{Database, RowValue, ScalarValue};

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
    let path = dir.path().join("t.typra");

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
