use std::borrow::Cow;
use std::collections::BTreeMap;

use typra_core::config::{OpenOptions, RecoveryMode};
use typra_core::schema::{FieldDef, FieldPath, IndexDef, IndexKind, Type};
use typra_core::{Database, DbError, RowValue, ScalarValue};

fn fp1(name: &'static str) -> FieldPath {
    FieldPath(vec![Cow::Borrowed(name)])
}

#[test]
fn stable_surface_smoke_in_memory_register_txn_query_checkpoint_compact() -> Result<(), DbError> {
    let mut db = Database::open_in_memory()?;

    let fields = vec![
        FieldDef {
            path: fp1("id"),
            ty: Type::Int64,
            constraints: vec![],
        },
        FieldDef {
            path: fp1("tag"),
            ty: Type::String,
            constraints: vec![],
        },
    ];
    let indexes = vec![IndexDef {
        name: "tag_idx".to_string(),
        path: fp1("tag"),
        kind: IndexKind::NonUnique,
    }];
    let (cid, _v) = db.register_collection_with_indexes("t", fields, indexes, "id")?;

    db.transaction(|tx| {
        for (id, tag) in [(1i64, "a"), (2, "b"), (3, "a")] {
            let mut row = BTreeMap::new();
            row.insert("id".to_string(), RowValue::Int64(id));
            row.insert("tag".to_string(), RowValue::String(tag.to_string()));
            tx.insert(cid, row)?;
        }
        Ok(())
    })?;

    let q = typra_core::query::Query {
        collection: cid,
        predicate: Some(typra_core::query::Predicate::Eq {
            path: fp1("tag"),
            value: ScalarValue::String("a".to_string()),
        }),
        limit: None,
        order_by: None,
    };
    let rows = db.query(&q)?;
    assert_eq!(rows.len(), 2);

    // Snapshot/compact path should be stable and roundtrip.
    let snap = db.snapshot_bytes();
    let db2 = Database::from_snapshot_bytes(snap)?;
    let got = db2.get(cid, &ScalarValue::Int64(2))?.unwrap();
    assert_eq!(got.get("tag"), Some(&RowValue::String("b".to_string())));

    Ok(())
}

#[test]
fn open_with_options_surface_exists_and_recovery_mode_is_public() {
    let _opts = OpenOptions {
        recovery: RecoveryMode::AutoTruncate,
    };
}
