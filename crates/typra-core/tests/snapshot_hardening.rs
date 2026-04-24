use std::borrow::Cow;
use std::collections::BTreeMap;

use typra_core::schema::{FieldDef, FieldPath, Type};
use typra_core::{Database, RowValue};

fn field(name: &'static str, ty: Type) -> FieldDef {
    FieldDef {
        path: FieldPath(vec![Cow::Borrowed(name)]),
        ty,
        constraints: vec![],
    }
}

#[test]
fn from_snapshot_bytes_never_panics_on_mutations_and_returns_error() {
    let mut db = Database::open_in_memory().unwrap();
    let fields = vec![field("id", Type::String), field("v", Type::Int64)];
    let (cid, _) = db.register_collection("t", fields, "id").unwrap();
    for i in 0..50i64 {
        let mut row = BTreeMap::new();
        row.insert("id".to_string(), RowValue::String(format!("k{i}")));
        row.insert("v".to_string(), RowValue::Int64(i));
        db.insert(cid, row).unwrap();
    }

    let base = db.snapshot_bytes();
    assert!(!base.is_empty());

    // Empty snapshot bytes are treated as a new empty DB image (see existing API contract).
    let res = std::panic::catch_unwind(|| Database::from_snapshot_bytes(Vec::new()));
    match res {
        Ok(Ok(db)) => assert!(db.catalog().is_empty()),
        Ok(Err(e)) => panic!("empty snapshot unexpectedly errored: {e:?}"),
        Err(_) => panic!("empty snapshot panicked"),
    }

    let mut cases: Vec<(Vec<u8>, bool)> = Vec::new();
    // Very short/truncated buffers should not panic (may be treated as empty by future changes).
    cases.push((base[..1].to_vec(), false));
    cases.push((base[..(base.len() / 3).max(1)].to_vec(), false));
    // Corrupt the file header magic: should deterministically error.
    cases.push({
        let mut b = base.clone();
        b[0] ^= 0xff;
        (b, true)
    });
    // Corrupt many bytes: should almost certainly error (and must never panic).
    cases.push({
        let mut b = base.clone();
        for i in (0..b.len()).step_by((b.len() / 16).max(1)).take(32) {
            b[i] ^= 0xff;
        }
        (b, false)
    });

    for (i, (bytes, must_err)) in cases.into_iter().enumerate() {
        let res = std::panic::catch_unwind(|| Database::from_snapshot_bytes(bytes));
        match res {
            Ok(Ok(_)) if must_err => panic!("case {i}: unexpectedly opened snapshot"),
            Ok(Ok(_)) => {}
            Ok(Err(_)) => {}
            Err(_) => panic!("case {i}: panicked"),
        }
    }
}
