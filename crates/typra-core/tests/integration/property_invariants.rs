use std::borrow::Cow;
use std::collections::BTreeMap;

use proptest::prelude::*;

use typra_core::schema::{FieldDef, FieldPath, Type};
use typra_core::{Database, RowValue, ScalarValue};

fn fp1(name: &'static str) -> FieldPath {
    FieldPath(vec![Cow::Borrowed(name)])
}

fn book_fields() -> Vec<FieldDef> {
    vec![
        FieldDef {
            path: fp1("id"),
            ty: Type::Int64,
            constraints: vec![],
        },
        FieldDef {
            path: fp1("x"),
            ty: Type::Int64,
            constraints: vec![],
        },
    ]
}

fn row(id: i64, x: i64) -> BTreeMap<String, RowValue> {
    BTreeMap::from([
        ("id".to_string(), RowValue::Int64(id)),
        ("x".to_string(), RowValue::Int64(x)),
    ])
}

proptest! {
    // Basic safety invariant: snapshot roundtrip preserves visible state.
    #[test]
    fn snapshot_roundtrip_preserves_rows(ops in proptest::collection::vec((any::<i64>(), any::<i64>()), 0..200)) {
        let mut db = Database::open_in_memory().unwrap();
        let (cid, _) = db.register_collection("books", book_fields(), "id").unwrap();
        for (id, x) in &ops {
            db.insert(cid, row(*id, *x)).unwrap();
        }

        let snap = db.snapshot_bytes();
        let db2 = Database::from_snapshot_bytes(snap).unwrap();

        for (id, x) in &ops {
            let got = db2.get(cid, &ScalarValue::Int64(*id)).unwrap();
            prop_assert_eq!(got, Some(row(*id, *x)));
        }
    }

    // Basic hardening invariant: SQL parsing should never panic on arbitrary input.
    #[test]
    fn sql_parse_select_never_panics(s in ".*") {
        let _ = typra_core::sql::parse_select(&s);
    }
}
