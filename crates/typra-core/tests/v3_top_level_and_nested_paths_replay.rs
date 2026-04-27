use std::borrow::Cow;
use std::collections::BTreeMap;

use typra_core::{Database, FieldDef, RowValue, ScalarValue, Type};
use typra_core::schema::FieldPath;

fn def(path: &[&'static str], ty: Type) -> FieldDef {
    FieldDef {
        path: FieldPath(path.iter().map(|s| Cow::Borrowed(*s)).collect()),
        ty,
        constraints: Vec::new(),
    }
}

#[test]
fn v3_replay_handles_top_level_and_nested_field_paths() {
    // Force v3 by including at least one multi-segment FieldPath, but also include
    // a top-level non-PK field so replay exercises both path-len==1 and path-len>1.
    let mut db = Database::open_in_memory().unwrap();
    let (cid, _) = db
        .register_collection(
            "t",
            vec![
                def(&["id"], Type::Int64),
                def(&["x"], Type::Int64),
                def(&["obj", "y"], Type::Int64),
            ],
            "id",
        )
        .unwrap();

    let mut row = BTreeMap::new();
    row.insert("id".to_string(), RowValue::from_scalar(ScalarValue::Int64(1)));
    row.insert("x".to_string(), RowValue::from_scalar(ScalarValue::Int64(10)));
    let mut obj = BTreeMap::new();
    obj.insert("y".to_string(), RowValue::from_scalar(ScalarValue::Int64(20)));
    row.insert("obj".to_string(), RowValue::Object(obj));
    db.insert(cid, row).unwrap();

    let snap = db.into_snapshot_bytes();
    let reopened = Database::from_snapshot_bytes(snap).unwrap();

    let got = reopened.get(cid, &ScalarValue::Int64(1)).unwrap().unwrap();
    assert!(got.contains_key("x"));
    assert!(got.contains_key("obj"));
}

