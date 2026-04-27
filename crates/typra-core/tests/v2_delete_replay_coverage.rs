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
fn v2_delete_payload_is_replayed_on_reopen() {
    let mut db = Database::open_in_memory().unwrap();
    let (cid, _) = db
        .register_collection(
            "t",
            vec![def(&["id"], Type::Int64), def(&["x"], Type::Int64)],
            "id",
        )
        .unwrap();

    let mut row = BTreeMap::new();
    row.insert("id".to_string(), RowValue::from_scalar(ScalarValue::Int64(1)));
    row.insert("x".to_string(), RowValue::from_scalar(ScalarValue::Int64(2)));
    db.insert(cid, row).unwrap();

    db.delete(cid, &ScalarValue::Int64(1)).unwrap();

    let snap = db.into_snapshot_bytes();
    let reopened = Database::from_snapshot_bytes(snap).unwrap();
    assert_eq!(reopened.get(cid, &ScalarValue::Int64(1)).unwrap(), None);
}

