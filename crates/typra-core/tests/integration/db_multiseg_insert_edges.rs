use std::borrow::Cow;
use std::collections::BTreeMap;

use typra_core::error::{DbError, SchemaError};
use typra_core::schema::{FieldDef, FieldPath, Type};
use typra_core::{Database, RowValue, ScalarValue};

fn obj(pairs: impl IntoIterator<Item = (&'static str, RowValue)>) -> RowValue {
    RowValue::Object(pairs.into_iter().map(|(k, v)| (k.to_string(), v)).collect())
}

#[test]
fn insert_multisegment_schema_rejects_unknown_leaf_path() {
    let mut db = Database::open_in_memory().unwrap();

    let fields = vec![
        FieldDef::new(FieldPath::new([Cow::Borrowed("id")]).unwrap(), Type::String),
        FieldDef::new(
            FieldPath::new([Cow::Borrowed("profile"), Cow::Borrowed("tz")]).unwrap(),
            Type::String,
        ),
    ];
    let (cid, _ver) = db.register_collection("users", fields, "id").unwrap();

    let mut row = BTreeMap::new();
    row.insert("id".into(), RowValue::String("u1".into()));
    row.insert(
        "profile".into(),
        obj([
            ("tz", RowValue::String("UTC".into())),
            ("bogus", RowValue::Int64(1)),
        ]),
    );

    let err = db.insert(cid, row).unwrap_err();
    assert!(matches!(
        err,
        DbError::Schema(SchemaError::RowUnknownField { .. })
    ));
}

#[test]
fn insert_multisegment_schema_missing_required_nested_field_errors() {
    let mut db = Database::open_in_memory().unwrap();

    let fields = vec![
        FieldDef::new(FieldPath::new([Cow::Borrowed("id")]).unwrap(), Type::String),
        FieldDef::new(
            FieldPath::new([Cow::Borrowed("profile"), Cow::Borrowed("tz")]).unwrap(),
            Type::String,
        ),
    ];
    let (cid, _ver) = db.register_collection("users2", fields, "id").unwrap();

    // Omit `profile` entirely; required nested leaf should become RowMissingField "profile.tz".
    let mut row = BTreeMap::new();
    row.insert("id".into(), RowValue::String("u1".into()));

    let err = db.insert(cid, row).unwrap_err();
    assert!(matches!(
        err,
        DbError::Schema(SchemaError::RowMissingField { name }) if name == "profile.tz"
    ));
}

#[test]
fn insert_multisegment_schema_absent_optional_nested_field_is_allowed() {
    let mut db = Database::open_in_memory().unwrap();

    let fields = vec![
        FieldDef::new(FieldPath::new([Cow::Borrowed("id")]).unwrap(), Type::String),
        FieldDef::new(
            FieldPath::new([Cow::Borrowed("profile"), Cow::Borrowed("tz")]).unwrap(),
            Type::Optional(Box::new(Type::String)),
        ),
    ];
    let (cid, _ver) = db.register_collection("users3", fields, "id").unwrap();

    let mut row = BTreeMap::new();
    row.insert("id".into(), RowValue::String("u1".into()));
    // omit profile entirely -> leaf is absent; optional should be accepted
    db.insert(cid, row).unwrap();

    let got = db
        .get(cid, &ScalarValue::String("u1".into()))
        .unwrap()
        .unwrap();
    let Some(RowValue::Object(profile)) = got.get("profile") else {
        panic!("expected `profile` to be materialized as an object");
    };
    assert!(matches!(profile.get("tz"), Some(RowValue::None)));
}
