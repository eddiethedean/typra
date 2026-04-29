use std::borrow::Cow;
use std::collections::BTreeMap;

use typra_core::validation::{validate_top_level_row, validate_value};
use typra_core::{Constraint, DbError, FieldDef, RowValue, Type};
use typra_core::schema::FieldPath;

fn def(name: &'static str, ty: Type, constraints: Vec<Constraint>) -> FieldDef {
    FieldDef {
        path: FieldPath(vec![Cow::Borrowed(name)]),
        ty,
        constraints,
    }
}

#[test]
fn validate_value_accepts_uint64_float64_bytes_uuid_and_timestamp() {
    let mut path = vec!["x".to_string()];
    validate_value(&mut path, &Type::Uint64, &[], &RowValue::Uint64(1)).unwrap();

    let mut path = vec!["x".to_string()];
    validate_value(&mut path, &Type::Float64, &[], &RowValue::Float64(1.25)).unwrap();

    let mut path = vec!["x".to_string()];
    validate_value(&mut path, &Type::Bytes, &[], &RowValue::Bytes(vec![1, 2, 3])).unwrap();

    let mut path = vec!["x".to_string()];
    validate_value(
        &mut path,
        &Type::Uuid,
        &[],
        &RowValue::Uuid([0u8; 16]),
    )
    .unwrap();

    let mut path = vec!["x".to_string()];
    validate_value(
        &mut path,
        &Type::Timestamp,
        &[],
        &RowValue::Timestamp(0),
    )
    .unwrap();
}

#[test]
fn validate_value_rejects_wrong_types_for_uint64_float64_and_bytes() {
    let mut path = vec!["x".to_string()];
    assert!(validate_value(&mut path, &Type::Uint64, &[], &RowValue::Int64(1)).is_err());

    let mut path = vec!["x".to_string()];
    assert!(validate_value(&mut path, &Type::Float64, &[], &RowValue::Int64(1)).is_err());

    let mut path = vec!["x".to_string()];
    assert!(validate_value(&mut path, &Type::Bytes, &[], &RowValue::String("x".to_string())).is_err());
}

#[test]
fn object_optional_field_absent_is_ok_but_required_absent_errors() {
    let schema = Type::Object(vec![
        FieldDef {
            path: FieldPath(vec![Cow::Borrowed("opt")]),
            ty: Type::Optional(Box::new(Type::Int64)),
            constraints: vec![],
        },
        FieldDef {
            path: FieldPath(vec![Cow::Borrowed("req")]),
            ty: Type::Int64,
            constraints: vec![],
        },
    ]);

    // Optional absent is OK; required present.
    let mut m = BTreeMap::new();
    m.insert("req".to_string(), RowValue::Int64(1));
    let mut path = vec!["root".to_string()];
    validate_value(&mut path, &schema, &[], &RowValue::Object(m)).unwrap();

    // Required absent is error.
    let mut m = BTreeMap::new();
    m.insert("opt".to_string(), RowValue::None);
    let mut path = vec!["root".to_string()];
    assert!(validate_value(&mut path, &schema, &[], &RowValue::Object(m)).is_err());
}

#[test]
fn numeric_constraints_have_passing_and_failing_examples() {
    // Passing cases exercise the non-error branch outcomes.
    let mut path = vec!["x".to_string()];
    validate_value(
        &mut path,
        &Type::Int64,
        &[Constraint::MaxI64(10)],
        &RowValue::Int64(10),
    )
    .unwrap();

    let mut path = vec!["x".to_string()];
    validate_value(
        &mut path,
        &Type::Uint64,
        &[Constraint::MinU64(10)],
        &RowValue::Uint64(10),
    )
    .unwrap();

    let mut path = vec!["x".to_string()];
    validate_value(
        &mut path,
        &Type::Uint64,
        &[Constraint::MaxU64(10)],
        &RowValue::Uint64(10),
    )
    .unwrap();

    let mut path = vec!["x".to_string()];
    validate_value(
        &mut path,
        &Type::Float64,
        &[Constraint::MinF64(1.0)],
        &RowValue::Float64(1.0),
    )
    .unwrap();

    let mut path = vec!["x".to_string()];
    validate_value(
        &mut path,
        &Type::Float64,
        &[Constraint::MaxF64(1.0)],
        &RowValue::Float64(1.0),
    )
    .unwrap();

    // Failing cases.
    let mut path = vec!["x".to_string()];
    assert!(validate_value(
        &mut path,
        &Type::Int64,
        &[Constraint::MaxI64(0)],
        &RowValue::Int64(1)
    )
    .is_err());
}

#[test]
fn email_and_url_constraints_cover_pass_and_fail() {
    let mut path = vec!["x".to_string()];
    validate_value(
        &mut path,
        &Type::String,
        &[Constraint::Email],
        &RowValue::String("a@b.com".to_string()),
    )
    .unwrap();

    let mut path = vec!["x".to_string()];
    assert!(validate_value(
        &mut path,
        &Type::String,
        &[Constraint::Email],
        &RowValue::String("a@b".to_string())
    )
    .is_err());

    let mut path = vec!["x".to_string()];
    validate_value(
        &mut path,
        &Type::String,
        &[Constraint::Url],
        &RowValue::String("http://example.com".to_string()),
    )
    .unwrap();

    let mut path = vec!["x".to_string()];
    validate_value(
        &mut path,
        &Type::String,
        &[Constraint::Url],
        &RowValue::String("https://example.com".to_string()),
    )
    .unwrap();

    let mut path = vec!["x".to_string()];
    assert!(validate_value(
        &mut path,
        &Type::String,
        &[Constraint::Url],
        &RowValue::String("ftp://example.com".to_string())
    )
    .is_err());
}

#[test]
fn validate_top_level_row_accepts_known_fields_and_rejects_unknown() {
    let fields = vec![
        def("id", Type::Int64, vec![]),
        def("x", Type::Int64, vec![]),
    ];

    // Only known keys: should pass the unknown-field scan.
    let mut row = BTreeMap::new();
    row.insert("id".to_string(), RowValue::Int64(1));
    row.insert("x".to_string(), RowValue::Int64(2));
    validate_top_level_row(&fields, "id", &row).unwrap();

    // Add an unknown key: should error.
    row.insert("nope".to_string(), RowValue::Int64(3));
    let e = validate_top_level_row(&fields, "id", &row).unwrap_err();
    assert!(matches!(e, DbError::Validation(_)));
}

#[test]
fn regex_constraint_rejects_invalid_schema_regex() {
    let mut path = vec!["x".to_string()];
    let e = validate_value(
        &mut path,
        &Type::String,
        &[Constraint::Regex("[".to_string())],
        &RowValue::String("ok".to_string()),
    )
    .unwrap_err();
    assert!(matches!(e, DbError::Validation(_)));
}

#[test]
fn validate_top_level_row_errors_on_missing_required_field() {
    let fields = vec![def("id", Type::Int64, vec![]), def("x", Type::Int64, vec![])];
    let mut row = BTreeMap::new();
    row.insert("id".to_string(), RowValue::Int64(1));
    assert!(validate_top_level_row(&fields, "id", &row).is_err());
}

#[test]
fn min_length_constraint_can_fail() {
    let mut path = vec!["x".to_string()];
    assert!(validate_value(
        &mut path,
        &Type::String,
        &[Constraint::MinLength(3)],
        &RowValue::String("hi".to_string())
    )
    .is_err());
}

#[test]
fn min_length_constraint_can_pass() {
    let mut path = vec!["x".to_string()];
    validate_value(
        &mut path,
        &Type::String,
        &[Constraint::MinLength(2)],
        &RowValue::String("hi".to_string()),
    )
    .unwrap();
}

