//! Unit tests for [`typra_core::validation`] (types, constraints, top-level row rules).

use std::borrow::Cow;
use std::collections::BTreeMap;

use typra_core::error::DbError;
use typra_core::record::RowValue;
use typra_core::schema::{Constraint, FieldDef, FieldPath, Type};
use typra_core::validation::{ensure_pk_type_primitive, validate_top_level_row, validate_value};

fn path_seg(s: &str) -> FieldPath {
    FieldPath::new([Cow::Owned(s.to_string())]).unwrap()
}

#[test]
fn ensure_pk_rejects_optional() {
    let e = ensure_pk_type_primitive(&Type::Optional(Box::new(Type::String))).unwrap_err();
    assert!(matches!(e, DbError::Validation(_)));
}

#[test]
fn validate_int64_min_constraint() {
    let mut p = vec!["n".into()];
    let e = validate_value(
        &mut p,
        &Type::Int64,
        &[Constraint::MinI64(10)],
        &RowValue::Int64(3),
    )
    .unwrap_err();
    match e {
        DbError::Validation(v) => {
            assert_eq!(v.path, vec!["n".to_string()]);
            assert!(v.message.contains("below minimum"));
        }
        _ => panic!("expected Validation"),
    }
}

#[test]
fn validate_string_regex_constraint_ok_and_fail() {
    let mut p = vec!["s".into()];
    validate_value(
        &mut p,
        &Type::String,
        &[Constraint::Regex("^[a-z]+$".into())],
        &RowValue::String("abc".into()),
    )
    .unwrap();

    let e = validate_value(
        &mut p,
        &Type::String,
        &[Constraint::Regex("^[a-z]+$".into())],
        &RowValue::String("A".into()),
    )
    .unwrap_err();
    assert!(matches!(e, DbError::Validation(_)));
}

#[test]
fn validate_object_rejects_unknown_key() {
    let fields = vec![FieldDef {
        path: path_seg("x"),
        ty: Type::String,
        constraints: vec![],
    }];
    let ty = Type::Object(fields);
    let mut m = BTreeMap::new();
    m.insert("x".into(), RowValue::String("a".into()));
    m.insert("oops".into(), RowValue::String("b".into()));
    let mut p = vec!["obj".into()];
    let e = validate_value(&mut p, &ty, &[], &RowValue::Object(m)).unwrap_err();
    match e {
        DbError::Validation(v) => {
            assert!(v.path.iter().any(|s| s == "oops"));
            assert!(v.message.contains("unknown field"));
        }
        _ => panic!("expected Validation"),
    }
}

#[test]
fn validate_enum_rejects_bad_variant() {
    let mut p = vec!["e".into()];
    let e = validate_value(
        &mut p,
        &Type::Enum(vec!["a".into(), "b".into()]),
        &[],
        &RowValue::String("c".into()),
    )
    .unwrap_err();
    assert!(matches!(e, DbError::Validation(_)));
}

#[test]
fn validate_object_missing_field_path() {
    let fields = vec![FieldDef {
        path: path_seg("x"),
        ty: Type::String,
        constraints: vec![],
    }];
    let ty = Type::Object(fields);
    let mut m = BTreeMap::new();
    // missing x
    m.insert("y".into(), RowValue::String("nope".into()));
    let mut p = vec!["obj".into()];
    let e = validate_value(&mut p, &ty, &[], &RowValue::Object(m)).unwrap_err();
    match e {
        DbError::Validation(v) => {
            assert!(v.path.contains(&"x".to_string()));
            assert!(v.message.contains("missing"));
        }
        _ => panic!("expected Validation"),
    }
}

#[test]
fn validate_constraint_type_mismatch_errors() {
    let mut p = vec!["x".into()];
    // MinI64 on a string hits the "requires int64" branch.
    let e = validate_value(
        &mut p,
        &Type::String,
        &[Constraint::MinI64(0)],
        &RowValue::String("s".into()),
    )
    .unwrap_err();
    assert!(matches!(e, DbError::Validation(v) if v.message.contains("requires int64")));

    // NonEmpty on an object hits the "applies to string, bytes, or list" branch.
    let e = validate_value(
        &mut p,
        &Type::Object(vec![]),
        &[Constraint::NonEmpty],
        &RowValue::Object(BTreeMap::new()),
    )
    .unwrap_err();
    assert!(matches!(e, DbError::Validation(v) if v.message.contains("NonEmpty")));
}

#[test]
fn validate_top_level_unknown_field() {
    let defs = vec![
        FieldDef {
            path: path_seg("id"),
            ty: Type::String,
            constraints: vec![],
        },
        FieldDef {
            path: path_seg("y"),
            ty: Type::Int64,
            constraints: vec![],
        },
    ];
    let mut row = BTreeMap::new();
    row.insert("id".into(), RowValue::String("k".into()));
    row.insert("y".into(), RowValue::Int64(1));
    row.insert("extra".into(), RowValue::Bool(true));
    let e = validate_top_level_row(&defs, "id", &row).unwrap_err();
    assert!(matches!(e, DbError::Validation(_)));
}

#[test]
fn validate_top_level_optional_omitted_is_ok() {
    let defs = vec![
        FieldDef {
            path: path_seg("id"),
            ty: Type::String,
            constraints: vec![],
        },
        FieldDef {
            path: path_seg("opt"),
            ty: Type::Optional(Box::new(Type::String)),
            constraints: vec![],
        },
    ];
    let mut row = BTreeMap::new();
    row.insert("id".into(), RowValue::String("k".into()));
    validate_top_level_row(&defs, "id", &row).unwrap();
}

#[test]
fn validate_top_level_required_null_rejected() {
    let defs = vec![
        FieldDef {
            path: path_seg("id"),
            ty: Type::String,
            constraints: vec![],
        },
        FieldDef {
            path: path_seg("y"),
            ty: Type::Int64,
            constraints: vec![],
        },
    ];
    let mut row = BTreeMap::new();
    row.insert("id".into(), RowValue::String("k".into()));
    row.insert("y".into(), RowValue::None);
    let e = validate_top_level_row(&defs, "id", &row).unwrap_err();
    assert!(matches!(e, DbError::Validation(_)));
}

#[test]
fn validate_list_element_path_in_error() {
    let mut p = vec!["items".into()];
    let e = validate_value(
        &mut p,
        &Type::List(Box::new(Type::Int64)),
        &[],
        &RowValue::List(vec![RowValue::Int64(1), RowValue::String("bad".into())]),
    )
    .unwrap_err();
    match e {
        DbError::Validation(v) => {
            assert!(
                v.path == vec!["items".to_string(), "1".to_string()],
                "path={:?}",
                v.path
            );
        }
        _ => panic!("expected Validation"),
    }
}
