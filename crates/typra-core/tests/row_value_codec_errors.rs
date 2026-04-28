//! Cover record v2 row-value codec error branches.

use std::borrow::Cow;
use std::collections::BTreeMap;

use typra_core::error::{DbError, FormatError};
use typra_core::record::{
    decode_record_payload, encode_record_payload_v2, encode_row_value, RowValue, ScalarValue,
};
use typra_core::schema::{FieldDef, FieldPath, Type};

fn seg(s: &str) -> FieldPath {
    FieldPath::new([Cow::Owned(s.to_string())]).unwrap()
}

#[test]
fn encode_row_value_object_missing_field_errors() {
    let mut out = Vec::new();
    let ty = Type::Object(vec![FieldDef {
        path: seg("x"),
        ty: Type::String,
        constraints: vec![],
    }]);
    let map = BTreeMap::<String, RowValue>::new();
    let e = encode_row_value(&mut out, &RowValue::Object(map), &ty).unwrap_err();
    assert!(matches!(
        e,
        DbError::Format(FormatError::TruncatedRecordPayload)
    ));
}

#[test]
fn encode_row_value_enum_requires_string() {
    let mut out = Vec::new();
    let e =
        encode_row_value(&mut out, &RowValue::Int64(1), &Type::Enum(vec!["a".into()])).unwrap_err();
    assert!(matches!(
        e,
        DbError::Format(FormatError::RecordPayloadTypeMismatch)
    ));
}

#[test]
fn encode_row_value_list_requires_list() {
    let mut out = Vec::new();
    let e = encode_row_value(
        &mut out,
        &RowValue::Int64(1),
        &Type::List(Box::new(Type::Int64)),
    )
    .unwrap_err();
    assert!(matches!(
        e,
        DbError::Format(FormatError::RecordPayloadTypeMismatch)
    ));
}

#[test]
fn encode_row_value_primitive_requires_primitive() {
    let mut out = Vec::new();
    let e = encode_row_value(&mut out, &RowValue::List(vec![]), &Type::String).unwrap_err();
    assert!(matches!(
        e,
        DbError::Format(FormatError::RecordPayloadTypeMismatch)
    ));
}

#[test]
fn decode_record_payload_v2_rejects_optional_presence_not_0_or_1() {
    let fields = vec![
        FieldDef {
            path: seg("id"),
            ty: Type::String,
            constraints: vec![],
        },
        FieldDef {
            path: seg("opt"),
            ty: Type::Optional(Box::new(Type::String)),
            constraints: vec![],
        },
    ];
    let pk = ScalarValue::String("k".into());
    let pk_ty = &fields[0].ty;
    let non_pk = vec![(fields[1].clone(), RowValue::None)];
    let mut payload = encode_record_payload_v2(1, 1, &pk, pk_ty, &non_pk).unwrap();

    // Patch presence from 0 -> 2 (same strategy as other tests).
    let idx = payload.iter().rposition(|b| *b == 0).unwrap();
    payload[idx] = 2;

    let e = decode_record_payload(&payload, "id", pk_ty, &fields).unwrap_err();
    assert!(matches!(
        e,
        DbError::Format(FormatError::RecordPayloadTypeMismatch)
    ));
}

#[test]
fn row_value_scalar_roundtrips_all_variants_and_into_scalar_errors_for_non_scalars() {
    let vals = vec![
        ScalarValue::Bool(true),
        ScalarValue::Int64(-1),
        ScalarValue::Uint64(2),
        ScalarValue::Float64(1.5),
        ScalarValue::String("x".into()),
        ScalarValue::Bytes(vec![1, 2]),
        ScalarValue::Uuid([9u8; 16]),
        ScalarValue::Timestamp(7),
    ];
    for s in vals {
        let rv = RowValue::from_scalar(s.clone());
        assert_eq!(rv.as_scalar(), Some(s.clone()));
        assert_eq!(rv.clone().into_scalar().unwrap(), s);
    }

    let e = RowValue::List(vec![]).into_scalar().unwrap_err();
    assert!(matches!(
        e,
        DbError::Format(FormatError::RecordPayloadTypeMismatch)
    ));
}

#[test]
fn encode_row_value_object_requires_object() {
    let mut out = Vec::new();
    let ty = Type::Object(vec![FieldDef {
        path: seg("x"),
        ty: Type::String,
        constraints: vec![],
    }]);
    let e = encode_row_value(&mut out, &RowValue::Int64(1), &ty).unwrap_err();
    assert!(matches!(
        e,
        DbError::Format(FormatError::RecordPayloadTypeMismatch)
    ));
}
