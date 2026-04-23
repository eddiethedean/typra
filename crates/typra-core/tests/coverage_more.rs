//! Targeted tests to raise practical line coverage for core modules:
//! - `validation` constraint variants and edge branches
//! - record v2 row-value encode/decode error paths
//! - record payload v2 decode error paths

use std::borrow::Cow;
use typra_core::error::{DbError, FormatError};
use typra_core::record::{
    decode_record_payload, encode_record_payload_v2, encode_row_value, encode_tagged_scalar,
    RowValue, ScalarValue, OP_INSERT, RECORD_PAYLOAD_VERSION_V2,
};
use typra_core::schema::{Constraint, FieldDef, FieldPath, Type};
use typra_core::validation::validate_value;

fn seg(s: &str) -> FieldPath {
    FieldPath::new([Cow::Owned(s.to_string())]).unwrap()
}

#[test]
fn validation_constraints_cover_all_variants() {
    let mut p = vec!["x".to_string()];

    // Signed ints.
    assert!(matches!(
        validate_value(
            &mut p,
            &Type::Int64,
            &[Constraint::MaxI64(1)],
            &RowValue::Int64(2)
        ),
        Err(DbError::Validation(_))
    ));
    validate_value(
        &mut p,
        &Type::Int64,
        &[Constraint::MinI64(-2)],
        &RowValue::Int64(-2),
    )
    .unwrap();

    // Unsigned ints.
    assert!(matches!(
        validate_value(
            &mut p,
            &Type::Uint64,
            &[Constraint::MinU64(2)],
            &RowValue::Uint64(1)
        ),
        Err(DbError::Validation(_))
    ));
    assert!(matches!(
        validate_value(
            &mut p,
            &Type::Uint64,
            &[Constraint::MaxU64(2)],
            &RowValue::Uint64(3)
        ),
        Err(DbError::Validation(_))
    ));

    // Floats.
    assert!(matches!(
        validate_value(
            &mut p,
            &Type::Float64,
            &[Constraint::MinF64(2.0)],
            &RowValue::Float64(1.0)
        ),
        Err(DbError::Validation(_))
    ));
    assert!(matches!(
        validate_value(
            &mut p,
            &Type::Float64,
            &[Constraint::MaxF64(2.0)],
            &RowValue::Float64(3.0)
        ),
        Err(DbError::Validation(_))
    ));

    // Length constraints (string/bytes/list).
    assert!(matches!(
        validate_value(
            &mut p,
            &Type::String,
            &[Constraint::MinLength(3)],
            &RowValue::String("hi".into())
        ),
        Err(DbError::Validation(_))
    ));
    assert!(matches!(
        validate_value(
            &mut p,
            &Type::Bytes,
            &[Constraint::MaxLength(1)],
            &RowValue::Bytes(vec![1, 2])
        ),
        Err(DbError::Validation(_))
    ));
    assert!(matches!(
        validate_value(
            &mut p,
            &Type::List(Box::new(Type::Int64)),
            &[Constraint::MinLength(1)],
            &RowValue::List(vec![])
        ),
        Err(DbError::Validation(_))
    ));

    // Regex invalid schema and mismatch.
    assert!(matches!(
        validate_value(
            &mut p,
            &Type::String,
            &[Constraint::Regex("(".into())],
            &RowValue::String("x".into())
        ),
        Err(DbError::Validation(v)) if v.message.contains("invalid regex")
    ));
    assert!(matches!(
        validate_value(
            &mut p,
            &Type::String,
            &[Constraint::Regex("^a+$".into())],
            &RowValue::String("b".into())
        ),
        Err(DbError::Validation(v)) if v.message.contains("does not match")
    ));

    // Email / URL.
    assert!(matches!(
        validate_value(
            &mut p,
            &Type::String,
            &[Constraint::Email],
            &RowValue::String("nope".into())
        ),
        Err(DbError::Validation(_))
    ));
    assert!(matches!(
        validate_value(
            &mut p,
            &Type::String,
            &[Constraint::Url],
            &RowValue::String("ftp://x".into())
        ),
        Err(DbError::Validation(_))
    ));

    // NonEmpty: empty and wrong type.
    assert!(matches!(
        validate_value(
            &mut p,
            &Type::String,
            &[Constraint::NonEmpty],
            &RowValue::String("".into())
        ),
        Err(DbError::Validation(_))
    ));
    assert!(matches!(
        validate_value(
            &mut p,
            &Type::Int64,
            &[Constraint::NonEmpty],
            &RowValue::Int64(1)
        ),
        Err(DbError::Validation(_))
    ));

    // Url / email success paths.
    validate_value(
        &mut p,
        &Type::String,
        &[Constraint::Url],
        &RowValue::String("https://example.com/x".into()),
    )
    .unwrap();
    validate_value(
        &mut p,
        &Type::String,
        &[Constraint::Email],
        &RowValue::String("a@b.co".into()),
    )
    .unwrap();

    // NonEmpty: empty bytes and non-empty bytes ok.
    assert!(matches!(
        validate_value(
            &mut p,
            &Type::Bytes,
            &[Constraint::NonEmpty],
            &RowValue::Bytes(vec![])
        ),
        Err(DbError::Validation(_))
    ));
    validate_value(
        &mut p,
        &Type::Bytes,
        &[Constraint::NonEmpty],
        &RowValue::Bytes(vec![0]),
    )
    .unwrap();

    // MinLength / MaxLength on bytes.
    assert!(matches!(
        validate_value(
            &mut p,
            &Type::Bytes,
            &[Constraint::MinLength(2)],
            &RowValue::Bytes(vec![1])
        ),
        Err(DbError::Validation(_))
    ));
    validate_value(
        &mut p,
        &Type::Bytes,
        &[Constraint::MaxLength(2)],
        &RowValue::Bytes(vec![1, 2]),
    )
    .unwrap();
}

#[test]
fn record_payload_v2_optional_presence_tag_mismatch_errors() {
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
    let pk_ty = &fields[0].ty;
    let pk = ScalarValue::String("k".into());
    let non_pk = vec![(fields[1].clone(), RowValue::None)];
    let mut payload = encode_record_payload_v2(1, 1, &pk, pk_ty, &non_pk).unwrap();

    // The optional value encoding is a single presence byte; flip it from 0 -> 2.
    let idx = payload
        .iter()
        .rposition(|b| *b == 0)
        .expect("presence byte");
    payload[idx] = 2;

    let e = decode_record_payload(&payload, "id", pk_ty, &fields).unwrap_err();
    assert!(matches!(
        e,
        DbError::Format(FormatError::RecordPayloadTypeMismatch)
    ));
}

#[test]
fn record_payload_v2_decode_errors_field_count_and_trailing_bytes() {
    let fields = vec![
        FieldDef {
            path: seg("id"),
            ty: Type::String,
            constraints: vec![],
        },
        FieldDef {
            path: seg("n"),
            ty: Type::Int64,
            constraints: vec![],
        },
    ];
    let pk_ty = &fields[0].ty;
    let pk = ScalarValue::String("k".into());
    let non_pk = vec![(
        fields[1].clone(),
        RowValue::Int64(1), // correct non-pk value
    )];
    let payload = encode_record_payload_v2(1, 1, &pk, pk_ty, &non_pk).unwrap();

    // Trailing bytes.
    let mut with_trailing = payload.clone();
    with_trailing.push(0);
    let e = decode_record_payload(&with_trailing, "id", pk_ty, &fields).unwrap_err();
    assert!(matches!(
        e,
        DbError::Format(FormatError::TrailingRecordPayload)
    ));

    // Field-count mismatch: encode a v2 payload that declares 0 non-pk fields,
    // but still includes one encoded value.
    let mut bad_n = Vec::new();
    bad_n.extend_from_slice(&RECORD_PAYLOAD_VERSION_V2.to_le_bytes());
    bad_n.extend_from_slice(&1u32.to_le_bytes()); // collection_id
    bad_n.extend_from_slice(&1u32.to_le_bytes()); // schema_version
    bad_n.push(OP_INSERT);
    encode_tagged_scalar(&mut bad_n, &pk, pk_ty).unwrap();
    bad_n.extend_from_slice(&0u32.to_le_bytes()); // declared count
    encode_row_value(&mut bad_n, &RowValue::Int64(1), &Type::Int64).unwrap(); // actual value

    let e = decode_record_payload(&bad_n, "id", pk_ty, &fields).unwrap_err();
    assert!(matches!(
        e,
        DbError::Format(FormatError::RecordPayloadTypeMismatch)
    ));
}
