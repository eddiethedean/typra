//! Exhaustive-style tests for record scalar encoding and payload v1 edges.

use std::borrow::Cow;

use typra_core::error::{DbError, FormatError};
use typra_core::record::{decode_record_payload_v1, encode_record_payload_v1, RowValue};
use typra_core::record::{decode_tagged_scalar, encode_tagged_scalar, Cursor, ScalarValue};
use typra_core::schema::{FieldDef, FieldPath, Type};

fn field(name: &str, ty: Type) -> FieldDef {
    FieldDef {
        path: FieldPath(vec![Cow::Owned(name.to_string())]),
        ty,
        constraints: vec![],
    }
}

fn roundtrip(v: ScalarValue, ty: &Type) {
    let mut buf = Vec::new();
    encode_tagged_scalar(&mut buf, &v, ty).unwrap();
    let mut c = Cursor::new(&buf);
    let got = decode_tagged_scalar(&mut c, ty).unwrap();
    assert_eq!(got, v);
    assert_eq!(c.remaining(), 0);
}

#[test]
fn scalar_roundtrip_all_primitives() {
    roundtrip(ScalarValue::Bool(false), &Type::Bool);
    roundtrip(ScalarValue::Bool(true), &Type::Bool);
    roundtrip(ScalarValue::Int64(-1), &Type::Int64);
    roundtrip(ScalarValue::Int64(i64::MAX), &Type::Int64);
    roundtrip(ScalarValue::Uint64(0), &Type::Uint64);
    roundtrip(ScalarValue::Uint64(u64::MAX), &Type::Uint64);
    roundtrip(ScalarValue::Float64(1.5), &Type::Float64);
    roundtrip(ScalarValue::String("hello".to_string()), &Type::String);
    roundtrip(ScalarValue::Bytes(vec![1, 2, 3]), &Type::Bytes);
    roundtrip(ScalarValue::Uuid([7u8; 16]), &Type::Uuid);
    roundtrip(ScalarValue::Timestamp(42), &Type::Timestamp);
}

#[test]
fn canonical_key_bytes_cover_all_variants() {
    assert_eq!(ScalarValue::Bool(false).canonical_key_bytes(), vec![0, 0]);
    assert_eq!(ScalarValue::Bool(true).canonical_key_bytes(), vec![0, 1]);
    assert_eq!(
        ScalarValue::Int64(-2).canonical_key_bytes(),
        (-2i64).to_le_bytes().to_vec()
    );
    assert_eq!(
        ScalarValue::Uint64(42).canonical_key_bytes(),
        42u64.to_le_bytes().to_vec()
    );
    assert_eq!(
        ScalarValue::Float64(1.5).canonical_key_bytes(),
        1.5f64.to_le_bytes().to_vec()
    );
    let k = ScalarValue::String("ab".to_string()).canonical_key_bytes();
    assert_eq!(k, b"ab");
    assert_eq!(
        ScalarValue::Bytes(vec![1, 2]).canonical_key_bytes(),
        vec![1u8, 2]
    );
    let u = [0x12u8; 16];
    assert_eq!(ScalarValue::Uuid(u).canonical_key_bytes(), u.to_vec());
    assert_eq!(
        ScalarValue::Timestamp(-9).canonical_key_bytes(),
        (-9i64).to_le_bytes().to_vec()
    );
}

#[test]
fn ty_matches_false_for_mismatches() {
    assert!(!ScalarValue::Int64(1).ty_matches(&Type::String));
    assert!(!ScalarValue::String("x".into()).ty_matches(&Type::Int64));
    assert!(!ScalarValue::Bool(true).ty_matches(&Type::Int64));
}

#[test]
fn encode_rejects_value_type_mismatch() {
    let mut buf = Vec::new();
    let e = encode_tagged_scalar(&mut buf, &ScalarValue::Int64(1), &Type::String).unwrap_err();
    assert!(matches!(
        e,
        DbError::Format(FormatError::RecordPayloadTypeMismatch)
    ));
}

#[test]
fn decode_rejects_wrong_tag_for_schema_type() {
    let mut buf = Vec::new();
    encode_tagged_scalar(&mut buf, &ScalarValue::Int64(1), &Type::Int64).unwrap();
    // Buffer is tag 1 + i64; decoding as String expects tag 4 first
    let mut c = Cursor::new(&buf);
    let e = decode_tagged_scalar(&mut c, &Type::String).unwrap_err();
    assert!(matches!(
        e,
        DbError::Format(FormatError::RecordPayloadTypeMismatch)
    ));
}

#[test]
fn decode_wrong_tag_for_each_primitive() {
    // Int64 payload (tag 1) decoded as Uint64 expects tag 2
    let mut buf = Vec::new();
    encode_tagged_scalar(&mut buf, &ScalarValue::Int64(1), &Type::Int64).unwrap();
    let mut c = Cursor::new(&buf);
    assert!(decode_tagged_scalar(&mut c, &Type::Uint64).is_err());

    // Uint64 decoded as Float64
    let mut buf = Vec::new();
    encode_tagged_scalar(&mut buf, &ScalarValue::Uint64(2), &Type::Uint64).unwrap();
    let mut c = Cursor::new(&buf);
    assert!(decode_tagged_scalar(&mut c, &Type::Float64).is_err());

    // Float64 decoded as Int64
    let mut buf = Vec::new();
    encode_tagged_scalar(&mut buf, &ScalarValue::Float64(1.0), &Type::Float64).unwrap();
    let mut c = Cursor::new(&buf);
    assert!(decode_tagged_scalar(&mut c, &Type::Int64).is_err());

    // String decoded as Bytes
    let mut buf = Vec::new();
    encode_tagged_scalar(&mut buf, &ScalarValue::String("x".into()), &Type::String).unwrap();
    let mut c = Cursor::new(&buf);
    assert!(decode_tagged_scalar(&mut c, &Type::Bytes).is_err());

    // Bytes decoded as Uuid
    let mut buf = Vec::new();
    encode_tagged_scalar(&mut buf, &ScalarValue::Bytes(vec![1]), &Type::Bytes).unwrap();
    let mut c = Cursor::new(&buf);
    assert!(decode_tagged_scalar(&mut c, &Type::Uuid).is_err());

    // Uuid decoded as Timestamp
    let mut buf = Vec::new();
    encode_tagged_scalar(&mut buf, &ScalarValue::Uuid([3; 16]), &Type::Uuid).unwrap();
    let mut c = Cursor::new(&buf);
    assert!(decode_tagged_scalar(&mut c, &Type::Timestamp).is_err());

    // Timestamp decoded as Bool
    let mut buf = Vec::new();
    encode_tagged_scalar(&mut buf, &ScalarValue::Timestamp(9), &Type::Timestamp).unwrap();
    let mut c = Cursor::new(&buf);
    assert!(decode_tagged_scalar(&mut c, &Type::Bool).is_err());
}

#[test]
fn roundtrip_empty_string() {
    roundtrip(ScalarValue::String(String::new()), &Type::String);
}

#[test]
fn decode_string_invalid_utf8() {
    let mut buf = vec![4u8];
    buf.extend_from_slice(&(1u32.to_le_bytes()));
    buf.push(0xff);
    let mut c = Cursor::new(&buf);
    let e = decode_tagged_scalar(&mut c, &Type::String).unwrap_err();
    assert!(matches!(e, DbError::Format(FormatError::InvalidRecordUtf8)));
}

#[test]
fn decode_unsupported_composite_type() {
    let buf = [0u8];
    let mut c = Cursor::new(&buf);
    let e = decode_tagged_scalar(&mut c, &Type::Optional(Box::new(Type::String))).unwrap_err();
    assert!(matches!(
        e,
        DbError::Format(FormatError::RecordPayloadUnsupportedType)
    ));
}

#[test]
fn cursor_truncation_errors() {
    let mut c = Cursor::new(&[0u8]);
    assert!(matches!(
        c.take_u16().unwrap_err(),
        DbError::Format(FormatError::TruncatedRecordPayload)
    ));
    let mut c = Cursor::new(&[1, 2, 3]);
    assert!(matches!(
        c.take_u32().unwrap_err(),
        DbError::Format(FormatError::TruncatedRecordPayload)
    ));
    let mut c = Cursor::new(&[]);
    assert!(matches!(
        c.take_u8().unwrap_err(),
        DbError::Format(FormatError::TruncatedRecordPayload)
    ));
    let mut c = Cursor::new(&[1, 2, 3, 4, 5, 6, 7]);
    assert!(matches!(
        c.take_i64().unwrap_err(),
        DbError::Format(FormatError::TruncatedRecordPayload)
    ));
    let mut c = Cursor::new(&[1, 2, 3, 4, 5, 6, 7]);
    assert!(matches!(
        c.take_u64().unwrap_err(),
        DbError::Format(FormatError::TruncatedRecordPayload)
    ));
    let mut c = Cursor::new(&[4, 2, 0, 0, 0]);
    assert!(matches!(
        c.take_bytes(10).unwrap_err(),
        DbError::Format(FormatError::TruncatedRecordPayload)
    ));
}

#[test]
fn payload_unknown_version() {
    let mut b = vec![99u8, 0];
    b.extend_from_slice(&1u32.to_le_bytes());
    let e = decode_record_payload_v1(&b, "pk", &Type::Int64, &[]).unwrap_err();
    assert!(matches!(
        e,
        DbError::Format(FormatError::UnknownRecordPayloadVersion { got: 99 })
    ));
}

#[test]
fn payload_field_count_mismatch() {
    let fields = vec![field("pk", Type::Int64), field("a", Type::String)];
    // Encoded body has 0 non-PK scalars, but schema expects one non-PK field ("a").
    let buf = encode_record_payload_v1(1, 1, &ScalarValue::Int64(1), &Type::Int64, &[]).unwrap();
    let e = decode_record_payload_v1(&buf, "pk", &Type::Int64, &fields).unwrap_err();
    assert!(matches!(
        e,
        DbError::Format(FormatError::RecordPayloadTypeMismatch)
    ));
}

#[test]
fn payload_trailing_bytes_rejected() {
    let fields = vec![field("pk", Type::Int64)];
    let buf = encode_record_payload_v1(1, 1, &ScalarValue::Int64(1), &Type::Int64, &[]).unwrap();
    let mut long = buf;
    long.push(0);
    let e = decode_record_payload_v1(&long, "pk", &Type::Int64, &fields).unwrap_err();
    assert!(matches!(
        e,
        DbError::Format(FormatError::TrailingRecordPayload)
    ));
}

#[test]
fn payload_truncated_mid_stream() {
    let fields = vec![field("pk", Type::Int64)];
    let buf = encode_record_payload_v1(1, 1, &ScalarValue::Int64(1), &Type::Int64, &[]).unwrap();
    let truncated = &buf[..buf.len().saturating_sub(3)];
    let e = decode_record_payload_v1(truncated, "pk", &Type::Int64, &fields).unwrap_err();
    assert!(matches!(
        e,
        DbError::Format(FormatError::TruncatedRecordPayload)
    ));
}

#[test]
fn payload_roundtrip_two_non_pk_fields() {
    let fields = vec![
        field("pk", Type::String),
        field("a", Type::Int64),
        field("b", Type::Bool),
    ];
    let non_pk = vec![
        (field("a", Type::Int64), ScalarValue::Int64(9)),
        (field("b", Type::Bool), ScalarValue::Bool(true)),
    ];
    let buf = encode_record_payload_v1(
        3,
        2,
        &ScalarValue::String("k".into()),
        &Type::String,
        &non_pk,
    )
    .unwrap();
    let d = decode_record_payload_v1(&buf, "pk", &Type::String, &fields).unwrap();
    assert_eq!(d.collection_id, 3);
    assert_eq!(d.schema_version, 2);
    assert_eq!(d.op, 1);
    assert_eq!(d.pk, ScalarValue::String("k".into()));
    assert_eq!(d.fields.get("a"), Some(&RowValue::Int64(9)));
    assert_eq!(d.fields.get("b"), Some(&RowValue::Bool(true)));
}

#[test]
fn decode_bool_bad_second_byte_treated_as_false_or_truncation() {
    // tag 0, second byte missing
    let mut c = Cursor::new(&[0u8]);
    let e = decode_tagged_scalar(&mut c, &Type::Bool).unwrap_err();
    assert!(matches!(
        e,
        DbError::Format(FormatError::TruncatedRecordPayload)
    ));
}
