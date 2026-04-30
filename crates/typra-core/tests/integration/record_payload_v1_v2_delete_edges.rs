use std::borrow::Cow;

use typra_core::error::{DbError, FormatError};
use typra_core::record::{
    decode_record_payload, decode_record_payload_v1, encode_record_payload_v1, encode_record_payload_v2_op,
    encode_tagged_scalar, Cursor, OP_DELETE, RECORD_PAYLOAD_VERSION_V2, ScalarValue,
};
use typra_core::schema::{FieldDef, FieldPath, Type};

fn seg(s: &str) -> FieldPath {
    FieldPath::new([Cow::Owned(s.to_string())]).unwrap()
}

#[test]
fn record_payload_v1_delete_rejects_nonzero_field_count_and_allows_zero() {
    let pk_ty = Type::String;
    let fields = vec![FieldDef {
        path: seg("id"),
        ty: pk_ty.clone(),
        constraints: vec![],
    }];

    // Build a delete payload by hand (v1 doesn't expose a delete encoder).
    let pk = ScalarValue::String("k".into());
    let mut b = Vec::new();
    b.extend_from_slice(&1u16.to_le_bytes());
    b.extend_from_slice(&1u32.to_le_bytes()); // collection_id
    b.extend_from_slice(&2u32.to_le_bytes()); // schema_version
    b.push(OP_DELETE);
    encode_tagged_scalar(&mut b, &pk, &pk_ty).unwrap();
    b.extend_from_slice(&1u32.to_le_bytes()); // n != 0 should error for delete

    let e = decode_record_payload_v1(&b, "id", &pk_ty, &fields).unwrap_err();
    assert!(matches!(
        e,
        DbError::Format(FormatError::RecordPayloadTypeMismatch)
    ));

    // Same but n=0 should decode successfully with empty fields.
    let mut b2 = b.clone();
    let n_pos = b2.len() - 4;
    b2[n_pos..].copy_from_slice(&0u32.to_le_bytes());
    let got = decode_record_payload_v1(&b2, "id", &pk_ty, &fields).unwrap();
    assert_eq!(got.op, OP_DELETE);
    assert!(got.fields.is_empty());
}

#[test]
fn record_payload_v1_trailing_bytes_errors() {
    let fields = vec![
        FieldDef {
            path: seg("id"),
            ty: Type::String,
            constraints: vec![],
        },
        FieldDef {
            path: seg("x"),
            ty: Type::Int64,
            constraints: vec![],
        },
    ];

    let pk = ScalarValue::String("k".into());
    let non_pk = vec![(fields[1].clone(), ScalarValue::Int64(1))];
    let mut b = encode_record_payload_v1(1, 1, &pk, &fields[0].ty, &non_pk).unwrap();
    b.push(0);

    let e = decode_record_payload_v1(&b, "id", &fields[0].ty, &fields).unwrap_err();
    assert!(matches!(e, DbError::Format(FormatError::TrailingRecordPayload)));
}

#[test]
fn record_payload_v2_delete_encoder_writes_zero_count_and_decoder_rejects_nonzero_count() {
    let fields = vec![
        FieldDef {
            path: seg("id"),
            ty: Type::String,
            constraints: vec![],
        },
        FieldDef {
            path: seg("x"),
            ty: Type::Optional(Box::new(Type::String)),
            constraints: vec![],
        },
    ];
    let pk = ScalarValue::String("k".into());

    let b = encode_record_payload_v2_op(1, 1, OP_DELETE, &pk, &fields[0].ty, &[]).unwrap();
    // Ensure count is present and zero by parsing through pk.
    let mut cur = Cursor::new(&b);
    assert_eq!(cur.take_u16().unwrap(), RECORD_PAYLOAD_VERSION_V2);
    let _ = cur.take_u32().unwrap(); // collection_id
    let _ = cur.take_u32().unwrap(); // schema_version
    assert_eq!(cur.take_u8().unwrap(), OP_DELETE);
    let _ = typra_core::record::decode_tagged_scalar(&mut cur, &fields[0].ty).unwrap();
    let count_pos = cur.pos;
    assert_eq!(cur.take_u32().unwrap(), 0);

    // Patch count to 1 and ensure decoder rejects.
    let mut b2 = b.clone();
    b2[count_pos..count_pos + 4].copy_from_slice(&1u32.to_le_bytes());
    let e = decode_record_payload(&b2, "id", &fields[0].ty, &fields).unwrap_err();
    assert!(matches!(
        e,
        DbError::Format(FormatError::RecordPayloadTypeMismatch)
    ));
}

