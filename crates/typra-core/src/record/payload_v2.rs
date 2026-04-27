//! Record segment payload version 2 (composite values). See `docs/07_record_encoding_v2.md`.

use std::collections::BTreeMap;

use crate::error::{DbError, FormatError};
use crate::record::payload_v1::{DecodedRecord, OP_DELETE};
use crate::record::payload_v3::decode_record_payload_any;
use crate::record::row_value::{decode_row_value, encode_row_value, RowValue};
use crate::record::scalar::{decode_tagged_scalar, encode_tagged_scalar, Cursor, ScalarValue};
use crate::schema::{FieldDef, Type};

pub const RECORD_PAYLOAD_VERSION_V2: u16 = 2;

/// Encode a record segment body (version 2). Preferred for all new inserts (0.6+).
pub fn encode_record_payload_v2(
    collection_id: u32,
    schema_version: u32,
    pk: &ScalarValue,
    pk_ty: &Type,
    non_pk_ordered: &[(FieldDef, RowValue)],
) -> Result<Vec<u8>, DbError> {
    encode_record_payload_v2_op(
        collection_id,
        schema_version,
        crate::record::payload_v1::OP_INSERT,
        pk,
        pk_ty,
        non_pk_ordered,
    )
}

/// Encode a record segment body (version 2) with an explicit operation code.
pub fn encode_record_payload_v2_op(
    collection_id: u32,
    schema_version: u32,
    op: u8,
    pk: &ScalarValue,
    pk_ty: &Type,
    non_pk_ordered: &[(FieldDef, RowValue)],
) -> Result<Vec<u8>, DbError> {
    let mut out = Vec::new();
    out.extend_from_slice(&RECORD_PAYLOAD_VERSION_V2.to_le_bytes());
    out.extend_from_slice(&collection_id.to_le_bytes());
    out.extend_from_slice(&schema_version.to_le_bytes());
    out.push(op);
    encode_tagged_scalar(&mut out, pk, pk_ty)?;
    if op == OP_DELETE {
        out.extend_from_slice(&0u32.to_le_bytes());
    } else {
        out.extend_from_slice(&(non_pk_ordered.len() as u32).to_le_bytes());
        for (def, val) in non_pk_ordered {
            encode_row_value(&mut out, val, &def.ty)?;
        }
    }
    Ok(out)
}

/// Decode v2 body (cursor after version `u16`).
pub(crate) fn decode_record_payload_v2_body(
    mut cur: Cursor<'_>,
    pk_name: &str,
    pk_ty: &Type,
    fields: &[FieldDef],
) -> Result<DecodedRecord, DbError> {
    let collection_id = cur.take_u32()?;
    let schema_version = cur.take_u32()?;
    let op = cur.take_u8()?;
    let pk = decode_tagged_scalar(&mut cur, pk_ty)?;
    let n = cur.take_u32()? as usize;

    let non_pk_defs: Vec<&FieldDef> = fields
        .iter()
        .filter(|f| f.path.0.len() == 1 && f.path.0[0] != pk_name)
        .collect();
    let expected_n = if op == OP_DELETE { 0 } else { non_pk_defs.len() };
    if n != expected_n {
        return Err(DbError::Format(FormatError::RecordPayloadTypeMismatch));
    }

    let mut out_fields = BTreeMap::new();
    if op != OP_DELETE {
        for def in non_pk_defs {
            let name = def.path.0[0].to_string();
            let v = decode_row_value(&mut cur, &def.ty)?;
            out_fields.insert(name, v);
        }
    }
    if cur.remaining() != 0 {
        return Err(DbError::Format(FormatError::TrailingRecordPayload));
    }

    Ok(DecodedRecord {
        collection_id,
        schema_version,
        op,
        pk,
        fields: out_fields,
    })
}

/// Decode either v1 or v2 record payload.
pub fn decode_record_payload(
    bytes: &[u8],
    pk_name: &str,
    pk_ty: &Type,
    fields: &[FieldDef],
) -> Result<DecodedRecord, DbError> {
    // Delegate to the central v1/v2/v3 dispatcher.
    decode_record_payload_any(bytes, pk_name, pk_ty, fields)
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use super::{decode_record_payload, decode_record_payload_v2_body, encode_record_payload_v2_op, OP_DELETE};
    use crate::record::payload_v1::OP_INSERT;
    use crate::record::row_value::RowValue;
    use crate::record::scalar::Cursor;
    use crate::schema::{FieldDef, FieldPath, Type};
    use crate::record::scalar::ScalarValue;

    fn def(path: &[&'static str], ty: Type) -> FieldDef {
        FieldDef {
            path: FieldPath(path.iter().map(|s| Cow::Borrowed(*s)).collect()),
            ty,
            constraints: Vec::new(),
        }
    }

    #[test]
    fn v2_delete_rejects_nonzero_count_and_trailing_bytes() {
        let pk_ty = Type::Int64;
        let pk = ScalarValue::Int64(1);
        let fields = vec![def(&["id"], Type::Int64)];

        let mut bytes = encode_record_payload_v2_op(1, 1, OP_DELETE, &pk, &pk_ty, &[]).unwrap();
        let len = bytes.len();
        bytes[len - 4..].copy_from_slice(&1u32.to_le_bytes());
        assert!(decode_record_payload(&bytes, "id", &pk_ty, &fields).is_err());

        let mut bytes = encode_record_payload_v2_op(1, 1, OP_DELETE, &pk, &pk_ty, &[]).unwrap();
        bytes.push(0);
        assert!(decode_record_payload(&bytes, "id", &pk_ty, &fields).is_err());
    }

    #[test]
    fn v2_body_delete_rejects_nonzero_count() {
        let pk_ty = Type::Int64;
        let pk = ScalarValue::Int64(1);
        let fields = vec![def(&["id"], Type::Int64)];

        let mut bytes = encode_record_payload_v2_op(1, 1, OP_DELETE, &pk, &pk_ty, &[]).unwrap();
        let len = bytes.len();
        bytes[len - 4..].copy_from_slice(&1u32.to_le_bytes());

        // Body expects cursor positioned after version u16.
        let cur = Cursor::new(&bytes[2..]);
        assert!(decode_record_payload_v2_body(cur, "id", &pk_ty, &fields).is_err());
    }

    #[test]
    fn v2_delete_with_zero_count_decodes() {
        let pk_ty = Type::Int64;
        let pk = ScalarValue::Int64(1);
        let fields = vec![def(&["id"], Type::Int64), def(&["x"], Type::Int64)];

        let bytes = encode_record_payload_v2_op(1, 1, OP_DELETE, &pk, &pk_ty, &[]).unwrap();
        let decoded = decode_record_payload(&bytes, "id", &pk_ty, &fields).unwrap();
        assert_eq!(decoded.op, OP_DELETE);
        assert!(decoded.fields.is_empty());
    }

    #[test]
    fn v2_decode_ignores_nested_fields_in_schema_filter() {
        let pk_ty = Type::Int64;
        let pk = ScalarValue::Int64(1);

        let fields = vec![
            def(&["id"], Type::Int64),
            def(&["x"], Type::Int64),
            def(&["a", "b"], Type::Int64),
        ];
        let non_pk = vec![(
            def(&["x"], Type::Int64),
            RowValue::from_scalar(ScalarValue::Int64(2)),
        )];

        let bytes = encode_record_payload_v2_op(1, 1, OP_INSERT, &pk, &pk_ty, &non_pk).unwrap();
        let decoded = decode_record_payload(&bytes, "id", &pk_ty, &fields).unwrap();
        assert_eq!(decoded.fields.len(), 1);
        assert!(decoded.fields.contains_key("x"));
    }

    #[test]
    fn v2_encode_rejects_pk_type_mismatch() {
        let pk_ty = Type::Int64;
        let pk = ScalarValue::String("nope".to_string());
        let non_pk: Vec<(FieldDef, RowValue)> = Vec::new();

        assert!(encode_record_payload_v2_op(1, 1, OP_INSERT, &pk, &pk_ty, &non_pk).is_err());
    }

    #[test]
    fn v2_encode_rejects_non_pk_value_type_mismatch() {
        let pk_ty = Type::Int64;
        let pk = ScalarValue::Int64(1);
        let non_pk = vec![(
            def(&["x"], Type::Int64),
            RowValue::from_scalar(ScalarValue::String("nope".to_string())),
        )];

        assert!(encode_record_payload_v2_op(1, 1, OP_INSERT, &pk, &pk_ty, &non_pk).is_err());
    }
}
