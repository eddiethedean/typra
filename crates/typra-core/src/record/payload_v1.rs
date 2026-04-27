//! Record segment body encoding v1 ([`RECORD_PAYLOAD_VERSION`]).

use std::collections::BTreeMap;

use crate::error::{DbError, FormatError};
use crate::record::row_value::RowValue;
use crate::record::scalar::{decode_tagged_scalar, encode_tagged_scalar, Cursor, ScalarValue};
use crate::schema::{FieldDef, Type};

pub const RECORD_PAYLOAD_VERSION: u16 = 1;
pub const OP_INSERT: u8 = 1;
pub const OP_REPLACE: u8 = 2;
pub const OP_DELETE: u8 = 3;

#[derive(Debug, Clone, PartialEq)]
pub struct DecodedRecord {
    pub collection_id: u32,
    pub schema_version: u32,
    pub op: u8,
    pub pk: ScalarValue,
    /// Top-level field name -> value for non-PK columns.
    pub fields: BTreeMap<String, RowValue>,
}

pub fn encode_record_payload_v1(
    collection_id: u32,
    schema_version: u32,
    pk: &ScalarValue,
    pk_ty: &Type,
    non_pk_ordered: &[(FieldDef, ScalarValue)],
) -> Result<Vec<u8>, DbError> {
    let mut out = Vec::new();
    out.extend_from_slice(&RECORD_PAYLOAD_VERSION.to_le_bytes());
    out.extend_from_slice(&collection_id.to_le_bytes());
    out.extend_from_slice(&schema_version.to_le_bytes());
    out.push(OP_INSERT);
    encode_tagged_scalar(&mut out, pk, pk_ty)?;
    out.extend_from_slice(&(non_pk_ordered.len() as u32).to_le_bytes());
    for (def, val) in non_pk_ordered {
        encode_tagged_scalar(&mut out, val, &def.ty)?;
    }
    Ok(out)
}

/// Decode v1 payload body (cursor positioned **after** the `u16` version field).
pub(crate) fn decode_record_payload_v1_body(
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
    if op == OP_DELETE {
        if n != 0 {
            return Err(DbError::Format(FormatError::RecordPayloadTypeMismatch));
        }
    } else if n != non_pk_defs.len() {
        return Err(DbError::Format(FormatError::RecordPayloadTypeMismatch));
    }

    let mut out_fields = BTreeMap::new();
    if op != OP_DELETE {
        for def in non_pk_defs {
            let name = def.path.0[0].to_string();
            let v = decode_tagged_scalar(&mut cur, &def.ty)?;
            out_fields.insert(name, RowValue::from_scalar(v));
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

pub fn decode_record_payload_v1(
    bytes: &[u8],
    pk_name: &str,
    pk_ty: &Type,
    fields: &[FieldDef],
) -> Result<DecodedRecord, DbError> {
    let mut cur = Cursor::new(bytes);
    let ver = cur.take_u16()?;
    if ver != RECORD_PAYLOAD_VERSION {
        return Err(DbError::Format(FormatError::UnknownRecordPayloadVersion {
            got: ver,
        }));
    }
    decode_record_payload_v1_body(cur, pk_name, pk_ty, fields)
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;

    use super::{
        decode_record_payload_v1, encode_record_payload_v1, DecodedRecord, OP_DELETE, OP_INSERT,
        RECORD_PAYLOAD_VERSION,
    };
    use crate::record::scalar::{encode_tagged_scalar, ScalarValue};
    use crate::schema::{FieldDef, FieldPath, Type};

    fn def(path: &[&'static str], ty: Type) -> FieldDef {
        FieldDef {
            path: FieldPath(path.iter().map(|s| Cow::Borrowed(*s)).collect()),
            ty,
            constraints: Vec::new(),
        }
    }

    #[test]
    fn v1_roundtrip_insert_decodes_and_rejects_trailing_bytes() {
        let pk_ty = Type::Int64;
        let pk = ScalarValue::Int64(1);
        let fields = vec![def(&["id"], Type::Int64), def(&["x"], Type::Int64)];
        let non_pk = vec![(def(&["x"], Type::Int64), ScalarValue::Int64(2))];

        let bytes = encode_record_payload_v1(1, 1, &pk, &pk_ty, &non_pk).unwrap();
        let DecodedRecord { op, fields: decoded, .. } =
            decode_record_payload_v1(&bytes, "id", &pk_ty, &fields).unwrap();
        assert_eq!(op, OP_INSERT);
        assert_eq!(decoded.len(), 1);

        let mut bytes2 = bytes.clone();
        bytes2.push(0);
        assert!(decode_record_payload_v1(&bytes2, "id", &pk_ty, &fields).is_err());
    }

    #[test]
    fn v1_decode_ignores_nested_fields_in_schema_filter() {
        let pk_ty = Type::Int64;
        let pk = ScalarValue::Int64(1);

        // Include a nested field (`a.b`) in the schema. v1 decoding should ignore it when building
        // the non-PK top-level ordered list.
        let fields = vec![
            def(&["id"], Type::Int64),
            def(&["x"], Type::Int64),
            def(&["a", "b"], Type::Int64),
        ];
        let non_pk = vec![(def(&["x"], Type::Int64), ScalarValue::Int64(2))];

        let bytes = encode_record_payload_v1(1, 1, &pk, &pk_ty, &non_pk).unwrap();
        let decoded = decode_record_payload_v1(&bytes, "id", &pk_ty, &fields).unwrap();
        assert_eq!(decoded.fields.len(), 1);
        assert!(decoded.fields.contains_key("x"));
    }

    #[test]
    fn v1_decode_rejects_non_pk_count_mismatch() {
        let pk_ty = Type::Int64;
        let pk = ScalarValue::Int64(1);

        let fields_pk_only = vec![def(&["id"], Type::Int64)];
        let non_pk = vec![(def(&["x"], Type::Int64), ScalarValue::Int64(2))];
        let bytes = encode_record_payload_v1(1, 1, &pk, &pk_ty, &non_pk).unwrap();
        assert!(decode_record_payload_v1(&bytes, "id", &pk_ty, &fields_pk_only).is_err());
    }

    #[test]
    fn v1_delete_rejects_nonzero_count() {
        let pk_ty = Type::Int64;
        let pk = ScalarValue::Int64(7);
        let fields = vec![def(&["id"], Type::Int64)];

        // Manual payload: v1 + OP_DELETE but with n=1 (invalid).
        let mut out = Vec::new();
        out.extend_from_slice(&RECORD_PAYLOAD_VERSION.to_le_bytes());
        out.extend_from_slice(&1u32.to_le_bytes()); // collection_id
        out.extend_from_slice(&1u32.to_le_bytes()); // schema_version
        out.push(OP_DELETE);
        encode_tagged_scalar(&mut out, &pk, &pk_ty).unwrap();
        out.extend_from_slice(&1u32.to_le_bytes()); // n (must be 0 for delete)

        assert!(decode_record_payload_v1(&out, "id", &pk_ty, &fields).is_err());
    }

    #[test]
    fn v1_delete_with_zero_count_decodes() {
        let pk_ty = Type::Int64;
        let pk = ScalarValue::Int64(7);
        let fields = vec![def(&["id"], Type::Int64), def(&["x"], Type::Int64)];

        let mut out = Vec::new();
        out.extend_from_slice(&RECORD_PAYLOAD_VERSION.to_le_bytes());
        out.extend_from_slice(&1u32.to_le_bytes()); // collection_id
        out.extend_from_slice(&1u32.to_le_bytes()); // schema_version
        out.push(OP_DELETE);
        encode_tagged_scalar(&mut out, &pk, &pk_ty).unwrap();
        out.extend_from_slice(&0u32.to_le_bytes()); // n=0

        let decoded = decode_record_payload_v1(&out, "id", &pk_ty, &fields).unwrap();
        assert_eq!(decoded.op, OP_DELETE);
        assert!(decoded.fields.is_empty());
    }
}
