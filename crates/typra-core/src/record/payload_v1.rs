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
