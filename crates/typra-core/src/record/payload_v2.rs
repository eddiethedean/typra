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
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/unit/src_record_payload_v2_tests.rs"
    ));
}
