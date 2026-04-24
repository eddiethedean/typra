//! Record segment payload version 3: supports multi-segment schema `FieldPath`s.
//!
//! v1/v2 encode non-PK fields in a fixed top-level order only. v3 encodes each field with its
//! `FieldPath` so nested leaf fields can be persisted/replayed without flattening.

use std::collections::{BTreeMap, HashMap, HashSet};

use crate::error::{DbError, FormatError, SchemaError};
use crate::record::payload_v1::{
    decode_record_payload_v1_body, DecodedRecord, OP_DELETE, RECORD_PAYLOAD_VERSION,
};
use crate::record::payload_v2::{decode_record_payload_v2_body, RECORD_PAYLOAD_VERSION_V2};
use crate::record::row_value::{decode_row_value, encode_row_value, RowValue};
use crate::record::scalar::{decode_tagged_scalar, encode_tagged_scalar, Cursor, ScalarValue};
use crate::schema::{FieldDef, FieldPath, Type};

pub const RECORD_PAYLOAD_VERSION_V3: u16 = 3;

fn encode_field_path(out: &mut Vec<u8>, fp: &FieldPath) -> Result<(), DbError> {
    // Keep encoding simple/stable: u8 segments, then for each segment u16 len + utf8 bytes.
    // FieldPath segments are validated at schema registration time; still guard here.
    let n = fp.0.len();
    if n == 0 || n > u8::MAX as usize {
        return Err(DbError::Schema(SchemaError::InvalidFieldPath));
    }
    out.push(n as u8);
    for seg in &fp.0 {
        let s = seg.as_ref();
        if s.is_empty() || s.len() > u16::MAX as usize {
            return Err(DbError::Schema(SchemaError::InvalidFieldPath));
        }
        out.extend_from_slice(&(s.len() as u16).to_le_bytes());
        out.extend_from_slice(s.as_bytes());
    }
    Ok(())
}

fn decode_field_path(cur: &mut Cursor<'_>) -> Result<FieldPath, DbError> {
    let n = cur.take_u8()? as usize;
    if n == 0 {
        return Err(DbError::Schema(SchemaError::InvalidFieldPath));
    }
    let mut parts = Vec::with_capacity(n);
    for _ in 0..n {
        let len = cur.take_u16()? as usize;
        if len == 0 {
            return Err(DbError::Schema(SchemaError::InvalidFieldPath));
        }
        let bytes = cur.take_bytes(len)?;
        let s = std::str::from_utf8(&bytes)
            .map_err(|_| DbError::Schema(SchemaError::InvalidFieldPath))?;
        parts.push(std::borrow::Cow::Owned(s.to_string()));
    }
    Ok(FieldPath(parts))
}

fn insert_value_at_path(
    root: &mut BTreeMap<String, RowValue>,
    path: &FieldPath,
    value: RowValue,
) -> Result<(), DbError> {
    if path.0.is_empty() {
        return Err(DbError::Schema(SchemaError::InvalidFieldPath));
    }
    let head = path.0[0].as_ref().to_string();
    if path.0.len() == 1 {
        root.insert(head, value);
        return Ok(());
    }

    // Walk/create nested objects.
    let mut cur = root
        .entry(head)
        .or_insert_with(|| RowValue::Object(BTreeMap::new()));
    for seg in path.0.iter().skip(1).take(path.0.len() - 2) {
        let key = seg.as_ref().to_string();
        cur = match cur {
            RowValue::Object(map) => map
                .entry(key)
                .or_insert_with(|| RowValue::Object(BTreeMap::new())),
            // If a parent is not an object, overwrite with an object to preserve the invariant that
            // schema paths describe object nesting.
            other => {
                *other = RowValue::Object(BTreeMap::new());
                let RowValue::Object(map) = other else {
                    unreachable!()
                };
                map.entry(key)
                    .or_insert_with(|| RowValue::Object(BTreeMap::new()))
            }
        };
    }
    let leaf_key = path.0.last().unwrap().as_ref().to_string();
    match cur {
        RowValue::Object(map) => {
            map.insert(leaf_key, value);
            Ok(())
        }
        _ => Err(DbError::Format(FormatError::RecordPayloadTypeMismatch)),
    }
}

/// Encode a record segment body (version 3) with an explicit operation code.
pub fn encode_record_payload_v3_op(
    collection_id: u32,
    schema_version: u32,
    op: u8,
    pk: &ScalarValue,
    pk_ty: &Type,
    non_pk_in_schema_order: &[(FieldDef, RowValue)],
) -> Result<Vec<u8>, DbError> {
    let mut out = Vec::new();
    out.extend_from_slice(&RECORD_PAYLOAD_VERSION_V3.to_le_bytes());
    out.extend_from_slice(&collection_id.to_le_bytes());
    out.extend_from_slice(&schema_version.to_le_bytes());
    out.push(op);
    encode_tagged_scalar(&mut out, pk, pk_ty)?;
    if op == OP_DELETE {
        out.extend_from_slice(&0u32.to_le_bytes());
        return Ok(out);
    }
    out.extend_from_slice(&(non_pk_in_schema_order.len() as u32).to_le_bytes());
    for (def, val) in non_pk_in_schema_order {
        encode_field_path(&mut out, &def.path)?;
        encode_row_value(&mut out, val, &def.ty)?;
    }
    Ok(out)
}

/// Encode a record segment body (version 3) insert.
pub fn encode_record_payload_v3(
    collection_id: u32,
    schema_version: u32,
    pk: &ScalarValue,
    pk_ty: &Type,
    non_pk_in_schema_order: &[(FieldDef, RowValue)],
) -> Result<Vec<u8>, DbError> {
    encode_record_payload_v3_op(
        collection_id,
        schema_version,
        crate::record::payload_v1::OP_INSERT,
        pk,
        pk_ty,
        non_pk_in_schema_order,
    )
}

pub(crate) fn decode_record_payload_v3_body(
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

    // Build expected defs (all non-PK defs, any path length).
    let expected: Vec<&FieldDef> = fields
        .iter()
        .filter(|f| !(f.path.0.len() == 1 && f.path.0[0] == pk_name))
        .collect();
    if op == OP_DELETE {
        if n != 0 {
            return Err(DbError::Format(FormatError::RecordPayloadTypeMismatch));
        }
    } else if n != expected.len() {
        return Err(DbError::Format(FormatError::RecordPayloadTypeMismatch));
    }

    let mut by_path: HashMap<&FieldPath, &FieldDef> = HashMap::new();
    for def in &expected {
        by_path.insert(&def.path, def);
    }

    let mut seen: HashSet<FieldPath> = HashSet::new();
    let mut out_fields: BTreeMap<String, RowValue> = BTreeMap::new();
    if op != OP_DELETE {
        for _ in 0..n {
            let fp = decode_field_path(&mut cur)?;
            if !seen.insert(fp.clone()) {
                return Err(DbError::Format(FormatError::RecordPayloadTypeMismatch));
            }
            let def = by_path
                .iter()
                .find(|(p, _)| **p == &fp)
                .map(|(_, d)| *d)
                .ok_or(DbError::Format(FormatError::RecordPayloadTypeMismatch))?;
            let v = decode_row_value(&mut cur, &def.ty)?;
            insert_value_at_path(&mut out_fields, &fp, v)?;
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

/// Decode v1/v2/v3 record payload.
pub fn decode_record_payload_any(
    bytes: &[u8],
    pk_name: &str,
    pk_ty: &Type,
    fields: &[FieldDef],
) -> Result<DecodedRecord, DbError> {
    if bytes.len() < 2 {
        return Err(DbError::Format(FormatError::TruncatedRecordPayload));
    }
    let ver = u16::from_le_bytes([bytes[0], bytes[1]]);
    let mut cur = Cursor::new(bytes);
    cur.take_u16()?; // consume version
    match ver {
        RECORD_PAYLOAD_VERSION => decode_record_payload_v1_body(cur, pk_name, pk_ty, fields),
        RECORD_PAYLOAD_VERSION_V2 => decode_record_payload_v2_body(cur, pk_name, pk_ty, fields),
        RECORD_PAYLOAD_VERSION_V3 => decode_record_payload_v3_body(cur, pk_name, pk_ty, fields),
        _ => Err(DbError::Format(FormatError::UnknownRecordPayloadVersion {
            got: ver,
        })),
    }
}
