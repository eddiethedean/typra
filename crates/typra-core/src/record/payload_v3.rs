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
        // If a parent is not an object, overwrite with an object to preserve the invariant that
        // schema paths describe object nesting.
        if !matches!(cur, RowValue::Object(_)) {
            *cur = RowValue::Object(BTreeMap::new());
        }
        if let RowValue::Object(map) = cur { cur = map.entry(key).or_insert_with(|| RowValue::Object(BTreeMap::new())); }
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

#[cfg(test)]
mod tests {
    use crate::record::row_value::decode_row_value;
    use std::borrow::Cow;
    use std::collections::BTreeMap;

    use super::{
        decode_field_path, encode_field_path, insert_value_at_path, decode_record_payload_any,
        encode_record_payload_v3_op, RECORD_PAYLOAD_VERSION_V3,
    };
    use crate::error::{DbError, FormatError, SchemaError};
    use crate::record::payload_v1::OP_DELETE;
    use crate::record::row_value::RowValue;
    use crate::record::scalar::{Cursor, ScalarValue};
    use crate::schema::{FieldDef, FieldPath, Type};

    #[test]
    fn encode_field_path_rejects_empty_path_and_empty_segment() {
        let mut out = Vec::new();
        let fp_empty = FieldPath(vec![]);
        let err = encode_field_path(&mut out, &fp_empty).unwrap_err();
        assert!(matches!(err, DbError::Schema(SchemaError::InvalidFieldPath)));

        let mut out2 = Vec::new();
        let fp_bad_seg = FieldPath(vec![Cow::Borrowed("")]);
        let err = encode_field_path(&mut out2, &fp_bad_seg).unwrap_err();
        assert!(matches!(err, DbError::Schema(SchemaError::InvalidFieldPath)));
    }

    #[test]
    fn decode_field_path_rejects_n_zero_and_len_zero() {
        // n=0
        let mut cur = Cursor::new(&[0u8]);
        let err = decode_field_path(&mut cur).unwrap_err();
        assert!(matches!(err, DbError::Schema(SchemaError::InvalidFieldPath)));

        // n=1, len=0
        let mut cur = Cursor::new(&[1u8, 0u8, 0u8]);
        let err = decode_field_path(&mut cur).unwrap_err();
        assert!(matches!(err, DbError::Schema(SchemaError::InvalidFieldPath)));
    }

    #[test]
    fn insert_value_at_path_overwrites_non_object_parent_and_rejects_non_object_root() {
        // Overwrite branch: parent exists but is not an object.
        let mut root = BTreeMap::new();
        root.insert("a".to_string(), RowValue::Int64(1));
        let path = FieldPath(vec![Cow::Borrowed("a"), Cow::Borrowed("b")]);
        // For len==2 paths, we reject when the existing top-level value isn't an object.
        let err = insert_value_at_path(&mut root, &path, RowValue::String("x".into())).unwrap_err();
        assert!(matches!(
            err,
            DbError::Format(FormatError::RecordPayloadTypeMismatch)
        ));

        // For len>=3, we overwrite non-object parents to preserve schema nesting invariants.
        let mut root2 = BTreeMap::new();
        root2.insert("a".to_string(), RowValue::Int64(1));
        let path2 = FieldPath(vec![
            Cow::Borrowed("a"),
            Cow::Borrowed("b"),
            Cow::Borrowed("c"),
        ]);
        insert_value_at_path(&mut root2, &path2, RowValue::Int64(9)).unwrap();
        assert!(matches!(root2.get("a"), Some(RowValue::Object(_))));
    }

    #[test]
    fn decode_record_payload_v3_rejects_delete_with_nonzero_field_count() {
        let pk_ty = Type::String;
        let fields = vec![FieldDef::new(
            FieldPath::new([Cow::Borrowed("id")]).unwrap(),
            pk_ty.clone(),
        )];

        let mut bytes = Vec::new();
        bytes.extend_from_slice(&RECORD_PAYLOAD_VERSION_V3.to_le_bytes());
        bytes.extend_from_slice(&1u32.to_le_bytes());
        bytes.extend_from_slice(&1u32.to_le_bytes());
        bytes.push(OP_DELETE);
        crate::record::scalar::encode_tagged_scalar(
            &mut bytes,
            &ScalarValue::String("k".into()),
            &pk_ty,
        )
        .unwrap();
        bytes.extend_from_slice(&1u32.to_le_bytes()); // n=1 (invalid for delete)

        let err = decode_record_payload_any(&bytes, "id", &pk_ty, &fields).unwrap_err();
        assert!(matches!(
            err,
            DbError::Format(FormatError::RecordPayloadTypeMismatch)
        ));
    }

    #[test]
    fn decode_record_payload_v3_rejects_duplicate_paths_and_trailing_bytes() {
        let pk_ty = Type::String;
        let f1 = FieldDef::new(
            FieldPath::new([Cow::Borrowed("id")]).unwrap(),
            pk_ty.clone(),
        );
        let f2 = FieldDef::new(
            FieldPath::new([Cow::Borrowed("x")]).unwrap(),
            Type::Int64,
        );
        let fields = vec![f1.clone(), f2.clone()];

        let bytes = encode_record_payload_v3_op(
            1,
            1,
            crate::record::payload_v1::OP_INSERT,
            &ScalarValue::String("k".into()),
            &pk_ty,
            &[(f2.clone(), RowValue::Int64(7))],
        )
        .unwrap();

        // Duplicate the single field entry by patching n=2 but leaving only one encoded field.
        // Layout: version(2) + cid(4) + schema(4) + op(1) + pk(tagged) + n(u32)
        let mut dup = bytes.clone();
        // Decode the header to compute n offset.
        let mut cur = Cursor::new(&dup);
        cur.take_u16().unwrap();
        cur.take_u32().unwrap();
        cur.take_u32().unwrap();
        cur.take_u8().unwrap();
        crate::record::scalar::decode_tagged_scalar(&mut cur, &pk_ty).unwrap();
        let n_offset = dup.len() - cur.remaining();
        dup[n_offset..n_offset + 4].copy_from_slice(&2u32.to_le_bytes());

        let err = decode_record_payload_any(&dup, "id", &pk_ty, &fields).unwrap_err();
        assert!(matches!(
            err,
            DbError::Format(FormatError::TruncatedRecordPayload)
                | DbError::Format(FormatError::RecordPayloadTypeMismatch)
        ));

        // Trailing bytes error: append one byte to a valid payload.
        let mut trailing = bytes;
        trailing.push(0);
        let err = decode_record_payload_any(&trailing, "id", &pk_ty, &fields).unwrap_err();
        assert!(matches!(err, DbError::Format(FormatError::TrailingRecordPayload)));
    }

    #[test]
    fn encode_record_payload_v3_delete_op_emits_zero_count_and_early_returns() {
        let pk_ty = Type::String;
        let bytes = encode_record_payload_v3_op(
            1,
            1,
            OP_DELETE,
            &ScalarValue::String("k".into()),
            &pk_ty,
            &[],
        )
        .unwrap();

        // version + cid + schema + op + pk(tagged) + n(u32==0)
        let mut cur = Cursor::new(&bytes);
        assert_eq!(cur.take_u16().unwrap(), RECORD_PAYLOAD_VERSION_V3);
        assert_eq!(cur.take_u32().unwrap(), 1);
        assert_eq!(cur.take_u32().unwrap(), 1);
        assert_eq!(cur.take_u8().unwrap(), OP_DELETE);
        let _ = crate::record::scalar::decode_tagged_scalar(&mut cur, &pk_ty).unwrap();
        assert_eq!(cur.take_u32().unwrap(), 0);
        assert_eq!(cur.remaining(), 0);
    }

    #[test]
    fn decode_record_payload_any_rejects_too_short_and_unknown_version() {
        let pk_ty = Type::String;
        let fields = vec![FieldDef::new(
            FieldPath::new([Cow::Borrowed("id")]).unwrap(),
            pk_ty.clone(),
        )];

        let err = decode_record_payload_any(&[], "id", &pk_ty, &fields).unwrap_err();
        assert!(matches!(
            err,
            DbError::Format(FormatError::TruncatedRecordPayload)
        ));

        let mut b = Vec::new();
        b.extend_from_slice(&999u16.to_le_bytes());
        b.extend_from_slice(&[0, 0, 0, 0]);
        let err = decode_record_payload_any(&b, "id", &pk_ty, &fields).unwrap_err();
        assert!(matches!(
            err,
            DbError::Format(FormatError::UnknownRecordPayloadVersion { .. })
        ));
    }

    #[test]
    fn insert_value_at_path_rejects_empty_and_creates_nested_objects_on_happy_path() {
        let mut root = BTreeMap::new();
        let err = insert_value_at_path(&mut root, &FieldPath(vec![]), RowValue::Int64(1)).unwrap_err();
        assert!(matches!(err, DbError::Schema(SchemaError::InvalidFieldPath)));

        let mut root2 = BTreeMap::new();
        let path = FieldPath(vec![Cow::Borrowed("a"), Cow::Borrowed("b"), Cow::Borrowed("c")]);
        insert_value_at_path(&mut root2, &path, RowValue::String("x".into())).unwrap();
        assert!(matches!(root2.get("a"), Some(RowValue::Object(_))));
    }

    #[test]
    fn decode_record_payload_any_dispatches_v1_and_v2() {
        let pk_ty = Type::String;
        let f_id = FieldDef::new(FieldPath::new([Cow::Borrowed("id")]).unwrap(), pk_ty.clone());
        let f_x = FieldDef::new(FieldPath::new([Cow::Borrowed("x")]).unwrap(), Type::Int64);
        let fields = vec![f_id.clone(), f_x.clone()];

        let pk = ScalarValue::String("k".into());

        let v1 = crate::record::payload_v1::encode_record_payload_v1(
            1,
            1,
            &pk,
            &pk_ty,
            &[(f_x.clone(), ScalarValue::Int64(7))],
        )
        .unwrap();
        let got1 = decode_record_payload_any(&v1, "id", &pk_ty, &fields).unwrap();
        assert_eq!(got1.op, crate::record::payload_v1::OP_INSERT);

        let v2 = crate::record::payload_v2::encode_record_payload_v2_op(
            1,
            1,
            crate::record::payload_v1::OP_INSERT,
            &pk,
            &pk_ty,
            &[(f_x.clone(), RowValue::Int64(7))],
        )
        .unwrap();
        let got2 = decode_record_payload_any(&v2, "id", &pk_ty, &fields).unwrap();
        assert_eq!(got2.op, crate::record::payload_v1::OP_INSERT);
    }

    #[test]
    fn decode_record_payload_v3_rejects_wrong_field_count_and_unknown_field_path() {
        let pk_ty = Type::String;
        let f_id = FieldDef::new(FieldPath::new([Cow::Borrowed("id")]).unwrap(), pk_ty.clone());
        let f_x = FieldDef::new(FieldPath::new([Cow::Borrowed("x")]).unwrap(), Type::Int64);
        let fields = vec![f_id.clone(), f_x.clone()];

        // Wrong n (claims 2, only 1 non-pk def exists) => type mismatch.
        let mut bytes = encode_record_payload_v3_op(
            1,
            1,
            crate::record::payload_v1::OP_INSERT,
            &ScalarValue::String("k".into()),
            &pk_ty,
            &[(f_x.clone(), RowValue::Int64(7))],
        )
        .unwrap();

        // Patch n to 2.
        let mut cur = Cursor::new(&bytes);
        cur.take_u16().unwrap();
        cur.take_u32().unwrap();
        cur.take_u32().unwrap();
        cur.take_u8().unwrap();
        crate::record::scalar::decode_tagged_scalar(&mut cur, &pk_ty).unwrap();
        let n_offset = bytes.len() - cur.remaining();
        bytes[n_offset..n_offset + 4].copy_from_slice(&2u32.to_le_bytes());
        let err = decode_record_payload_any(&bytes, "id", &pk_ty, &fields).unwrap_err();
        assert!(matches!(
            err,
            DbError::Format(FormatError::RecordPayloadTypeMismatch)
        ));

        // Unknown path: encode one entry but with field path "y" not in schema.
        let mut bad = encode_record_payload_v3_op(
            1,
            1,
            crate::record::payload_v1::OP_INSERT,
            &ScalarValue::String("k".into()),
            &pk_ty,
            &[(f_x.clone(), RowValue::Int64(7))],
        )
        .unwrap();
        // Find field path start: after version+cid+schema+op+pk+n.
        let mut c2 = Cursor::new(&bad);
        c2.take_u16().unwrap();
        c2.take_u32().unwrap();
        c2.take_u32().unwrap();
        c2.take_u8().unwrap();
        crate::record::scalar::decode_tagged_scalar(&mut c2, &pk_ty).unwrap();
        c2.take_u32().unwrap(); // n
        let fp_start = bad.len() - c2.remaining();
        // Overwrite encoded field path with single segment "y".
        let mut repl = Vec::new();
        repl.push(1u8);
        repl.extend_from_slice(&1u16.to_le_bytes());
        repl.extend_from_slice(b"y");
        bad[fp_start..fp_start + repl.len()].copy_from_slice(&repl);
        let err = decode_record_payload_any(&bad, "id", &pk_ty, &fields).unwrap_err();
        assert!(matches!(
            err,
            DbError::Format(FormatError::RecordPayloadTypeMismatch)
        ));
    }

    #[test]
    fn decode_record_payload_v3_delete_with_zero_fields_is_ok() {
        let pk_ty = Type::String;
        let f_id = FieldDef::new(FieldPath::new([Cow::Borrowed("id")]).unwrap(), pk_ty.clone());
        let f_x = FieldDef::new(FieldPath::new([Cow::Borrowed("x")]).unwrap(), Type::Int64);
        let fields = vec![f_id, f_x];

        let bytes = encode_record_payload_v3_op(
            1,
            1,
            crate::record::payload_v1::OP_DELETE,
            &ScalarValue::String("k".into()),
            &pk_ty,
            &[],
        )
        .unwrap();
        let dec = decode_record_payload_any(&bytes, "id", &pk_ty, &fields).unwrap();
        assert_eq!(dec.op, crate::record::payload_v1::OP_DELETE);
        assert!(dec.fields.is_empty());
    }

    #[test]
    fn decode_record_payload_v3_rejects_duplicate_paths() {
        let pk_ty = Type::String;
        let f_id = FieldDef::new(FieldPath::new([Cow::Borrowed("id")]).unwrap(), pk_ty.clone());
        let f_a = FieldDef::new(FieldPath::new([Cow::Borrowed("a")]).unwrap(), Type::Int64);
        let f_b = FieldDef::new(FieldPath::new([Cow::Borrowed("b")]).unwrap(), Type::Int64);
        let fields = vec![f_id, f_a.clone(), f_b.clone()];

        // Start from a valid payload with two distinct fields, then overwrite the second path
        // to be a duplicate of the first.
        let mut bytes = encode_record_payload_v3_op(
            1,
            1,
            crate::record::payload_v1::OP_INSERT,
            &ScalarValue::String("k".into()),
            &pk_ty,
            &[
                (f_a.clone(), RowValue::Int64(1)),
                (f_b.clone(), RowValue::Int64(2)),
            ],
        )
        .unwrap();

        let mut cur = Cursor::new(&bytes);
        cur.take_u16().unwrap();
        cur.take_u32().unwrap();
        cur.take_u32().unwrap();
        cur.take_u8().unwrap();
        crate::record::scalar::decode_tagged_scalar(&mut cur, &pk_ty).unwrap();
        cur.take_u32().unwrap(); // n
        let fp1_start = bytes.len() - cur.remaining();
        let _fp1 = decode_field_path(&mut cur).unwrap();
        let fp1_len = (bytes.len() - cur.remaining()) - fp1_start;
        // fp2 starts immediately after fp1 + first value
        let _v1 = decode_row_value(&mut cur, &Type::Int64).unwrap();
        let fp2_start = bytes.len() - cur.remaining();

        let fp1_bytes: Vec<u8> = bytes[fp1_start..fp1_start + fp1_len].to_vec();
        bytes[fp2_start..fp2_start + fp1_len].copy_from_slice(&fp1_bytes);

        let err = decode_record_payload_any(&bytes, "id", &pk_ty, &fields).unwrap_err();
        assert!(matches!(
            err,
            DbError::Format(FormatError::RecordPayloadTypeMismatch)
        ));
    }

    #[test]
    fn insert_value_at_path_creates_nested_objects_for_len_ge_3() {
        let mut root = BTreeMap::new();
        let fp =
            FieldPath::new([Cow::Borrowed("a"), Cow::Borrowed("b"), Cow::Borrowed("c")]).unwrap();
        insert_value_at_path(&mut root, &fp, RowValue::Int64(9)).unwrap();

        let mut expected_c = BTreeMap::new();
        expected_c.insert("c".to_string(), RowValue::Int64(9));
        let mut expected_b = BTreeMap::new();
        expected_b.insert("b".to_string(), RowValue::Object(expected_c));
        let mut expected_a = BTreeMap::new();
        expected_a.insert("a".to_string(), RowValue::Object(expected_b));
        assert_eq!(root, expected_a);
    }
}
