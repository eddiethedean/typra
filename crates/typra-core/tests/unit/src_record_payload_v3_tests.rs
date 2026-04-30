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
    fn insert_value_at_path_nested_existing_object_children() {
        let mut root = BTreeMap::new();
        root.insert(
            "a".to_string(),
            RowValue::Object(BTreeMap::from([(
                "b".to_string(),
                RowValue::Object(BTreeMap::new()),
            )])),
        );
        let path = FieldPath(vec![
            Cow::Borrowed("a"),
            Cow::Borrowed("b"),
            Cow::Borrowed("c"),
        ]);
        insert_value_at_path(&mut root, &path, RowValue::Int64(42)).unwrap();
        let a = root.get("a").unwrap();
        let RowValue::Object(am) = a else {
            panic!();
        };
        let b = am.get("b").unwrap();
        let RowValue::Object(bm) = b else {
            panic!();
        };
        assert_eq!(bm.get("c"), Some(&RowValue::Int64(42)));
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
