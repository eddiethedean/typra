    use std::borrow::Cow;

    use super::*;
    use crate::record::payload_v1::OP_INSERT;
    use crate::schema::FieldPath;

    fn seg(s: &str) -> FieldPath {
        FieldPath::new([Cow::Owned(s.to_string())]).unwrap()
    }

    #[test]
    fn decode_record_payload_v2_body_delete_and_insert_ok_paths() {
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

        // DELETE: n==0, no fields.
        let b_del = encode_record_payload_v2_op(1, 1, OP_DELETE, &pk, &fields[0].ty, &[]).unwrap();
        let mut cur = Cursor::new(&b_del);
        assert_eq!(cur.take_u16().unwrap(), RECORD_PAYLOAD_VERSION_V2);
        let got = decode_record_payload_v2_body(cur, "id", &fields[0].ty, &fields).unwrap();
        assert_eq!(got.op, OP_DELETE);
        assert!(got.fields.is_empty());

        // INSERT: one non-pk field present.
        let b_ins = encode_record_payload_v2_op(
            1,
            1,
            crate::record::payload_v1::OP_INSERT,
            &pk,
            &fields[0].ty,
            &[(fields[1].clone(), RowValue::None)],
        )
        .unwrap();
        let mut cur2 = Cursor::new(&b_ins);
        assert_eq!(cur2.take_u16().unwrap(), RECORD_PAYLOAD_VERSION_V2);
        let got2 = decode_record_payload_v2_body(cur2, "id", &fields[0].ty, &fields).unwrap();
        assert_eq!(got2.op, OP_INSERT);
        assert_eq!(got2.fields.len(), 1);
    }

    #[test]
    fn decode_v2_delete_rejects_nonzero_field_count() {
        let fields = vec![FieldDef {
            path: seg("id"),
            ty: Type::Int64,
            constraints: vec![],
        }];
        let pk = ScalarValue::Int64(7);
        let mut b = encode_record_payload_v2_op(1, 1, OP_DELETE, &pk, &Type::Int64, &[]).unwrap();
        let n_off = b.len() - 4;
        b[n_off..n_off + 4].copy_from_slice(&9u32.to_le_bytes());

        let mut cur = Cursor::new(&b);
        assert_eq!(cur.take_u16().unwrap(), RECORD_PAYLOAD_VERSION_V2);
        let err = decode_record_payload_v2_body(cur, "id", &Type::Int64, &fields).unwrap_err();
        assert!(matches!(
            err,
            DbError::Format(crate::error::FormatError::RecordPayloadTypeMismatch)
        ));
    }

    #[test]
    fn decode_v2_insert_rejects_field_count_mismatch() {
        let fields = vec![
            FieldDef {
                path: seg("id"),
                ty: Type::Int64,
                constraints: vec![],
            },
            FieldDef {
                path: seg("x"),
                ty: Type::String,
                constraints: vec![],
            },
        ];
        let pk = ScalarValue::Int64(1);
        let b = encode_record_payload_v2_op(1, 1, OP_INSERT, &pk, &Type::Int64, &[]).unwrap();

        let mut cur = Cursor::new(&b);
        assert_eq!(cur.take_u16().unwrap(), RECORD_PAYLOAD_VERSION_V2);
        let err = decode_record_payload_v2_body(cur, "id", &Type::Int64, &fields).unwrap_err();
        assert!(matches!(
            err,
            DbError::Format(crate::error::FormatError::RecordPayloadTypeMismatch)
        ));
    }

    #[test]
    fn decode_v2_rejects_trailing_bytes_after_delete() {
        let fields = vec![FieldDef {
            path: seg("id"),
            ty: Type::Int64,
            constraints: vec![],
        }];
        let pk = ScalarValue::Int64(1);
        let mut b = encode_record_payload_v2_op(1, 1, OP_DELETE, &pk, &Type::Int64, &[]).unwrap();
        b.push(0);

        let mut cur = Cursor::new(&b);
        assert_eq!(cur.take_u16().unwrap(), RECORD_PAYLOAD_VERSION_V2);
        let err = decode_record_payload_v2_body(cur, "id", &Type::Int64, &fields).unwrap_err();
        assert!(matches!(
            err,
            DbError::Format(crate::error::FormatError::TrailingRecordPayload)
        ));
    }

    #[test]
    fn encode_record_payload_v2_wraps_insert_encoder() {
        let fields = vec![
            FieldDef {
                path: seg("id"),
                ty: Type::Int64,
                constraints: vec![],
            },
            FieldDef {
                path: seg("note"),
                ty: Type::String,
                constraints: vec![],
            },
        ];
        let pk = ScalarValue::Int64(9);
        let b = encode_record_payload_v2(
            2,
            3,
            &pk,
            &Type::Int64,
            &[(fields[1].clone(), RowValue::String("a".into()))],
        )
        .unwrap();

        let mut cur = Cursor::new(&b);
        assert_eq!(cur.take_u16().unwrap(), RECORD_PAYLOAD_VERSION_V2);
        let got = decode_record_payload_v2_body(cur, "id", &Type::Int64, &fields).unwrap();
        assert_eq!(got.op, OP_INSERT);
        assert_eq!(got.collection_id, 2);
        assert_eq!(got.schema_version, 3);
    }
