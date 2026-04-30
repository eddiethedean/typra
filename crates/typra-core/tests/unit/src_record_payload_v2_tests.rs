    use std::borrow::Cow;

    use super::*;
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
        assert_eq!(got2.op, crate::record::payload_v1::OP_INSERT);
        assert_eq!(got2.fields.len(), 1);
    }
