    use super::*;

    #[test]
    fn decode_tagged_string_rejects_wrong_tag() {
        let mut cur = Cursor::new(&[0u8]);
        let e = decode_tagged_string(&mut cur).unwrap_err();
        assert!(matches!(
            e,
            DbError::Format(FormatError::RecordPayloadTypeMismatch)
        ));
    }

    #[test]
    fn decode_tagged_scalar_rejects_optional_type() {
        let mut cur = Cursor::new(&[1u8, 0, 0, 0, 0, 0, 0, 0, 0]);
        let ty = Type::Optional(Box::new(Type::Int64));
        let e = decode_tagged_scalar(&mut cur, &ty).unwrap_err();
        assert!(matches!(
            e,
            DbError::Format(FormatError::RecordPayloadUnsupportedType)
        ));
    }

    #[test]
    fn decode_tagged_string_rejects_invalid_utf8() {
        let mut buf = vec![4u8];
        buf.extend_from_slice(&3u32.to_le_bytes());
        buf.extend_from_slice(&[0xFF, 0xFE, 0xFF]);
        let mut cur = Cursor::new(&buf);
        let e = decode_tagged_string(&mut cur).unwrap_err();
        assert!(matches!(
            e,
            DbError::Format(FormatError::InvalidRecordUtf8)
        ));
    }
