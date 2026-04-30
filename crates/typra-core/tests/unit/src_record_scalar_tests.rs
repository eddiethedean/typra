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
