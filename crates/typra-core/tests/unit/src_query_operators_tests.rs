    use super::*;
    use crate::error::FormatError;

    struct ErrOnce {
        returned: bool,
    }

    impl RowSource for ErrOnce {
        fn next_key(&mut self) -> Option<Result<RowKey, DbError>> {
            if self.returned {
                None
            } else {
                self.returned = true;
                Some(Err(DbError::Format(FormatError::InvalidCatalogPayload {
                    message: "boom".to_string(),
                })))
            }
        }
    }

    #[test]
    fn limit_op_propagates_inner_error() {
        let mut op = LimitOp::new(ErrOnce { returned: false }, 10);
        let got = op.next_key().unwrap();
        assert!(got.is_err());
        assert!(op.next_key().is_none());
    }
