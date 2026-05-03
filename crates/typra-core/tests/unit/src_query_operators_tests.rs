    use super::*;
    use crate::error::FormatError;
    use crate::schema::CollectionId;

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

    struct OkTwice {
        n: u8,
    }

    impl RowSource for OkTwice {
        fn next_key(&mut self) -> Option<Result<RowKey, DbError>> {
            if self.n == 0 {
                return None;
            }
            self.n -= 1;
            Some(Ok((
                CollectionId(1),
                vec![self.n],
            )))
        }
    }

    #[test]
    fn limit_op_returns_none_when_limit_zero() {
        let mut op = LimitOp::new(OkTwice { n: 5 }, 0);
        assert!(op.next_key().is_none());
    }

    #[test]
    fn limit_op_stops_after_n_and_none_from_inner() {
        let mut op = LimitOp::new(OkTwice { n: 2 }, 1);
        assert!(op.next_key().unwrap().is_ok());
        assert!(op.next_key().is_none());
    }

    #[test]
    fn boxed_row_source_delegates_next_key() {
        let mut src: Box<dyn RowSource> = Box::new(OkTwice { n: 1 });
        assert!(src.next_key().unwrap().is_ok());
        assert!(src.next_key().is_none());
    }
