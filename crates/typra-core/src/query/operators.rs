use crate::error::DbError;
use crate::schema::CollectionId;

pub type RowKey = (CollectionId, Vec<u8>);

pub trait RowSource {
    fn next_key(&mut self) -> Option<Result<RowKey, DbError>>;
}

impl<T: RowSource + ?Sized> RowSource for Box<T> {
    fn next_key(&mut self) -> Option<Result<RowKey, DbError>> {
        (**self).next_key()
    }
}

pub struct LimitOp<S: RowSource> {
    inner: S,
    remaining: usize,
}

impl<S: RowSource> LimitOp<S> {
    pub fn new(inner: S, n: usize) -> Self {
        Self {
            inner,
            remaining: n,
        }
    }
}

impl<S: RowSource> RowSource for LimitOp<S> {
    fn next_key(&mut self) -> Option<Result<RowKey, DbError>> {
        if self.remaining == 0 {
            return None;
        }
        match self.inner.next_key() {
            None => None,
            Some(Ok(rk)) => {
                self.remaining = self.remaining.saturating_sub(1);
                Some(Ok(rk))
            }
            Some(Err(e)) => Some(Err(e)),
        }
    }
}

#[cfg(test)]
mod tests {
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
}
