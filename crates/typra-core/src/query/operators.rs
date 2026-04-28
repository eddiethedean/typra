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
mod limit_op_propagation_tests {
    use super::{LimitOp, RowSource, RowKey};
    use crate::error::{DbError, FormatError};

    struct ThenErr;
    impl RowSource for ThenErr {
        fn next_key(&mut self) -> Option<Result<RowKey, DbError>> {
            Some(Err(DbError::Format(FormatError::UnsupportedVersion {
                major: 0,
                minor: 0,
            })))
        }
    }

    #[test]
    fn limit_op_propagates_source_error() {
        let mut lim = LimitOp::new(ThenErr, 1);
        assert!(lim.next_key().unwrap().is_err());
    }
}
