use crate::error::DbError;

pub struct Database;

impl Database {
    pub fn open(_path: &str) -> Result<Self, DbError> {
        Ok(Self)
    }
}
