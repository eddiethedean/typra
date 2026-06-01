//! Shared helpers for collection naming rules.

use crate::catalog::MAX_COLLECTION_NAME_BYTES;
use crate::error::{DbError, SchemaError};

pub(crate) fn normalize_collection_name(name: &str) -> Result<String, DbError> {
    let name = name.trim();
    if name.is_empty() {
        return Err(DbError::Schema(SchemaError::InvalidCollectionName));
    }
    if name.len() > MAX_COLLECTION_NAME_BYTES {
        return Err(DbError::Schema(SchemaError::InvalidCollectionName));
    }
    Ok(name.to_string())
}
