//! Maps [`modelvault_core::DbError`] to `OSError`, `ValueError`, or `RuntimeError` so Python callers get
//! stable exception types from the C extension.

use modelvault_core::DbError;
use pyo3::create_exception;
use pyo3::exceptions::{PyOSError, PyRuntimeError, PyValueError};
use pyo3::PyErr;

create_exception!(modelvault, ModelVaultFormatError, PyValueError);
create_exception!(modelvault, ModelVaultSchemaError, PyValueError);
create_exception!(modelvault, ModelVaultValidationError, PyValueError);
create_exception!(modelvault, ModelVaultQueryError, PyValueError);
create_exception!(modelvault, ModelVaultTransactionError, PyRuntimeError);

/// Convert a core error into the Python exception type used for that category (I/O vs format/schema vs stub).
pub fn db_error_to_py(err: DbError) -> PyErr {
    match err {
        DbError::Io(e) => PyOSError::new_err(e.to_string()),
        DbError::Format(e) => ModelVaultFormatError::new_err(e.to_string()),
        DbError::Schema(e) => ModelVaultSchemaError::new_err(e.to_string()),
        DbError::Validation(e) => ModelVaultValidationError::new_err(e.to_string()),
        DbError::Transaction(e) => ModelVaultTransactionError::new_err(e.to_string()),
        DbError::Query(e) => ModelVaultQueryError::new_err(e.message),
        DbError::NotImplemented => PyRuntimeError::new_err("not implemented"),
    }
}
