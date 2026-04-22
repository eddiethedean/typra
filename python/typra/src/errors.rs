//! Maps [`typra_core::DbError`] to `OSError`, `ValueError`, or `RuntimeError` so Python callers get
//! stable exception types from the C extension.

use pyo3::exceptions::{PyOSError, PyRuntimeError, PyValueError};
use pyo3::PyErr;
use typra_core::DbError;

/// Convert a core error into the Python exception type used for that category (I/O vs format/schema vs stub).
pub fn db_error_to_py(err: DbError) -> PyErr {
    match err {
        DbError::Io(e) => PyOSError::new_err(e.to_string()),
        DbError::Format(e) => PyValueError::new_err(e.to_string()),
        DbError::Schema(e) => PyValueError::new_err(e.to_string()),
        DbError::NotImplemented => PyRuntimeError::new_err("not implemented"),
    }
}
