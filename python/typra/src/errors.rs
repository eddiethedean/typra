//! Map [`typra_core::DbError`] to Python exceptions (single place for error policy).

use pyo3::exceptions::{PyOSError, PyRuntimeError, PyValueError};
use pyo3::PyErr;
use typra_core::DbError;

pub fn db_error_to_py(err: DbError) -> PyErr {
    match err {
        DbError::Io(e) => PyOSError::new_err(e.to_string()),
        DbError::Format(e) => PyValueError::new_err(e.to_string()),
        DbError::Schema(e) => PyValueError::new_err(e.to_string()),
        DbError::NotImplemented => PyRuntimeError::new_err("not implemented"),
    }
}
