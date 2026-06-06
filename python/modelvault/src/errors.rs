//! Maps [`modelvault_core::DbError`] to Python exceptions with stable `.code` and `.details`.

use modelvault_core::DbError;
use pyo3::create_exception;
use pyo3::exceptions::{PyOSError, PyRuntimeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::PyDict;
use pyo3::PyErr;

create_exception!(modelvault, ModelVaultFormatError, PyValueError);
create_exception!(modelvault, ModelVaultSchemaError, PyValueError);
create_exception!(modelvault, ModelVaultValidationError, PyValueError);
create_exception!(modelvault, ModelVaultQueryError, PyValueError);
create_exception!(modelvault, ModelVaultTransactionError, PyRuntimeError);

fn details_dict(py: Python<'_>, err: &DbError) -> Py<PyDict> {
    let dict = PyDict::new(py);
    for (k, v) in err.details() {
        let _ = dict.set_item(k, v);
    }
    dict.unbind()
}

fn attach_structured_fields(py: Python<'_>, py_err: PyErr, err: &DbError) -> PyErr {
    let value = py_err.value(py);
    let _ = value.setattr("code", err.kind().as_str());
    let _ = value.setattr("details", details_dict(py, err));
    py_err
}

/// Convert a core error into the Python exception type used for that category.
pub fn db_error_to_py(err: DbError) -> PyErr {
    Python::attach(|py| {
        let py_err = match &err {
            DbError::Io(e) => PyOSError::new_err(e.to_string()),
            DbError::Format(_) => ModelVaultFormatError::new_err(err.to_string()),
            DbError::Schema(_) => ModelVaultSchemaError::new_err(err.to_string()),
            DbError::Validation(_) => ModelVaultValidationError::new_err(err.to_string()),
            DbError::Transaction(_) => ModelVaultTransactionError::new_err(err.to_string()),
            DbError::Query(_) => ModelVaultQueryError::new_err(err.to_string()),
            DbError::NotImplemented => PyRuntimeError::new_err("not implemented"),
        };
        match &err {
            DbError::Io(_) | DbError::NotImplemented => py_err,
            _ => attach_structured_fields(py, py_err, &err),
        }
    })
}
