//! Helpers to run sync engine work on a thread pool and expose asyncio awaitables.

use pyo3::prelude::*;

use crate::db_handle::SharedDbHandle;

pub(crate) type SharedInner = SharedDbHandle;

/// Run `f` without holding the GIL, then return a Python awaitable wrapping the result.
pub(crate) fn future_into_blocking<'py, T, F>(py: Python<'py>, f: F) -> PyResult<Bound<'py, PyAny>>
where
    T: for<'a> IntoPyObject<'a> + Send + 'static,
    F: FnOnce() -> PyResult<T> + Send + 'static,
{
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        tokio::task::spawn_blocking(f).await.map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "spawn_blocking join error: {e}"
            ))
        })?
    })
}

/// Run `gil_setup` with the GIL (for Py object conversion), then release the GIL for engine work.
pub(crate) fn future_into_gil_then_blocking<'py, T, G, B>(
    py: Python<'py>,
    gil_setup: G,
) -> PyResult<Bound<'py, PyAny>>
where
    T: for<'a> IntoPyObject<'a> + Send + 'static,
    G: FnOnce(Python<'py>) -> PyResult<B> + Send + 'static,
    B: FnOnce() -> PyResult<T> + Send + 'static,
{
    let blocking = gil_setup(py)?;
    pyo3_async_runtimes::tokio::future_into_py(py, async move {
        tokio::task::spawn_blocking(blocking).await.map_err(|e| {
            PyErr::new::<pyo3::exceptions::PyRuntimeError, _>(format!(
                "spawn_blocking join error: {e}"
            ))
        })?
    })
}
