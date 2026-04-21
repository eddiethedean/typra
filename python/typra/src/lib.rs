use pyo3::prelude::*;
use pyo3::types::PyModule;

#[pymodule]
fn typra(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__doc__", "Python bindings for Typra (preview).")?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
