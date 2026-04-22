// PyO3 `IntoPy` / `extract` patterns often trigger `useless_conversion`; keep noise down.
#![allow(clippy::useless_conversion)]

mod database;
mod errors;
mod fields_json;
mod inner_db;
mod row_values;

use pyo3::prelude::*;

#[pymodule]
fn typra(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add(
        "__doc__",
        "Typra Python bindings: typed embedded database built on the Rust engine.\n\n\
         Import ``Database`` for ``open``, ``open_in_memory``, ``register_collection``, ``insert``, ``get``, \
         and ``collection_names``. See the package README for ``fields_json`` and error mapping.",
    )?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_class::<database::Database>()?;
    Ok(())
}
