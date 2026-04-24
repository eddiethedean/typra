// PyO3 `IntoPy` / `extract` patterns often trigger `useless_conversion`; keep noise down.
#![allow(clippy::useless_conversion)]

mod database;
mod dbapi;
mod errors;
mod fields_json;
mod inner_db;
mod query;
mod row_values;

use pyo3::prelude::*;

#[pymodule]
fn typra(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add(
        "__doc__",
        "Typra Python bindings: typed embedded database built on the Rust engine.\n\n\
         Import ``Database`` for ``open``, ``open_in_memory``, ``register_collection``, ``insert``, ``get``, \
         ``transaction`` (context manager), ``collection`` (query builder), and ``collection_names``. \
         See the package README for ``fields_json`` and error mapping.",
    )?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_class::<database::Database>()?;
    m.add_class::<database::PyTransaction>()?;
    m.add_class::<query::Collection>()?;
    m.add_class::<query::QueryBuilder>()?;

    // DB-API 2.0 (PEP 249) read-only adapter (0.10.0).
    let dbapi_mod = PyModule::new_bound(m.py(), "dbapi")?;
    dbapi_mod.add_function(wrap_pyfunction!(dbapi::connect, &dbapi_mod)?)?;
    dbapi_mod.add_class::<dbapi::Connection>()?;
    dbapi_mod.add_class::<dbapi::Cursor>()?;
    m.add_submodule(&dbapi_mod)?;
    m.add("dbapi", dbapi_mod)?;
    Ok(())
}
