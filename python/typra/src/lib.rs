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
        "Python bindings for Typra: typed embedded database (Database.open, register_collection, collection_names).",
    )?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_class::<database::Database>()?;
    Ok(())
}
