// PyO3 `IntoPy` / `extract` patterns often trigger `useless_conversion`; keep noise down.
#![allow(clippy::useless_conversion)]
// `pyo3::create_exception!` expands to cfgs that trip `unexpected_cfgs` under `-D warnings`.
#![allow(unexpected_cfgs)]

mod database;
mod dbapi;
mod errors;
mod fields_json;
mod inner_db;
mod models;
mod query;
mod row_values;

use pyo3::prelude::*;

#[pymodule]
fn typra(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add(
        "__doc__",
        "Typra Python bindings: typed embedded database built on the Rust engine.\n\n\
         Import ``Database`` for ``open``, ``open_in_memory``, ``register_collection``, ``register_schema_version``, \
         ``insert``, ``get``, ``delete``, ``transaction`` (context manager), ``collection`` (query builder), \
         and ``collection_names``. The experimental ``typra.dbapi`` module provides a small read-only DB-API 2.0 \
         adapter (minimal SELECT subset). See the package README for ``fields_json`` and error mapping.",
    )?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_class::<database::Database>()?;
    m.add_class::<database::PyTransaction>()?;
    m.add_class::<query::Collection>()?;
    m.add_class::<query::QueryBuilder>()?;

    // Stable error kinds via distinct exception subclasses (still `isinstance(..., ValueError)` etc).
    m.add(
        "TypraFormatError",
        m.py().get_type_bound::<errors::TypraFormatError>(),
    )?;
    m.add(
        "TypraSchemaError",
        m.py().get_type_bound::<errors::TypraSchemaError>(),
    )?;
    m.add(
        "TypraValidationError",
        m.py().get_type_bound::<errors::TypraValidationError>(),
    )?;
    m.add(
        "TypraQueryError",
        m.py().get_type_bound::<errors::TypraQueryError>(),
    )?;
    m.add(
        "TypraTransactionError",
        m.py().get_type_bound::<errors::TypraTransactionError>(),
    )?;

    // Python model helpers (class-based schemas).
    let models_mod = PyModule::new_bound(m.py(), "models")?;
    models_mod.add_function(wrap_pyfunction!(models::collection, &models_mod)?)?;
    models_mod.add_function(wrap_pyfunction!(models::plan, &models_mod)?)?;
    models_mod.add_function(wrap_pyfunction!(models::apply, &models_mod)?)?;
    models_mod.add_function(wrap_pyfunction!(models::index, &models_mod)?)?;
    models_mod.add_function(wrap_pyfunction!(models::unique, &models_mod)?)?;
    models_mod.add_function(wrap_pyfunction!(models::constrained, &models_mod)?)?;
    models_mod.add_class::<models::ModelCollection>()?;
    models_mod.add_class::<models::ModelQuery>()?;
    models_mod.add_class::<models::IndexSpec>()?;
    models_mod.add_class::<models::ConstraintSpec>()?;
    models_mod.add_class::<models::FieldRef>()?;
    m.add_submodule(&models_mod)?;
    m.add("models", models_mod)?;

    // DB-API 2.0 (PEP 249) read-only adapter (0.10.0+).
    let dbapi_mod = PyModule::new_bound(m.py(), "dbapi")?;
    dbapi_mod.add_function(wrap_pyfunction!(dbapi::connect, &dbapi_mod)?)?;
    dbapi_mod.add_class::<dbapi::Connection>()?;
    dbapi_mod.add_class::<dbapi::Cursor>()?;
    m.add_submodule(&dbapi_mod)?;
    m.add("dbapi", dbapi_mod)?;
    Ok(())
}
