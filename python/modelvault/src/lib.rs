// PyO3 `IntoPy` / `extract` patterns often trigger `useless_conversion`; keep noise down.
#![allow(clippy::useless_conversion)]
// `pyo3::create_exception!` expands to cfgs that trip `unexpected_cfgs` under `-D warnings`.
#![allow(unexpected_cfgs)]

mod async_database;
mod async_query;
mod async_util;
mod database;
mod db_handle;
mod dbapi;
mod errors;
mod fields_json;
mod inner_db;
mod models;
mod query;
mod row_values;

use pyo3::prelude::*;

#[pymodule]
fn modelvault(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add(
        "__doc__",
        "ModelVault Python bindings: typed embedded database built on the Rust engine.\n\n\
         Import ``Database`` (``open``, ``register_collection``, ``insert``, ``get``, …) or \
         ``AsyncDatabase`` for asyncio (``await AsyncDatabase.open(...)``). Reads on one handle \
         may run concurrently; writes and open transactions are exclusive. \
         The experimental ``modelvault.dbapi`` module provides a small read-only DB-API 2.0 \
         adapter (minimal SELECT subset). See the package README for ``fields_json`` and error mapping.",
    )?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add_class::<database::Database>()?;
    m.add_class::<database::PyTransaction>()?;
    m.add_class::<async_database::AsyncDatabase>()?;
    m.add_class::<async_database::AsyncTransaction>()?;
    m.add_class::<query::Collection>()?;
    m.add_class::<query::QueryBuilder>()?;
    m.add_class::<async_query::AsyncCollection>()?;
    m.add_class::<async_query::AsyncQueryBuilder>()?;

    // Stable error kinds via distinct exception subclasses (still `isinstance(..., ValueError)` etc).
    m.add(
        "ModelVaultFormatError",
        m.py().get_type::<errors::ModelVaultFormatError>(),
    )?;
    m.add(
        "ModelVaultSchemaError",
        m.py().get_type::<errors::ModelVaultSchemaError>(),
    )?;
    m.add(
        "ModelVaultValidationError",
        m.py().get_type::<errors::ModelVaultValidationError>(),
    )?;
    m.add(
        "ModelVaultQueryError",
        m.py().get_type::<errors::ModelVaultQueryError>(),
    )?;
    m.add(
        "ModelVaultTransactionError",
        m.py().get_type::<errors::ModelVaultTransactionError>(),
    )?;

    // Python model helpers (class-based schemas).
    let models_mod = PyModule::new(m.py(), "models")?;
    models_mod.add_function(wrap_pyfunction!(models::collection, &models_mod)?)?;
    models_mod.add_function(wrap_pyfunction!(models::async_collection, &models_mod)?)?;
    models_mod.add_function(wrap_pyfunction!(models::plan, &models_mod)?)?;
    models_mod.add_function(wrap_pyfunction!(models::apply, &models_mod)?)?;
    models_mod.add_function(wrap_pyfunction!(models::async_plan, &models_mod)?)?;
    models_mod.add_function(wrap_pyfunction!(models::async_apply, &models_mod)?)?;
    models_mod.add_function(wrap_pyfunction!(models::index, &models_mod)?)?;
    models_mod.add_function(wrap_pyfunction!(models::unique, &models_mod)?)?;
    models_mod.add_function(wrap_pyfunction!(models::constrained, &models_mod)?)?;
    models_mod.add_class::<models::ModelCollection>()?;
    models_mod.add_class::<models::ModelQuery>()?;
    models_mod.add_class::<models::AsyncModelCollection>()?;
    models_mod.add_class::<models::AsyncModelQuery>()?;
    models_mod.add_class::<models::IndexSpec>()?;
    models_mod.add_class::<models::ConstraintSpec>()?;
    models_mod.add_class::<models::FieldRef>()?;
    m.add_submodule(&models_mod)?;
    m.add("models", models_mod)?;

    // DB-API 2.0 (PEP 249) read-only adapter (0.10.0+).
    let dbapi_mod = PyModule::new(m.py(), "dbapi")?;
    dbapi_mod.add_function(wrap_pyfunction!(dbapi::connect, &dbapi_mod)?)?;
    dbapi_mod.add_class::<dbapi::Connection>()?;
    dbapi_mod.add_class::<dbapi::Cursor>()?;
    m.add_submodule(&dbapi_mod)?;
    m.add("dbapi", dbapi_mod)?;
    Ok(())
}
