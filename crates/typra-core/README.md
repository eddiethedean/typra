## `typra-core` (Rust)

[![CI](https://github.com/eddiethedean/typra/actions/workflows/ci.yml/badge.svg)](https://github.com/eddiethedean/typra/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/typra-core.svg)](https://crates.io/crates/typra-core)

Core engine crate for **Typra**, a typed embedded database.

**Status (0.4.0):** `Database::open`, persisted **schema catalog** (`register_collection` / `register_schema_version`), `DbError`. Record storage and queries are under development.

### Install

```toml
[dependencies]
typra-core = "0.4"
```

### Notes

- Most applications should depend on `typra` (the facade) instead of `typra-core`.

