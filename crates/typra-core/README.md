## `typra-core` (Rust)

[![CI](https://github.com/eddiethedean/typra/actions/workflows/ci.yml/badge.svg)](https://github.com/eddiethedean/typra/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/typra-core.svg)](https://crates.io/crates/typra-core)

Core engine crate for **Typra**, a typed embedded database.

**Status (0.3.0):** minimal API surface for establishing semver and wiring crates together (e.g. `Database::open`, `DbError`). The storage engine is under development.

### Install

```toml
[dependencies]
typra-core = "0.3"
```

### Notes

- Most applications should depend on `typra` (the facade) instead of `typra-core`.

