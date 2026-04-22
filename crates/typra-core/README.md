## `typra-core` (Rust)

[![CI](https://github.com/eddiethedean/typra/actions/workflows/ci.yml/badge.svg)](https://github.com/eddiethedean/typra/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/typra-core.svg)](https://crates.io/crates/typra-core)

Core engine crate for **Typra**, a typed embedded database.

**Status (0.5.0):** `Database` over **`Store`** (default on-disk `FileStore`, **`VecStore`** in-memory); persisted **schema catalog** with optional **`primary_field`**; **`insert` / `get`** (record payload v1); snapshots; `DbError`. Secondary indexes and a query engine are not implemented yet.

### Install

```toml
[dependencies]
typra-core = "0.5"
```

### Notes

- Most applications should depend on `typra` (the facade) instead of `typra-core`.

