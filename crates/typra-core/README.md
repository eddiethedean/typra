# `typra-core`

[![CI](https://github.com/eddiethedean/typra/actions/workflows/ci.yml/badge.svg)](https://github.com/eddiethedean/typra/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/typra-core.svg)](https://crates.io/crates/typra-core)

Core engine for **Typra**: typed, embedded storage with a persisted schema catalog and record payload encoding (v1 + v2 + v3).

## Status (v1.0.x)

`Database<S: Store>` with default on-disk **`FileStore`** and in-memory **`VecStore`**; replayed **schema catalog** (including **`primary_field`**, **constraints**, and **multi-segment field paths**); **`insert` / `get` / `delete`** with **`RowValue`** and validation; **secondary indexes** and typed **query** execution (`Eq` / `And` / `Or` / ranges, plus `limit`, `order_by`, `explain`), **`Database::query_iter`**, subset projections; transactions, checkpoints, compaction, snapshot bytes; **`DbError`** / **`ValidationError`**. Typra includes a minimal SQL parser (for Python DB-API use); most consumers should use the typed query AST directly.

## Stability and feature policy

- Most applications should depend on **`typra`** (the facade) instead of **`typra-core`** directly.
- **`typra-core` 1.0.x** is **stable and safe to depend on directly**.
  - Crate-root exports (e.g. `Database`, schema types, and error types) are treated as the stable surface.
  - Module-level APIs under `typra_core::*` are also public today; treat them as stable unless explicitly marked otherwise in docs.
- **Feature flags** are intended to be **additive**.

| Resource | Link |
|----------|------|
| **Repository** | [github.com/eddiethedean/typra](https://github.com/eddiethedean/typra) |
| **Changelog** | [CHANGELOG.md](https://github.com/eddiethedean/typra/blob/main/CHANGELOG.md) |
| **Design / format** | [On-disk format](https://github.com/eddiethedean/typra/blob/main/docs/specs/on_disk_file_format.md) · [Record v3](https://github.com/eddiethedean/typra/blob/main/docs/specs/record_encoding_v3.md) · [Rust module layout](https://github.com/eddiethedean/typra/blob/main/docs/specs/rust_crate_layout.md) |

## Install

```toml
[dependencies]
typra-core = "1.0"
```

## Notes

Most applications should depend on **`typra`** (the facade) instead of **`typra-core`** directly. Use this crate when you want a minimal dependency tree or are building custom tooling on top of the engine.

## License

MIT — see [LICENSE](https://github.com/eddiethedean/typra/blob/main/LICENSE).
