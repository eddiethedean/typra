# `typra-core`

[![CI](https://github.com/eddiethedean/typra/actions/workflows/ci.yml/badge.svg)](https://github.com/eddiethedean/typra/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/typra-core.svg)](https://crates.io/crates/typra-core)

Core engine for **Typra**: typed, embedded storage with a persisted schema catalog and record payload encoding (v1 + v2).

## Status (v0.10.x)

`Database<S: Store>` with default on-disk **`FileStore`** and in-memory **`VecStore`**; replayed **schema catalog** (including **`primary_field`** and **constraints**); **`insert` / `get` / `delete`** with **`RowValue`** and validation; **secondary indexes** and typed **query** execution (`Eq` / `And` / `Or` / ranges, plus `limit`, `order_by`, `explain`), **`Database::query_iter`**, subset projections; snapshot bytes; **`DbError`** / **`ValidationError`**. Typra includes a minimal SQL parser (for Python DB-API use); most consumers should use the typed query AST directly.

| Resource | Link |
|----------|------|
| **Repository** | [github.com/eddiethedean/typra](https://github.com/eddiethedean/typra) |
| **Changelog** | [CHANGELOG.md](https://github.com/eddiethedean/typra/blob/main/CHANGELOG.md) |
| **Design / format** | [On-disk format](https://github.com/eddiethedean/typra/blob/main/docs/02_on_disk_file_format.md) · [Record v1](https://github.com/eddiethedean/typra/blob/main/docs/06_record_encoding_v1.md) · [Record v2](https://github.com/eddiethedean/typra/blob/main/docs/07_record_encoding_v2.md) · [Rust module layout](https://github.com/eddiethedean/typra/blob/main/docs/03_rust_crate_and_module_layout.md) |

## Install

```toml
[dependencies]
typra-core = "0.10"
typra-core = "0.10"
```

## Notes

Most applications should depend on **`typra`** (the facade) instead of **`typra-core`** directly. Use this crate when you want a minimal dependency tree or are building custom tooling on top of the engine.

## License

MIT — see [LICENSE](https://github.com/eddiethedean/typra/blob/main/LICENSE).
