# `typra-core`

[![CI](https://github.com/eddiethedean/typra/actions/workflows/ci.yml/badge.svg)](https://github.com/eddiethedean/typra/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/typra-core.svg)](https://crates.io/crates/typra-core)

Core engine for **Typra**: typed, embedded storage with a persisted schema catalog and record payload encoding (v1 + v2).

## Status (v0.6.x)

`Database<S: Store>` with default on-disk **`FileStore`** and in-memory **`VecStore`**; replayed **schema catalog** (including **`primary_field`** and **constraints**); **`insert` / `get`** with **`RowValue`** and validation; snapshot bytes; **`DbError`** / **`ValidationError`**. Secondary indexes and a query engine are **not** implemented yet.

| Resource | Link |
|----------|------|
| **Repository** | [github.com/eddiethedean/typra](https://github.com/eddiethedean/typra) |
| **Changelog** | [CHANGELOG.md](https://github.com/eddiethedean/typra/blob/main/CHANGELOG.md) |
| **Design / format** | [On-disk format](https://github.com/eddiethedean/typra/blob/main/docs/02_on_disk_file_format.md) · [Record v1](https://github.com/eddiethedean/typra/blob/main/docs/06_record_encoding_v1.md) · [Record v2](https://github.com/eddiethedean/typra/blob/main/docs/07_record_encoding_v2.md) · [Rust module layout](https://github.com/eddiethedean/typra/blob/main/docs/03_rust_crate_and_module_layout.md) |

## Install

```toml
[dependencies]
typra-core = "0.6"
```

## Notes

Most applications should depend on **`typra`** (the facade) instead of **`typra-core`** directly. Use this crate when you want a minimal dependency tree or are building custom tooling on top of the engine.

## License

MIT — see [LICENSE](https://github.com/eddiethedean/typra/blob/main/LICENSE).
