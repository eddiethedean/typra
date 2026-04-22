# `typra-derive`

[![CI](https://github.com/eddiethedean/typra/actions/workflows/ci.yml/badge.svg)](https://github.com/eddiethedean/typra/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/typra-derive.svg)](https://crates.io/crates/typra-derive)

Proc-macro crate for **Typra** (`#[derive(DbModel)]`).

## Status (v0.5.x)

The derive emits a minimal **`DbModel`** implementation. Field attributes, enums, and richer mapping are **not** implemented yet.

| Resource | Link |
|----------|------|
| **Repository** | [github.com/eddiethedean/typra](https://github.com/eddiethedean/typra) |
| **Changelog** | [CHANGELOG.md](https://github.com/eddiethedean/typra/blob/main/CHANGELOG.md) |
| **Facade crate** | [`typra` on crates.io](https://crates.io/crates/typra) |

## Install

Most users should depend on **`typra`** (default `derive` feature). To depend on this crate directly:

```toml
[dependencies]
typra-derive = "0.5"
typra-core = "0.5"
```

## Example

```rust
use typra_derive::DbModel;

#[derive(DbModel)]
struct Book {
    title: String,
}
```

Use **`typra_core::DbModel`** as a trait bound when you need the marker trait explicitly.

## License

MIT — see [LICENSE](https://github.com/eddiethedean/typra/blob/main/LICENSE).
