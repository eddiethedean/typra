# `typra-derive`

[![CI](https://github.com/eddiethedean/typra/actions/workflows/ci.yml/badge.svg)](https://github.com/eddiethedean/typra/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/typra-derive.svg)](https://crates.io/crates/typra-derive)

Proc-macro crate for **Typra** (`#[derive(DbModel)]`).

## Status (v1.0.x)

The derive emits **`DbModel`** for structs with named fields. Supported attributes:

- `#[db(primary)]` — mark the primary key field (required)
- `#[db(unique)]` — unique secondary index on the field
- `#[db(index)]` — non-unique secondary index
- `#[db(collection = "books")]` — override the default collection name

**Limitations:** top-level scalar/list fields only; nested `FieldPath`s and constraint metadata are not generated (use manual `FieldDef` registration or Python `typra.models`).

| Resource | Link |
|----------|------|
| **Repository** | [github.com/eddiethedean/typra](https://github.com/eddiethedean/typra) |
| **Changelog** | [CHANGELOG.md](https://github.com/eddiethedean/typra/blob/main/CHANGELOG.md) |
| **Facade crate** | [`typra` on crates.io](https://crates.io/crates/typra) |

## Install

Most users should depend on **`typra`** (default `derive` feature). To depend on this crate directly:

```toml
[dependencies]
typra-derive = "1.0"
typra-core = "1.0"
```

## Example

```rust
use typra_derive::DbModel;

#[derive(DbModel)]
struct Book {
    #[db(primary)]
    title: String,
    #[db(index)]
    year: i64,
}
```

Use **`typra_core::DbModel`** as a trait bound when you need the marker trait explicitly.

## License

MIT — see [LICENSE](https://github.com/eddiethedean/typra/blob/main/LICENSE).
