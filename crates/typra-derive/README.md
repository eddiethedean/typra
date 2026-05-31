# `typra-derive`

[![CI](https://github.com/eddiethedean/typra/actions/workflows/ci.yml/badge.svg)](https://github.com/eddiethedean/typra/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/typra-derive.svg)](https://crates.io/crates/typra-derive)
[![Docs](https://readthedocs.org/projects/typra/badge/?version=latest)](https://typra.readthedocs.io/en/latest/?badge=latest)

Proc-macro crate for **Typra**: **`#[derive(DbModel)]`**.

Most users should depend on **[`typra`](https://github.com/eddiethedean/typra/blob/main/crates/typra/README.md)** with the default **`derive`** feature instead of this crate directly.

| Resource | Link |
|----------|------|
| **Repository** | [github.com/eddiethedean/typra](https://github.com/eddiethedean/typra) |
| **Facade crate** | [`typra` on crates.io](https://crates.io/crates/typra) |
| **Changelog** | [CHANGELOG.md](https://github.com/eddiethedean/typra/blob/main/CHANGELOG.md) |
| **Quickstart** | [docs/guides/quickstart.md](https://github.com/eddiethedean/typra/blob/main/docs/guides/quickstart.md) |

## What ships (v1.0.x)

The derive emits **`DbModel`** for structs with named fields.

| Attribute | Effect |
|-----------|--------|
| `#[db(primary)]` | Primary key (required) |
| `#[db(unique)]` | Unique secondary index |
| `#[db(index)]` | Non-unique secondary index |
| `#[db(collection = "books")]` | Override collection name |

**Limitations (1.0):** top-level scalar/list fields only. Nested **`FieldPath`s** and constraint metadata are not generated — use manual **`FieldDef`** registration or Python **`typra.models`**.

## Install

Via the facade (recommended):

```toml
[dependencies]
typra = "1.0"
```

Direct dependency:

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

Use **`typra_core::DbModel`** (or **`typra::DbModel`**) as a trait bound when you need the marker explicitly.

Runnable facade example with output verification: **`cargo run -p typra --example open`** — see [crates/typra/README.md](https://github.com/eddiethedean/typra/blob/main/crates/typra/README.md).

## License

MIT — see [LICENSE](https://github.com/eddiethedean/typra/blob/main/LICENSE).
