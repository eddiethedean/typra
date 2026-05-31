# `typra-derive`

[![CI](https://github.com/eddiethedean/typra/actions/workflows/ci.yml/badge.svg)](https://github.com/eddiethedean/typra/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/typra-derive.svg)](https://crates.io/crates/typra-derive)
[![Docs](https://readthedocs.org/projects/typra/badge/?version=latest)](https://typra.readthedocs.io/en/latest/)

Proc-macros for Typra: **`#[derive(DbModel)]`** — map Rust structs to Typra collections with validation and indexes.

Most users should depend on **[`typra`](https://github.com/eddiethedean/typra/blob/main/crates/typra/README.md)** with the default **`derive`** feature.

**Documentation:** **[Quickstart](https://typra.readthedocs.io/en/latest/guides/quickstart/)** · **[Models & collections](https://typra.readthedocs.io/en/latest/guides/models_and_collections/)**

## Install

Via facade (recommended):

```toml
[dependencies]
typra = "1.0"
```

Direct:

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

Attributes and nested paths: **[Models & collections](https://typra.readthedocs.io/en/latest/guides/models_and_collections/)**.

Runnable demo: `cargo run -p typra --example open`

## License

MIT — [LICENSE](https://github.com/eddiethedean/typra/blob/main/LICENSE)
