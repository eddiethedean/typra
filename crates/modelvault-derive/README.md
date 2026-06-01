# `modelvault-derive`

[![CI](https://github.com/eddiethedean/modelvault/actions/workflows/ci.yml/badge.svg)](https://github.com/eddiethedean/modelvault/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/modelvault-derive.svg)](https://crates.io/crates/modelvault-derive)
[![Docs](https://readthedocs.org/projects/modelvault/badge/?version=latest)](https://modelvault.readthedocs.io/en/latest/)

Proc-macros for ModelVault: **`#[derive(DbModel)]`** — map Rust structs to ModelVault collections with validation and indexes.

Most users should depend on **[`modelvault`](https://github.com/eddiethedean/modelvault/blob/main/crates/modelvault/README.md)** with the default **`derive`** feature.

**Documentation:** **[Quickstart](https://modelvault.readthedocs.io/en/latest/guides/quickstart/)** · **[Models & collections](https://modelvault.readthedocs.io/en/latest/guides/models_and_collections/)**

## Install

Via facade (recommended):

```toml
[dependencies]
modelvault = "0.15"
```

Direct:

```toml
[dependencies]
modelvault-derive = "0.15"
modelvault-core = "0.15"
```

## Example

```rust
use modelvault_derive::DbModel;

#[derive(DbModel)]
struct Book {
    #[db(primary)]
    title: String,
    #[db(index)]
    year: i64,
}
```

Attributes and nested paths: **[Models & collections](https://modelvault.readthedocs.io/en/latest/guides/models_and_collections/)**.

Runnable demo: `cargo run -p modelvault --example open`

## License

MIT — [LICENSE](https://github.com/eddiethedean/modelvault/blob/main/LICENSE)
