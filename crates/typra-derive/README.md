## `typra-derive` (Rust)

[![CI](https://github.com/eddiethedean/typra/actions/workflows/ci.yml/badge.svg)](https://github.com/eddiethedean/typra/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/typra-derive.svg)](https://crates.io/crates/typra-derive)

Proc-macro derives for **Typra** database models.

### Install

Most users should depend on `typra` (which enables this via the default `derive` feature). If you need the macro crate directly:

```toml
[dependencies]
typra-derive = "0.3"
typra-core = "0.3"
```

### Example

```rust
use typra_core::DbModel;

#[derive(DbModel)]
struct Book {
    title: String,
}
```

### Status (0.4.0)

This derive currently provides a minimal `DbModel` impl. Field attributes and enums are not implemented yet.

