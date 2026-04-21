## `typra` (Rust)

[![CI](https://github.com/eddiethedean/typra/actions/workflows/ci.yml/badge.svg)](https://github.com/eddiethedean/typra/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/typra.svg)](https://crates.io/crates/typra)

User-facing facade crate for **Typra**, a typed embedded database.

**Status (0.4.0):** `Database::open`, **`register_collection`**, **`register_schema_version`**, and `#[derive(DbModel)]` are available. Record storage, queries, validation, and schema evolution beyond catalog registration are not implemented yet.

### Install

```toml
[dependencies]
typra = "0.3"
```

### Example

```rust
use typra::prelude::*;
use typra::DbModel;

#[derive(DbModel)]
struct Book {
    title: String,
}

fn main() -> Result<(), DbError> {
    let _db = Database::open("example.typra")?;
    Ok(())
}
```

### Features

- `derive` (default): enables `#[derive(DbModel)]` via `typra-derive`.

### Lower-level crates

- `typra-core`: engine API
- `typra-derive`: proc-macro derives

