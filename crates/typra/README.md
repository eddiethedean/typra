## `typra` (Rust)

[![CI](https://github.com/eddiethedean/typra/actions/workflows/ci.yml/badge.svg)](https://github.com/eddiethedean/typra/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/typra.svg)](https://crates.io/crates/typra)

User-facing facade crate for **Typra**, a typed embedded database.

**Status (0.5.0):** `Database::open`, **`register_collection`** (with primary key field), **`insert` / `get`**, **`Database::open_in_memory`** / snapshots, **`register_schema_version`**, and `#[derive(DbModel)]` are available. Queries, rich validation, and secondary indexes are not implemented yet.

### Install

```toml
[dependencies]
typra = "0.5"
```

### Example

```rust
use std::borrow::Cow;

use typra::prelude::*;
use typra::schema::FieldPath;
use typra::DbModel;
use typra::FieldDef;
use typra::Type;

#[derive(DbModel)]
struct Book {
    title: String,
}

fn main() -> Result<(), DbError> {
    let mut db = Database::open("example.typra")?;
    let _ = db.register_collection(
        "books",
        vec![FieldDef {
            path: FieldPath::new([Cow::Borrowed("title")])?,
            ty: Type::String,
        }],
        "title",
    )?;
    Ok(())
}
```

### Features

- `derive` (default): enables `#[derive(DbModel)]` via `typra-derive`.

### Lower-level crates

- `typra-core`: engine API
- `typra-derive`: proc-macro derives

