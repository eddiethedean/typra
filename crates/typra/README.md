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

Same program as **`examples/open.rs`** (`cargo run -p typra --example open`):

```rust
use std::borrow::Cow;

use typra::prelude::*;
use typra::schema::FieldPath;
use typra::FieldDef;
use typra::Type;

fn main() -> Result<(), DbError> {
    let mut db = Database::open_in_memory()?;
    println!("opened: {}", db.path().display());
    let (id, ver) = db.register_collection(
        "books",
        vec![FieldDef {
            path: FieldPath::new([Cow::Borrowed("title")])?,
            ty: Type::String,
        }],
        "title",
    )?;
    println!("registered collection id={} version={}", id.0, ver.0);
    Ok(())
}
```

Output:

```text
opened: :memory:
registered collection id=1 version=1
```

### Features

- `derive` (default): enables `#[derive(DbModel)]` via `typra-derive`.

### Lower-level crates

- `typra-core`: engine API
- `typra-derive`: proc-macro derives

