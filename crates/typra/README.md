## `typra` (Rust)

User-facing facade crate for **Typra**, a typed embedded database.

**Status (0.2.0):** `Database::open` exists and `#[derive(DbModel)]` is available. Storage, queries, validation, and schema evolution are not implemented yet.

### Install

```toml
[dependencies]
typra = "0.2"
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

