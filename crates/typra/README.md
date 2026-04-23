# `typra` (Rust facade)

[![CI](https://github.com/eddiethedean/typra/actions/workflows/ci.yml/badge.svg)](https://github.com/eddiethedean/typra/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/typra.svg)](https://crates.io/crates/typra)

User-facing crate for **Typra**: a typed, embedded database (single file, append-only segments, schema catalog, record insert/get, secondary indexes, minimal typed queries).

## Status (v0.7.x)

`Database::open`, **`register_collection` / `register_schema_version`** (with **`primary_field`** on create), **`insert` / `get`** with **`RowValue`** and validation/constraints, **`Database::open_in_memory`**, snapshot import/export, **`#[derive(DbModel)]`** (via the default `derive` feature), **secondary indexes**, minimal **query** execution (**equality**, **`limit`**, **`explain`**), **`Database::query_iter`**, and **subset projections**. SQL text and DB-API layers are **not** implemented yet.

| Resource | Link |
|----------|------|
| **Repository** | [github.com/eddiethedean/typra](https://github.com/eddiethedean/typra) |
| **Changelog** | [CHANGELOG.md](https://github.com/eddiethedean/typra/blob/main/CHANGELOG.md) |
| **User guides** | [Getting started](https://github.com/eddiethedean/typra/blob/main/docs/guide_getting_started.md) · [Concepts](https://github.com/eddiethedean/typra/blob/main/docs/guide_concepts.md) · [Python](https://github.com/eddiethedean/typra/blob/main/docs/guide_python.md) · [Rust module layout](https://github.com/eddiethedean/typra/blob/main/docs/03_rust_crate_and_module_layout.md) · [Roadmap](https://github.com/eddiethedean/typra/blob/main/ROADMAP.md) |

## Install

```toml
[dependencies]
typra = "0.7"
```

Disable the default `derive` feature if you only need the engine:

```toml
typra = { version = "0.7", default-features = false }
```

## Example

Same program as **`examples/open.rs`**:

```bash
cargo run -p typra --example open
```

```rust
use std::borrow::Cow;

use typra::prelude::*;
use typra::schema::FieldPath;
use typra::FieldDef;
use typra::Type;

fn main() -> Result<(), DbError> {
    // Setup: in-memory database (no file on disk).
    let mut db = Database::open_in_memory()?;
    println!("opened: {}", db.path().display());
    // Example: register a `books` collection with a string primary key `title`.
    let (id, ver) = db.register_collection(
        "books",
        vec![FieldDef {
            path: FieldPath::new([Cow::Borrowed("title")])?,
            ty: Type::String,
            constraints: vec![],
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

Field attributes (`#[db(primary)]`, etc.) on `DbModel` are **not** implemented yet.

## Features

| Feature | Role |
|---------|------|
| **`derive`** (default) | `#[derive(DbModel)]` via **`typra-derive`** |

## Related crates

| Crate | Role |
|-------|------|
| **`typra-core`** | Engine (`Database`, storage, catalog, records) |
| **`typra-derive`** | Proc-macros |

## License

MIT — see [LICENSE](https://github.com/eddiethedean/typra/blob/main/LICENSE).
