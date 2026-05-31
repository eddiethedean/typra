# `typra` (Rust facade)

[![CI](https://github.com/eddiethedean/typra/actions/workflows/ci.yml/badge.svg)](https://github.com/eddiethedean/typra/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/typra.svg)](https://crates.io/crates/typra)
[![Docs](https://readthedocs.org/projects/typra/badge/?version=latest)](https://typra.readthedocs.io/en/latest/?badge=latest)

> **SQLite simplicity, with real types.**

User-facing Rust crate for **Typra**: a typed, embedded, single-file database with schema catalog, validation, indexes, and typed queries.

| Resource | Link |
|----------|------|
| **Repository** | [github.com/eddiethedean/typra](https://github.com/eddiethedean/typra) |
| **Changelog** | [CHANGELOG.md](https://github.com/eddiethedean/typra/blob/main/CHANGELOG.md) |
| **Quickstart** | [docs/guides/quickstart.md](https://github.com/eddiethedean/typra/blob/main/docs/guides/quickstart.md) |
| **Concepts** | [docs/guides/concepts.md](https://github.com/eddiethedean/typra/blob/main/docs/guides/concepts.md) |
| **Python bindings** | [python/typra/README.md](https://github.com/eddiethedean/typra/blob/main/python/typra/README.md) |
| **Roadmap** | [ROADMAP.md](https://github.com/eddiethedean/typra/blob/main/ROADMAP.md) |

## What ships (v1.0.x)

- **`Database::open`** / **`open_in_memory`** / **`open_with_options`** with **`FileStore`** and **`VecStore`**
- **`register_collection`** / **`register_schema_version`** with validation, constraints, and **multi-segment field paths**
- **`insert` / `get` / `delete`** with **`RowValue`**, transactions, checkpoints, compaction, snapshots
- **Secondary indexes** and typed **queries** (equality, `And`, `Or`, ranges, `order_by`, `limit`, `explain`, **`query_iter`**, subset projections)
- **`#[derive(DbModel)]`** via default **`derive`** feature ([`typra-derive`](https://github.com/eddiethedean/typra/blob/main/crates/typra-derive/README.md))
- Optional **`async`** feature: **`AsyncDatabase`** (experimental wrapper)

SQL text is minimal (primarily for Python DB-API); prefer the typed query APIs in application code.

## Guarantees

- [Compatibility and recovery](https://github.com/eddiethedean/typra/blob/main/docs/reference/compatibility.md)
- [Types matrix](https://github.com/eddiethedean/typra/blob/main/docs/reference/types.md)
- [Operations and failure modes](https://github.com/eddiethedean/typra/blob/main/docs/ops/operations_and_failure_modes.md)

## Stability

- **`typra` 1.0.x** is the **recommended** stable entry point for Rust applications.
- **Feature flags** are additive: default features are safe; experimental features are documented separately.

## Install

```toml
[dependencies]
typra = "1.0"
```

Engine only (no proc-macros):

```toml
typra = { version = "1.0", default-features = false }
```

Optional async facade:

```toml
typra = { version = "1.0", features = ["async"] }
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

### `#[derive(DbModel)]`

| Attribute | Effect |
|-----------|--------|
| `#[db(primary)]` | Primary key |
| `#[db(unique)]` | Unique index |
| `#[db(index)]` | Non-unique index |
| `#[db(collection = "books")]` | Collection name override |

Nested paths and constraint attributes are not generated in 1.0 — use explicit **`FieldDef`** values or Python **`typra.models`**.

## Features

| Feature | Role |
|---------|------|
| **`derive`** (default) | `#[derive(DbModel)]` via **`typra-derive`** |
| **`async`** | **`AsyncDatabase`** via Tokio `spawn_blocking` (experimental) |

## Related crates

| Crate | Role | README |
|-------|------|--------|
| **`typra-core`** | Engine | [crates/typra-core/README.md](https://github.com/eddiethedean/typra/blob/main/crates/typra-core/README.md) |
| **`typra-derive`** | Proc-macros | [crates/typra-derive/README.md](https://github.com/eddiethedean/typra/blob/main/crates/typra-derive/README.md) |

Use **`typra-core`** directly when you need a minimal dependency graph or lower-level engine types.

## License

MIT — see [LICENSE](https://github.com/eddiethedean/typra/blob/main/LICENSE).
