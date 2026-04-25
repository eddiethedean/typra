# `typra` (Rust facade)

[![CI](https://github.com/eddiethedean/typra/actions/workflows/ci.yml/badge.svg)](https://github.com/eddiethedean/typra/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/typra.svg)](https://crates.io/crates/typra)

User-facing crate for **Typra**: a typed, embedded database (single file, append-only segments, schema catalog, record insert/get, secondary indexes, minimal typed queries).

## Status (v1.0.x)

`Database::open`, **`register_collection` / `register_schema_version`** (with **`primary_field`** on create), **`insert` / `get` / `delete`** with **`RowValue`** and validation/constraints, **`Database::open_in_memory`**, snapshot import/export, **`#[derive(DbModel)]`** (via the default `derive` feature), **secondary indexes**, typed **query** execution (**equality**, `And`, `Or`, ranges, `limit`, `order_by`, `explain`), **`Database::query_iter`**, and **subset projections**. Typra’s **SQL text** surface remains minimal (primarily to support Python DB-API); applications should prefer the typed query builder APIs.

For guarantees and operational behavior, see the repo docs:

- [`docs/reference/compatibility.md`](../../docs/reference/compatibility.md)
- [`docs/reference/types.md`](../../docs/reference/types.md)
- [`docs/ops/operations_and_failure_modes.md`](../../docs/ops/operations_and_failure_modes.md)

## Stability and feature policy

- **Prefer this crate** (`typra`) in applications. It is the stable facade for Typra’s Rust ecosystem.
- **Feature flags** are intended to be **additive**:
  - Default features should be safe for most users.
  - Experimental features should be clearly labeled in docs and may change faster than the default surface.

| Resource | Link |
|----------|------|
| **Repository** | [github.com/eddiethedean/typra](https://github.com/eddiethedean/typra) |
| **Changelog** | [CHANGELOG.md](https://github.com/eddiethedean/typra/blob/main/CHANGELOG.md) |
| **User guides** | [Quickstart](https://github.com/eddiethedean/typra/blob/main/docs/guides/quickstart.md) · [Concepts](https://github.com/eddiethedean/typra/blob/main/docs/guides/concepts.md) · [Python](https://github.com/eddiethedean/typra/blob/main/docs/guides/python.md) · [Operations](https://github.com/eddiethedean/typra/blob/main/docs/ops/operations_and_failure_modes.md) · [Roadmap](https://github.com/eddiethedean/typra/blob/main/ROADMAP.md) |

## Install

```toml
[dependencies]
typra = "1.0"
```

Disable the default `derive` feature if you only need the engine:

```toml
typra = { version = "1.0", default-features = false }
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
| **`async`** | Async wrapper API (`AsyncDatabase`) implemented via Tokio `spawn_blocking` |

## When to use `typra-core` directly

Use `typra-core` instead of `typra` if you need:

- a minimal dependency graph (no proc-macros, no facade re-exports)
- access to lower-level engine types that the facade intentionally doesn’t surface

## Related crates

| Crate | Role |
|-------|------|
| **`typra-core`** | Engine (`Database`, storage, catalog, records) |
| **`typra-derive`** | Proc-macros |

## License

MIT — see [LICENSE](https://github.com/eddiethedean/typra/blob/main/LICENSE).
