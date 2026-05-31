# `typra` (Rust facade)

[![CI](https://github.com/eddiethedean/typra/actions/workflows/ci.yml/badge.svg)](https://github.com/eddiethedean/typra/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/typra.svg)](https://crates.io/crates/typra)
[![Docs](https://readthedocs.org/projects/typra/badge/?version=latest)](https://typra.readthedocs.io/en/latest/)

## Application-focused embedded database

**SQLite simplicity, with real types.** The recommended Rust crate for Typra — **the database for application models**.

- **Model-driven schemas** — collections from field definitions or `#[derive(DbModel)]`
- **Validation on write** — types and constraints enforced before persistence
- **Migrations** — schema catalog versioning and compatibility helpers
- **Nested objects** — typed multi-segment field paths
- **Single-file deploy** — on-disk `.typra` or in-memory for tests

**Documentation:** **[typra.readthedocs.io](https://typra.readthedocs.io/en/latest/)** · rustdoc: [docs.rs/typra](https://docs.rs/typra)

| | |
|--|--|
| [Why Typra](https://typra.readthedocs.io/en/latest/guides/why_typra/) | When to choose Typra |
| [Quickstart](https://typra.readthedocs.io/en/latest/guides/quickstart/) | First collection |
| [Comparisons](https://typra.readthedocs.io/en/latest/comparisons/) | vs SQLite, JSON, DuckDB |
| [Python bindings](https://typra.readthedocs.io/en/latest/guides/python/) | Pydantic / dataclass path |

## Install

```toml
[dependencies]
typra = "1.0"
```

| Variant | Dependency |
|---------|------------|
| Engine only (no macros) | `typra = { version = "1.0", default-features = false }` |
| Experimental async | `typra = { version = "1.0", features = ["async"] }` |

Features: **`derive`** (default, `#[derive(DbModel)]`) · **`async`** (`AsyncDatabase`, experimental) — see [async policy](https://typra.readthedocs.io/en/latest/reference/async_policy/).

## Example

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
    let mut db = Database::open_in_memory()?;
    println!("opened: {}", db.path().display());
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

`DbModel` and nested schemas: **[Models & collections](https://typra.readthedocs.io/en/latest/guides/models_and_collections/)**

## Related crates

| Crate | When |
|-------|------|
| [`typra-core`](https://github.com/eddiethedean/typra/blob/main/crates/typra-core/README.md) | Engine-only or tooling |
| [`typra-derive`](https://github.com/eddiethedean/typra/blob/main/crates/typra-derive/README.md) | Direct proc-macro dependency |

## License

MIT — [LICENSE](https://github.com/eddiethedean/typra/blob/main/LICENSE)
