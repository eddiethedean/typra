# `modelvault` (Rust facade)

[![CI](https://github.com/eddiethedean/modelvault/actions/workflows/ci.yml/badge.svg)](https://github.com/eddiethedean/modelvault/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/modelvault.svg)](https://crates.io/crates/modelvault)
[![Docs](https://readthedocs.org/projects/modelvault/badge/?version=latest)](https://modelvault.readthedocs.io/en/latest/)

## Application-focused embedded database

**Schema-first typed storage for application models.** The recommended Rust crate for ModelVault — **the database for application models**.

- **Model-driven schemas** — collections from field definitions or `#[derive(DbModel)]`
- **Validation on write** — types and constraints enforced before persistence
- **Migrations** — schema catalog versioning and compatibility helpers
- **Nested objects** — typed multi-segment field paths
- **Single-file deploy** — on-disk `.modelvault` or in-memory for tests

**Documentation:** **[modelvault.readthedocs.io](https://modelvault.readthedocs.io/en/latest/)** · rustdoc: [docs.rs/modelvault](https://docs.rs/modelvault)

| | |
|--|--|
| [Why ModelVault](https://modelvault.readthedocs.io/en/latest/guides/why_modelvault/) | When to choose ModelVault |
| [Quickstart](https://modelvault.readthedocs.io/en/latest/guides/quickstart/) | First collection |
| [Why ModelVault](https://modelvault.readthedocs.io/en/latest/guides/why_modelvault/) | Positioning and design goals |
| [Python bindings](https://modelvault.readthedocs.io/en/latest/guides/python/) | Pydantic / dataclass path |

## Install

```toml
[dependencies]
modelvault = "0.16"
```

| Variant | Dependency |
|---------|------------|
| Engine only (no macros) | `modelvault = { version = "0.16", default-features = false }` |
| Experimental async | `modelvault = { version = "0.16", features = ["async"] }` |

Features: **`derive`** (default, `#[derive(DbModel)]`) · **`async`** (`AsyncDatabase` on a thread pool with **concurrent reads** and exclusive writes — experimental) — see [async policy](https://modelvault.readthedocs.io/en/latest/reference/async_policy/).

## Example

```bash
cargo run -p modelvault --example open
```

```rust
use std::borrow::Cow;

use modelvault::prelude::*;
use modelvault::schema::FieldPath;
use modelvault::FieldDef;
use modelvault::Type;

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

`DbModel` and nested schemas: **[Models & collections](https://modelvault.readthedocs.io/en/latest/guides/models_and_collections/)**

## Related crates

| Crate | When |
|-------|------|
| [`modelvault-core`](https://github.com/eddiethedean/modelvault/blob/main/crates/modelvault-core/README.md) | Engine-only or tooling |
| [`modelvault-derive`](https://github.com/eddiethedean/modelvault/blob/main/crates/modelvault-derive/README.md) | Direct proc-macro dependency |

## License

MIT — [LICENSE](https://github.com/eddiethedean/modelvault/blob/main/LICENSE)
