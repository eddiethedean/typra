# `typra` (Rust facade)

[![CI](https://github.com/eddiethedean/typra/actions/workflows/ci.yml/badge.svg)](https://github.com/eddiethedean/typra/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/typra.svg)](https://crates.io/crates/typra)
[![Docs](https://readthedocs.org/projects/typra/badge/?version=latest)](https://typra.readthedocs.io/en/latest/)

> **SQLite simplicity, with real types.**

Recommended Rust crate for Typra: typed embedded storage, schema catalog, validation, indexes, and queries.

**Documentation:** **[typra.readthedocs.io](https://typra.readthedocs.io/en/latest/)** · rustdoc: [docs.rs/typra](https://docs.rs/typra)

| | |
|--|--|
| [Quickstart](https://typra.readthedocs.io/en/latest/guides/quickstart/) | Install and first collection |
| [Rust API reference](https://typra.readthedocs.io/en/latest/reference/rust_api/) | Imports and stability |
| [Core concepts](https://typra.readthedocs.io/en/latest/guides/concepts/) | Mental model |
| [Compatibility](https://typra.readthedocs.io/en/latest/reference/compatibility/) · [Types](https://typra.readthedocs.io/en/latest/reference/types/) | Contracts |
| [Python bindings](https://typra.readthedocs.io/en/latest/guides/python/) | CPython package |

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

`DbModel` attributes and nested schemas: **[Quickstart](https://typra.readthedocs.io/en/latest/guides/quickstart/)** · **[Models & collections](https://typra.readthedocs.io/en/latest/guides/models_and_collections/)**

## Related crates

| Crate | When |
|-------|------|
| [`typra-core`](https://github.com/eddiethedean/typra/blob/main/crates/typra-core/README.md) | Engine-only or tooling |
| [`typra-derive`](https://github.com/eddiethedean/typra/blob/main/crates/typra-derive/README.md) | Direct proc-macro dependency |

## License

MIT — [LICENSE](https://github.com/eddiethedean/typra/blob/main/LICENSE)
