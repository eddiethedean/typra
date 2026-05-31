# Typra

[![CI](https://github.com/eddiethedean/typra/actions/workflows/ci.yml/badge.svg)](https://github.com/eddiethedean/typra/actions/workflows/ci.yml)
[![Docs](https://readthedocs.org/projects/typra/badge/?version=latest)](https://typra.readthedocs.io/en/latest/)
[![crates.io](https://img.shields.io/crates/v/typra.svg)](https://crates.io/crates/typra)
[![PyPI](https://img.shields.io/pypi/v/typra.svg)](https://pypi.org/project/typra/)

> **SQLite simplicity, with real types.**

Typra is a **typed, embedded database** for application data: one file, strict schemas, validation on write, and nested objects as first-class citizens. Same engine in **Rust** and **Python**.

**Documentation:** **[typra.readthedocs.io](https://typra.readthedocs.io/en/latest/)**

| | |
|--|--|
| [Quickstart](https://typra.readthedocs.io/en/latest/guides/quickstart/) | Install and first insert in minutes |
| [Python guide](https://typra.readthedocs.io/en/latest/guides/python/) | `typra.models`, queries, DB-API |
| [Core concepts](https://typra.readthedocs.io/en/latest/guides/concepts/) | Mental model |
| [Compatibility](https://typra.readthedocs.io/en/latest/reference/compatibility/) · [Types](https://typra.readthedocs.io/en/latest/reference/types/) · [Security](https://typra.readthedocs.io/en/latest/reference/security/) | Production contracts |
| [Operations runbook](https://typra.readthedocs.io/en/latest/ops/operations_and_failure_modes/) | Backup, recovery, locking |
| [Contributing](https://typra.readthedocs.io/en/latest/dev/contributing_guide/) | Dev setup and release |
| [Changelog](https://github.com/eddiethedean/typra/blob/main/CHANGELOG.md) · [Roadmap](https://github.com/eddiethedean/typra/blob/main/ROADMAP.md) | Release notes and plans |

## Install

```bash
pip install "typra>=1.0.0,<2"
```

```toml
[dependencies]
typra = "1.0"
```

Full install notes and examples: **[Quickstart](https://typra.readthedocs.io/en/latest/guides/quickstart/)**.

## Python (30 seconds)

Recommended path: **`typra.models`** with dataclasses or Pydantic.

```python
# Setup: class-defined schema + in-memory DB.
from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, Optional

import typra


@dataclass
class Book:
    __typra_primary_key__ = "title"
    __typra_indexes__ = [
        typra.models.index("year"),
        typra.models.unique("title"),
    ]

    title: str
    year: Annotated[int, typra.models.constrained(min_i64=0)]
    rating: Optional[float] = None


db = typra.Database.open_in_memory()
books = typra.models.collection(db, Book)

books.insert(Book(title="Hello", year=2020, rating=4.5))
print(books.get("Hello"))
print(typra.__version__)
```

Output:

```text
Book(title='Hello', year=2020, rating=4.5)
1.0.0
```

More: **[Python guide](https://typra.readthedocs.io/en/latest/guides/python/)** · **[Models & collections](https://typra.readthedocs.io/en/latest/guides/models_and_collections/)** · PyPI package notes: [python/typra/README.md](https://github.com/eddiethedean/typra/blob/main/python/typra/README.md)

## Rust (30 seconds)

Use the **`typra`** facade crate. From this repo: `cargo run -p typra --example open`.

```rust
use std::borrow::Cow;
use typra::prelude::*;
use typra::FieldDef;
use typra::Type;
use typra::schema::FieldPath;

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

More: **[Rust API reference](https://typra.readthedocs.io/en/latest/reference/rust_api/)** · **[Quickstart](https://typra.readthedocs.io/en/latest/guides/quickstart/)** · crate README: [crates/typra/README.md](https://github.com/eddiethedean/typra/blob/main/crates/typra/README.md)

## Repository layout

| Path | Role |
|------|------|
| **`crates/`** | Rust crates ([`typra`](https://github.com/eddiethedean/typra/blob/main/crates/typra/README.md), [`typra-core`](https://github.com/eddiethedean/typra/blob/main/crates/typra-core/README.md), [`typra-derive`](https://github.com/eddiethedean/typra/blob/main/crates/typra-derive/README.md)) |
| **`python/`** | PyPI packaging ([python/README.md](https://github.com/eddiethedean/typra/blob/main/python/README.md)) |
| **`docs/`** | Source for [typra.readthedocs.io](https://typra.readthedocs.io/en/latest/) |

Local checks: `make check-full` · 1.0 gate: `make check-1p0-ready`

## License

MIT — [LICENSE](https://github.com/eddiethedean/typra/blob/main/LICENSE)
