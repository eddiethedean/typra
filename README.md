# Typra

[![CI](https://github.com/eddiethedean/typra/actions/workflows/ci.yml/badge.svg)](https://github.com/eddiethedean/typra/actions/workflows/ci.yml)
[![Docs](https://readthedocs.org/projects/typra/badge/?version=latest)](https://typra.readthedocs.io/en/latest/?badge=latest)
[![crates.io](https://img.shields.io/crates/v/typra.svg)](https://crates.io/crates/typra)
[![PyPI](https://img.shields.io/pypi/v/typra.svg)](https://pypi.org/project/typra/)

> **SQLite simplicity, with real types.**

Typra is a **typed, embedded database** for application data.  
It combines the ease of SQLite with **strict schemas, validation, and nested data support**—so your data is modeled explicitly end to end.

| Resource | Link |
|----------|------|
| **Documentation** | [typra.readthedocs.io](https://typra.readthedocs.io/en/latest/) |
| **Changelog** | [CHANGELOG.md](https://github.com/eddiethedean/typra/blob/main/CHANGELOG.md) |
| **Roadmap** | [ROADMAP.md](https://github.com/eddiethedean/typra/blob/main/ROADMAP.md) |
| **Contributing** | [docs/contributing.md](https://github.com/eddiethedean/typra/blob/main/docs/contributing.md) |

## What ships (v1.0.x)

- **Typed schemas + validation on write** (constraints, nested objects/lists, multi-segment field paths)
- **Single-file durability** with transactions, recovery modes, checkpoints, and compaction
- **Secondary indexes** (unique + non-unique) and **typed queries** (equality/AND/OR/ranges/order_by/limit)
- **Rust facade** ([`typra`](https://github.com/eddiethedean/typra/blob/main/crates/typra/README.md)) with optional `#[derive(DbModel)]`
- **Python package** ([`typra`](https://github.com/eddiethedean/typra/blob/main/python/typra/README.md)) with **`typra.models`** (recommended), `fields_json`, and a minimal read-only DB-API adapter
- **Operational CLI** (`typra inspect`, `verify`, `backup`, `compact`, migrations) — see [CLI reference](https://github.com/eddiethedean/typra/blob/main/docs/reference/cli.md)

### Non-goals (for now)

- Full SQL / SQLAlchemy (phased post-1.0 — see [ROADMAP](https://github.com/eddiethedean/typra/blob/main/ROADMAP.md#post-10-isoiec-9075-sql-track); today DB-API is a minimal read-only `SELECT` subset)
- General-purpose OLAP engine features (joins/group-by SQL at scale, etc.)

## Guarantees and contracts

- **Compatibility and recovery**: [docs/reference/compatibility.md](https://github.com/eddiethedean/typra/blob/main/docs/reference/compatibility.md)
- **Types, constraints, indexes, queries**: [docs/reference/types.md](https://github.com/eddiethedean/typra/blob/main/docs/reference/types.md)
- **Operations and failure modes**: [docs/ops/operations_and_failure_modes.md](https://github.com/eddiethedean/typra/blob/main/docs/ops/operations_and_failure_modes.md)
- **Security posture**: [docs/reference/security.md](https://github.com/eddiethedean/typra/blob/main/docs/reference/security.md) · [SECURITY.md](https://github.com/eddiethedean/typra/blob/main/SECURITY.md)
- **1.0 readiness checklist**: [docs/reference/readiness.md](https://github.com/eddiethedean/typra/blob/main/docs/reference/readiness.md)

## Start here

- [Quickstart](https://github.com/eddiethedean/typra/blob/main/docs/guides/quickstart.md)
- [Python guide](https://github.com/eddiethedean/typra/blob/main/docs/guides/python.md)
- [Concepts](https://github.com/eddiethedean/typra/blob/main/docs/guides/concepts.md)
- [Operations](https://github.com/eddiethedean/typra/blob/main/docs/ops/operations_and_failure_modes.md)

---

## Python

The **`typra`** package on PyPI is a native extension. The **primary** interface is **class-defined schemas** via **`typra.models`** (dataclasses or Pydantic), with typed collections and queries returning model instances.

The lower-level **`fields_json`** API remains fully supported for programmatic schema generation and interop.

- **Python:** 3.9+
- **Wheels:** `cp39-abi3` (one wheel per platform)

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

```bash
pip install "typra>=1.0.0,<2"
```

Full package docs: [python/typra/README.md](https://github.com/eddiethedean/typra/blob/main/python/typra/README.md)

---

## Rust

### Application crate (recommended)

Use the **`typra`** crate — it re-exports the engine and enables **`#[derive(DbModel)]`** by default.

```toml
[dependencies]
typra = "1.0"
```

Without proc-macros (engine only):

```toml
typra = { version = "1.0", default-features = false }
```

Crate README: [crates/typra/README.md](https://github.com/eddiethedean/typra/blob/main/crates/typra/README.md)

### Lower-level crates

| Crate | Role | README |
|-------|------|--------|
| **`typra-core`** | Engine (storage, catalog, queries) | [crates/typra-core/README.md](https://github.com/eddiethedean/typra/blob/main/crates/typra-core/README.md) |
| **`typra-derive`** | `#[derive(DbModel)]` proc-macros | [crates/typra-derive/README.md](https://github.com/eddiethedean/typra/blob/main/crates/typra-derive/README.md) |

### Example

In-memory (repeatable; no leftover file). From the repo: **`cargo run -p typra --example open`**.

```rust
use std::borrow::Cow;
use typra::prelude::*;
use typra::FieldDef;
use typra::Type;
use typra::schema::FieldPath;

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

Field attributes on **`DbModel`** (top-level fields via `#[db(...)]`):

| Attribute | Effect |
|-----------|--------|
| `#[db(primary)]` | Primary key (exactly one field) |
| `#[db(unique)]` | Unique secondary index |
| `#[db(index)]` | Non-unique secondary index |
| `#[db(collection = "name")]` | Override collection name |

**Limitations (1.0):** nested field paths and constraint attributes are not emitted by the derive macro yet (use explicit `FieldDef` registration or Python `typra.models` for nested schemas).

---

## Philosophy

> **Your data should be correct by construction.**

---

## Development

| Path | Role |
|------|------|
| **`crates/`** | Rust crates — see per-crate READMEs linked above |
| **`python/`** | PyPI packaging — [python/README.md](https://github.com/eddiethedean/typra/blob/main/python/README.md) |
| **`docs/`** | Guides and specs — [docs/](https://github.com/eddiethedean/typra/tree/main/docs) |

Full local checks (ruff, ty, cargo fmt/clippy/test, pytest, documented example verification):

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -U pip
make check-full          # standard gate
make check-1p0-ready     # check-full + async facade tests
```

Benchmarks (Criterion):

```bash
cargo bench -p typra-core --bench query
cargo bench -p typra-core --bench workflows
```

## License

MIT — see [LICENSE](https://github.com/eddiethedean/typra/blob/main/LICENSE).
