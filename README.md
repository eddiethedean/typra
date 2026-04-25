# Typra

[![CI](https://github.com/eddiethedean/typra/actions/workflows/ci.yml/badge.svg)](https://github.com/eddiethedean/typra/actions/workflows/ci.yml)
[![Docs](https://readthedocs.org/projects/typra/badge/?version=latest)](https://typra.readthedocs.io/en/latest/?badge=latest)
[![crates.io](https://img.shields.io/crates/v/typra.svg)](https://crates.io/crates/typra)
[![PyPI](https://img.shields.io/pypi/v/typra.svg)](https://pypi.org/project/typra/)

> **SQLite simplicity, with real types.**

Typra is a **typed, embedded database** for application data.  
It combines the ease of SQLite with **strict schemas, validation, and nested data support**—so your data is modeled explicitly end to end.

## What ships (v1.0.x)

- **Typed schemas + validation on write** (constraints, nested objects/lists)
- **Single-file durability** with transactions, recovery modes, checkpoints, and compaction
- **Secondary indexes** (unique + non-unique) and **typed queries** (equality/AND/OR/ranges/order_by/limit)
- **Rust facade** (`typra`) with optional `#[derive(DbModel)]`
- **Python package** (`typra`) with `typra.models` (recommended) plus `fields_json` and a minimal read-only DB-API adapter

### Non-goals (for now)

- Full SQL surface / SQLAlchemy dialect support (DB-API is a minimal read-only `SELECT` subset)
- General-purpose OLAP engine features (joins/group-by SQL, etc.)

## Guarantees and contracts

- **Compatibility and recovery contract**: [`docs/reference/compatibility.md`](docs/reference/compatibility.md)
- **Supported features matrix** (types, constraints, indexes, queries): [`docs/reference/types.md`](docs/reference/types.md)
- **Operations and failure modes**: [`docs/ops/operations_and_failure_modes.md`](docs/ops/operations_and_failure_modes.md)
- **Security posture** (threat model + disclosure): [`docs/reference/security.md`](docs/reference/security.md) and [`SECURITY.md`](SECURITY.md)

## Start here

- **Quickstart**: [`docs/guides/quickstart.md`](docs/guides/quickstart.md)
- **Python guide**: [`docs/guides/python.md`](docs/guides/python.md)
- **Operations**: [`docs/ops/operations_and_failure_modes.md`](docs/ops/operations_and_failure_modes.md)
- **1.0 readiness checklist**: [`docs/reference/readiness.md`](docs/reference/readiness.md)

---

## Why Typra?

Modern applications already define their data using Rust structs, Pydantic models, or TypeScript schemas—but many databases accept loosely typed rows anyway.

**Typra** targets **models as schema**, **validation on write**, **nested data as first-class**, and **single-file** deployment.

---

## Python

The **`typra`** package on PyPI is a native extension. The **primary** interface is **class-defined schemas** via **`typra.models`** (dataclasses or Pydantic), with typed-ish collections/queries returning instances.

The lower-level **`fields_json`** API is still available and fully supported, but is documented as an advanced escape hatch for programmatic schema generation and interop.

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

---

## Rust

### Application crate (recommended)

Use the **`typra`** crate — it re-exports the engine and enables **`#[derive(DbModel)]`** by default. See **[`crates/typra/README.md`](crates/typra/README.md)**.

```toml
[dependencies]
typra = "1.0"
```

Without proc-macros (engine only):

```toml
typra = { version = "1.0", default-features = false }
```

### Lower-level crates

Depend on **`typra-core`** and **`typra-derive`** directly when you need a minimal graph or custom macro wiring (same semver as **`typra`**).

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

Field attributes (`#[db(primary)]`, etc.) on **`DbModel`** are **not** implemented yet.

---

## Philosophy

> **Your data should be correct by construction.**

---

## Development

| Path | Role |
|------|------|
| **`crates/`** | Rust crates (**`typra`**, **`typra-core`**, **`typra-derive`**) — see per-crate READMEs |
| **`python/`** | PyPI packaging — see **[`python/README.md`](python/README.md)** |

Full local checks (ruff, ty, cargo fmt/clippy/test, pytest, **documented example output verification**):

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -U pip
make check-full
```

Benchmarks (Criterion):

```bash
cargo bench -p typra-core --bench query
cargo bench -p typra-core --bench workflows
```

Design specs live under **[`docs/`](docs/)**.

## License

MIT — see **[LICENSE](LICENSE)**.
