# Quickstart

**Audience:** beginners — first model stored in about five minutes.

This guide walks through install, open a database, define a schema, and read a row back. It mirrors the [project README](https://github.com/eddiethedean/modelvault/blob/main/README.md) examples so you can trust the same mental model everywhere.

!!! tip "Evaluating ModelVault?"
    Read [Why ModelVault](why_modelvault.md) for positioning and the [comparison matrix](../comparisons/index.md) vs SQLite, JSON, and TinyDB.

!!! tip "Already use embedded databases?"
    ModelVault is **schema-first**: types and constraints are declared before writes; the engine rejects invalid data at the boundary. See [Core concepts](concepts.md) for the full model.

## Install

=== "Python"

    **Requires CPython 3.9+.** Wheels use the stable ABI (`cp39-abi3`).

    ```bash
    pip install "modelvault>=0.15.0,<0.16"
    ```

=== "Rust"

    Add to your application `Cargo.toml`:

    ```toml
    [dependencies]
    modelvault = "0.15"
    ```

## Python: models, insert, get (recommended) {#python-models-insert-get}

The recommended path is a **dataclass** or **Pydantic** model plus **`modelvault.models.collection`**—your class is the schema.

```python
# Setup: class-defined schema + in-memory DB.
from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, Optional

import modelvault


@dataclass
class Book:
    __modelvault_primary_key__ = "title"
    __modelvault_indexes__ = [
        modelvault.models.index("year"),
        modelvault.models.unique("title"),
    ]

    title: str
    year: Annotated[int, modelvault.models.constrained(min_i64=0)]
    rating: Optional[float] = None


db = modelvault.Database.open_in_memory()
books = modelvault.models.collection(db, Book)

books.insert(Book(title="Hello", year=2020, rating=4.5))
print("get:", books.get("Hello"))
print("modelvault", modelvault.__version__)
```

### Run it (from this repo)

```bash
make python-develop
.venv/bin/python your_script.py   # paste the snippet above
```

Output:

```text
get: Book(title='Hello', year=2020, rating=4.5)
modelvault 0.15.0
```

For a durable file, use `Database.open("app.modelvault")` instead of `open_in_memory()`. Pydantic follows the same pattern—see the [Pydantic guide](pydantic.md).

## Rust: open and register a collection

Use the **`modelvault`** facade crate. This example is in-memory; for on-disk storage, use `Database::open("my.modelvault")?`.

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

### Run it (from this repo)

```bash
cargo run -q -p modelvault --example open
```

Output:

```text
opened: :memory:
registered collection id=1 version=1
```

## What the current release includes

| Capability | Notes |
|------------|-------|
| **Schema catalog** | Versioned collections, fields, constraints, indexes |
| **Validation on write** | Types and engine constraints before append |
| **Queries** | Equality, ranges, `AND`/`OR`, `order_by`, `limit` |
| **Durability** | Transactions, checkpoints, compaction, recovery modes |
| **Nested paths** | Multi-segment fields (e.g. `profile.timezone`) end-to-end |
| **DB-API (read-only)** | Experimental `modelvault.dbapi` with a minimal `SELECT` subset |

!!! info "SQL and SQLAlchemy"
    ModelVault is **model-first**. Full ISO SQL and SQLAlchemy are planned post–current milestone. See the [roadmap on GitHub](https://github.com/eddiethedean/modelvault/blob/main/ROADMAP.md).

## Reference material

- [Compatibility & recovery](../reference/compatibility.md)
- [Types, constraints, indexes, queries](../reference/types.md)

## Next steps

| Topic | Guide |
|-------|-------|
| Why ModelVault exists | [Why ModelVault](why_modelvault.md) |
| Pydantic & FastAPI | [Pydantic](pydantic.md) · [FastAPI](fastapi.md) (`AsyncDatabase`, async routes) |
| Mental model | [Core concepts](concepts.md) |
| Python in depth | [Python guide](python.md) |
| Class schemas & projections | [Models & collections](models_and_collections.md) |
| Disk vs memory | [Storage modes](storage_modes.md) |
| Compare alternatives | [Comparisons](../comparisons/index.md) |
| Engine design | [Specifications](../specs/index.md) |

## Contributors

From the repo root:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -U pip
make check-full
```

This runs Rust and Python checks, tests, and **`verify-doc-examples`** (stdout from snippets on this page, the root README, and selected guides must match documented output).
