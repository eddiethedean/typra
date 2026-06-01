# Quickstart

**Audience:** beginner — get your first model stored in about five minutes.

Install, open a database, define a schema, insert a row. Covers **Python** (`modelvault.models`) and a **minimal Rust** registration example.

!!! tip "New here?"
    Read [Why ModelVault](why_modelvault.md) for positioning, or [Comparisons](../comparisons/index.md) vs SQLite and JSON. Prefer Pydantic? See the [Pydantic guide](pydantic.md).

!!! tip "Already know embedded DBs?"
    ModelVault is **schema-first**: you declare types and constraints up front; invalid writes fail at the boundary. See [Core concepts](concepts.md) for the full picture.

## Install

=== "Python"

    **Requires CPython 3.9+.** Wheels use the stable ABI (`cp39-abi3`).

    ```bash
    pip install "modelvault>=0.14.0,<0.15"
    ```

=== "Rust"

    Add to your application `Cargo.toml`:

    ```toml
    [dependencies]
    modelvault = "0.14"
    ```

## Rust: open and register a collection

In-memory (no file left behind). For on-disk, use `Database::open("my.modelvault")?`.

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

## Python: models, insert, get

Recommended path: define a **dataclass** (or Pydantic model) and use **`modelvault.models.collection`**.

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
modelvault 0.14.0
```

On disk, swap `open_in_memory()` for `Database.open("app.modelvault")`.

## What’s in 1.0

- Persisted **schema catalog**, **validation**, **indexes**, **queries** (including ranges and `order_by`)
- **Transactions**, **migrations**, **compaction**, **checkpoints**
- **Multi-segment nested field paths** (record payload v3)
- Read-only **DB-API** (`modelvault.dbapi`) with a minimal `SELECT` subset

!!! info "Roadmap"
    Full SQL and SQLAlchemy are planned post-1.0. See the [roadmap on GitHub](https://github.com/eddiethedean/modelvault/blob/main/ROADMAP.md).

## Contracts & matrices

- [Compatibility & recovery](../reference/compatibility.md)
- [Types, constraints, indexes, queries](../reference/types.md)

## Next steps

| Topic | Guide |
|-------|-------|
| Why ModelVault exists | [Why ModelVault](why_modelvault.md) |
| Pydantic & FastAPI | [Pydantic](pydantic.md) · [FastAPI](fastapi.md) |
| Mental model | [Core concepts](concepts.md) |
| Python in depth | [Python guide](python.md) |
| Class schemas & projections | [Models & collections](models_and_collections.md) |
| Disk vs memory | [Storage modes](storage_modes.md) |
| Compare alternatives | [Comparisons](../comparisons/index.md) |
| Design specs | [Specifications](../specs/index.md) |

## Contributors

From the repo root:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -U pip
make check-full
```

Runs Rust + Python checks, tests, and **`verify-doc-examples`** (stdout from snippets on this page, the root README, and the Python guide must match documented output).
