# Typra

[![CI](https://github.com/eddiethedean/typra/actions/workflows/ci.yml/badge.svg)](https://github.com/eddiethedean/typra/actions/workflows/ci.yml)
[![Docs](https://readthedocs.org/projects/typra/badge/?version=latest)](https://typra.readthedocs.io/en/latest/)
[![crates.io](https://img.shields.io/crates/v/typra.svg)](https://crates.io/crates/typra)
[![PyPI](https://img.shields.io/pypi/v/typra.svg)](https://pypi.org/project/typra/)

## SQLite simplicity, with real types

**Typra is the database for application models** — a typed embedded database for application data.

Store dataclasses and Pydantic models directly with validation, indexes, migrations, and single-file deployment. Same engine in **Rust** and **Python**.

**Documentation:** **[typra.readthedocs.io](https://typra.readthedocs.io/en/latest/)**

## Why Typra?

| Alternative | Limitation | Typra |
|-------------|------------|-------|
| **SQLite** | SQL schemas and migrations; loose typing for app objects | Model-first schemas; validation on write; nested objects native |
| **JSON files** | No indexes, weak queries, manual integrity | Typed storage, indexes, queries, durability |
| **TinyDB** | Document store without production-grade validation or evolution | Strict schemas, migrations, indexes, crash-safe file format |
| **DuckDB** | Built for analytics (OLAP), not app CRUD | Embedded OLTP for application models (complementary, not competing) |

Deeper comparisons: [Typra vs SQLite](https://typra.readthedocs.io/en/latest/comparisons/sqlite/) · [JSON](https://typra.readthedocs.io/en/latest/comparisons/json/) · [TinyDB](https://typra.readthedocs.io/en/latest/comparisons/tinydb/) · [DuckDB](https://typra.readthedocs.io/en/latest/comparisons/duckdb/) · [Why Typra](https://typra.readthedocs.io/en/latest/guides/why_typra/)

## 60-second example (Python)

Store a Pydantic model — no low-level schema JSON:

```python
# pip install "typra>=1.0.0,<2" pydantic
from pydantic import BaseModel
import typra


class Book(BaseModel):
    __typra_primary_key__ = "title"

    title: str
    year: int


db = typra.Database.open_in_memory()
books = typra.models.collection(db, Book)
books.insert(Book(title="Hello", year=2020))
print(books.get("Hello"))
print(typra.__version__)
```

Output:

```text
title='Hello' year=2020
1.0.0
```

Also works with **dataclasses** — see the [Pydantic guide](https://typra.readthedocs.io/en/latest/guides/pydantic/) and [Quickstart](https://typra.readthedocs.io/en/latest/guides/quickstart/).

## Who is it for?

| Persona | What you get |
|---------|----------------|
| **FastAPI developer** | Local persistence without PostgreSQL; models map straight to storage — [FastAPI guide](https://typra.readthedocs.io/en/latest/guides/fastapi/) |
| **Desktop app** | Ship a `.typra` file; validation and indexes built in |
| **CLI tool** | Durable, typed local data — better than ad-hoc JSON |
| **Local-first app** | Offline storage with schema evolution — no database server |

## Install

```bash
pip install "typra>=1.0.0,<2"
```

```toml
[dependencies]
typra = "1.0"
```

## Documentation (by goal)

| I want to… | Start here |
|------------|------------|
| Understand why Typra exists | [Why Typra](https://typra.readthedocs.io/en/latest/guides/why_typra/) |
| Store my first model in 5 minutes | [Quickstart](https://typra.readthedocs.io/en/latest/guides/quickstart/) |
| Use Pydantic or FastAPI | [Pydantic](https://typra.readthedocs.io/en/latest/guides/pydantic/) · [FastAPI](https://typra.readthedocs.io/en/latest/guides/fastapi/) |
| Compare to SQLite / JSON / TinyDB | [Comparisons](https://typra.readthedocs.io/en/latest/comparisons/) |
| Run sample apps | [Examples](https://typra.readthedocs.io/en/latest/examples/) · [examples/](https://github.com/eddiethedean/typra/tree/main/examples) |
| Build with Python or Rust | [Python guide](https://typra.readthedocs.io/en/latest/guides/python/) · [Rust API](https://typra.readthedocs.io/en/latest/reference/rust_api/) |
| Run backups and recovery | [Operations runbook](https://typra.readthedocs.io/en/latest/ops/operations_and_failure_modes/) |
| Evaluate production contracts | [Compatibility](https://typra.readthedocs.io/en/latest/reference/compatibility/) · [Security](https://typra.readthedocs.io/en/latest/reference/security/) |
| Read the launch essay | [Blog](https://github.com/eddiethedean/typra/blob/main/blog/typra-for-application-models.md) |

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

More: **[Rust API](https://typra.readthedocs.io/en/latest/reference/rust_api/)** · crate README: [crates/typra/README.md](https://github.com/eddiethedean/typra/blob/main/crates/typra/README.md)

## Repository layout

| Path | Role |
|------|------|
| **`crates/`** | Rust engine and facade |
| **`python/`** | PyPI package ([python/typra/README.md](https://github.com/eddiethedean/typra/blob/main/python/typra/README.md)) |
| **`docs/`** | [typra.readthedocs.io](https://typra.readthedocs.io/en/latest/) |
| **`examples/`** | Runnable todo, CLI, FastAPI, and desktop samples |
| **`blog/`** | [Launch essay](https://github.com/eddiethedean/typra/blob/main/blog/typra-for-application-models.md) |

Local checks: `make check-full` · 1.0 gate: `make check-1p0-ready`

## License

MIT — [LICENSE](https://github.com/eddiethedean/typra/blob/main/LICENSE)
