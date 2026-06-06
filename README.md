# ModelVault

[![CI](https://github.com/eddiethedean/modelvault/actions/workflows/ci.yml/badge.svg)](https://github.com/eddiethedean/modelvault/actions/workflows/ci.yml)
[![Docs](https://readthedocs.org/projects/modelvault/badge/?version=latest)](https://modelvault.readthedocs.io/en/latest/)
[![crates.io](https://img.shields.io/crates/v/modelvault.svg)](https://crates.io/crates/modelvault)
[![PyPI](https://img.shields.io/pypi/v/modelvault.svg)](https://pypi.org/project/modelvault/)

## Typed embedded storage for application models

**ModelVault is the database for application models** — a schema-first, typed embedded database for application data.

Store dataclasses and Pydantic models directly with validation, indexes, migrations, and single-file deployment. Same engine in **Rust** and **Python**. **Concurrent reads** on one handle (`asyncio.gather`, thread pools) overlap safely; writes stay exclusive.

**Non-goals:** ModelVault does **not** aim to support **SQL**, **SQLAlchemy**, or to compete with relational databases like SQLite. ModelVault’s supported interfaces are its typed Rust APIs and the model-first Python APIs.

**Documentation:** **[modelvault.readthedocs.io](https://modelvault.readthedocs.io/en/latest/)**

## Why ModelVault?

| Alternative | Limitation | ModelVault |
|-------------|------------|-------|
| **Relational DBs** | App models must be mapped into tables/rows; schema friction | Model-first schemas; validation on write; nested objects native |
| **JSON files** | No indexes, weak queries, manual integrity | Typed storage, indexes, queries, durability |
| **TinyDB** | Document store without production-grade validation or evolution | Strict schemas, migrations, indexes, crash-safe file format |
| **DuckDB** | Built for analytics (OLAP), not app CRUD | Embedded OLTP for application models (complementary, not competing) |

More: [Why ModelVault](https://modelvault.readthedocs.io/en/latest/guides/why_modelvault/)

## 60-second example (Python)

Store a Pydantic model — no low-level schema JSON:

```python
# pip install "modelvault>=0.16.0,<0.17" pydantic
from pydantic import BaseModel
import modelvault


class Book(BaseModel):
    __modelvault_primary_key__ = "title"

    title: str
    year: int


db = modelvault.Database.open_in_memory()
books = modelvault.models.collection(db, Book)
books.insert(Book(title="Hello", year=2020))
print(books.get("Hello"))
print(modelvault.__version__)
```

Output:

```text
title='Hello' year=2020
0.16.0
```

Also works with **dataclasses** — see the [Pydantic guide](https://modelvault.readthedocs.io/en/latest/guides/pydantic/) and [Quickstart](https://modelvault.readthedocs.io/en/latest/guides/quickstart/).

## Who is it for?

| Persona | What you get |
|---------|----------------|
| **FastAPI developer** | Local persistence without PostgreSQL; `AsyncDatabase` + parallel reads for list endpoints — [FastAPI guide](https://modelvault.readthedocs.io/en/latest/guides/fastapi/) |
| **Desktop app** | Ship a `.modelvault` file; validation and indexes built in |
| **CLI tool** | Durable, typed local data — better than ad-hoc JSON |
| **Local-first app** | Offline storage with schema evolution — no database server |

## Install

```bash
pip install "modelvault>=0.16.0,<0.17"
```

```toml
[dependencies]
modelvault = "0.16"
```

**Toolchain:** Rust **stable** (see [`rust-toolchain.toml`](rust-toolchain.toml)); Python **3.9+**.

**Sizing (0.16+):** the pager keeps up to **512** pages (~2–8 MB depending on page size) in an LRU cache by default. Large `ORDER BY` queries spill to temp segments; small `LIMIT` uses a top-K heap instead of a full sort. Run `compact()` periodically if the log grows large.

## Documentation (by goal)

| I want to… | Start here |
|------------|------------|
| Understand why ModelVault exists | [Why ModelVault](https://modelvault.readthedocs.io/en/latest/guides/why_modelvault/) |
| Store my first model in 5 minutes | [Quickstart](https://modelvault.readthedocs.io/en/latest/guides/quickstart/) |
| Use Pydantic or FastAPI | [Pydantic](https://modelvault.readthedocs.io/en/latest/guides/pydantic/) · [FastAPI](https://modelvault.readthedocs.io/en/latest/guides/fastapi/) (`AsyncDatabase`, async routes) |
| Run sample apps | [Examples](https://modelvault.readthedocs.io/en/latest/examples/) · [examples/](https://github.com/eddiethedean/modelvault/tree/main/examples) |
| Build with Python or Rust | [Python guide](https://modelvault.readthedocs.io/en/latest/guides/python/) · [Rust API](https://modelvault.readthedocs.io/en/latest/reference/rust_api/) |
| Run backups and recovery | [Operations runbook](https://modelvault.readthedocs.io/en/latest/ops/operations_and_failure_modes/) |
| Evaluate production contracts | [Compatibility](https://modelvault.readthedocs.io/en/latest/reference/compatibility/) · [Security](https://modelvault.readthedocs.io/en/latest/reference/security/) |
| Read the launch essay | [Blog](https://github.com/eddiethedean/modelvault/blob/main/blog/modelvault-for-application-models.md) |

## Rust (30 seconds)

Use the **`modelvault`** facade crate. From this repo: `cargo run -p modelvault --example open`.

```rust
use std::borrow::Cow;
use modelvault::prelude::*;
use modelvault::FieldDef;
use modelvault::Type;
use modelvault::schema::FieldPath;

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

More: **[Rust API](https://modelvault.readthedocs.io/en/latest/reference/rust_api/)** · [Async / concurrency policy](https://modelvault.readthedocs.io/en/latest/reference/async_policy/) · crate README: [crates/modelvault/README.md](https://github.com/eddiethedean/modelvault/blob/main/crates/modelvault/README.md)

## Repository layout

| Path | Role |
|------|------|
| **`crates/`** | Rust engine and facade |
| **`python/`** | PyPI package ([python/modelvault/README.md](https://github.com/eddiethedean/modelvault/blob/main/python/modelvault/README.md)) |
| **`docs/`** | [modelvault.readthedocs.io](https://modelvault.readthedocs.io/en/latest/) |
| **`examples/`** | Runnable todo, CLI, FastAPI, and desktop samples |
| **`blog/`** | [Launch essay](https://github.com/eddiethedean/modelvault/blob/main/blog/modelvault-for-application-models.md) |

Local checks: `make check-full` · Release gate: `make check-2p0-ready` (see [readiness checklist](docs/reference/readiness.md#release-cut-checklist))

## License

MIT — [LICENSE](https://github.com/eddiethedean/modelvault/blob/main/LICENSE)
