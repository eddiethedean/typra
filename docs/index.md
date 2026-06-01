# ModelVault

## SQLite simplicity, with real types

**The database for application models** — store dataclasses and Pydantic models directly with validation, indexes, migrations, and single-file deployment. Ship a `.modelvault` file with your app, or run in memory for tests. Same engine in **Rust** and **Python**.

<div class="grid cards" markdown>

-   :material-lightbulb-on:{ .lg .middle } **Why ModelVault?**

    ---

    When to choose ModelVault over SQLite, JSON, TinyDB, or DuckDB.

    [:octicons-arrow-right-24: Why ModelVault](guides/why_modelvault.md)

-   :material-rocket-launch:{ .lg .middle } **Get started in 5 minutes**

    ---

    Install, define a model, insert your first row.

    [:octicons-arrow-right-24: Quickstart](guides/quickstart.md)

-   :material-heart-pulse:{ .lg .middle } **Pydantic & FastAPI**

    ---

    Model-first Python: feels like an extension of your app types.

    [:octicons-arrow-right-24: Pydantic](guides/pydantic.md) · [FastAPI](guides/fastapi.md)

-   :material-scale-balance:{ .lg .middle } **Comparisons**

    ---

    Feature matrix and deep dives vs common alternatives.

    [:octicons-arrow-right-24: Comparisons](comparisons/index.md)

</div>

## Three benefits

| Benefit | What it means |
|---------|----------------|
| **Store models directly** | Your Python class or Rust struct is the schema — no parallel SQL DDL |
| **Validate on write** | Invalid data fails before it hits disk |
| **Deploy as a single file** | One `.modelvault` file (or `:memory:`) — no database server to run |

## Quick example (Pydantic)

```python
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
```

[:octicons-arrow-right-24: Full quickstart](guides/quickstart.md)

## Who is it for?

- **FastAPI** — local persistence without standing up PostgreSQL for small services
- **Desktop apps** — embedded storage with validation; ship one file
- **CLI tools** — structured, durable local data beyond JSON blobs
- **Local-first** — offline-first apps with schema evolution over time

## How is ModelVault different?

| | SQLite | JSON files | TinyDB | ModelVault |
|--|--------|------------|--------|-------|
| App model mapping | Manual SQL + ORM | Manual | Manual | **Native** |
| Validation on write | App layer | None | Limited | **Engine** |
| Nested objects | JSON columns / serialize | Native but untyped | Native | **Typed paths** |
| Indexes | SQL indexes | None | Basic | **Declared on model** |
| Single-file deploy | Yes | Yes | Yes | **Yes** |

[:octicons-arrow-right-24: Full comparison matrix](comparisons/index.md)

## Install

=== "Python"

    ```bash
    pip install "modelvault>=0.14.0,<0.15"
    ```

=== "Rust"

    ```toml
    [dependencies]
    modelvault = "0.14"
    ```

## What you get in 1.0

| Capability | Summary |
|------------|---------|
| **Schema-first** | Collections with typed fields, constraints, and nested paths |
| **Validation on write** | Invalid data fails before it hits disk |
| **Indexes & queries** | Secondary indexes; equality, ranges, `AND`/`OR`, `order_by`, `limit` |
| **Durability** | Transactions, checkpoints, compaction, recovery modes |
| **Python ergonomics** | `modelvault.models` + optional read-only DB-API |
| **Operations** | `modelvault` CLI for inspect, verify, backup, migrate |

!!! note "Not SQL-first (yet)"
    ModelVault is **model-first**. A minimal read-only SQL subset exists for DB-API; full ISO SQL and SQLAlchemy are on the [roadmap](https://github.com/eddiethedean/modelvault/blob/main/ROADMAP.md#post-10-isoiec-9075-sql-track).

## Documentation by goal

| I want to… | Go to |
|------------|-------|
| Decide if ModelVault fits | [Why ModelVault](guides/why_modelvault.md) · [Comparisons](comparisons/index.md) · [Launch blog](https://github.com/eddiethedean/modelvault/blob/main/blog/modelvault-for-application-models.md) |
| Try it quickly | [Quickstart](guides/quickstart.md) |
| Build with Pydantic / FastAPI | [Pydantic](guides/pydantic.md) · [FastAPI](guides/fastapi.md) |
| Learn the mental model | [Core concepts](guides/concepts.md) |
| Production guarantees | [Compatibility](reference/compatibility.md) · [Security](reference/security.md) |
| Backups and recovery | [Operations runbook](ops/operations_and_failure_modes.md) |
| File format and engine internals | [Specifications](specs/index.md) |
