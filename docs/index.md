# ModelVault

**The database for application models.**

ModelVault is a schema-first, typed embedded database for application data. Store **Pydantic models**, **dataclasses**, or **Rust structs** directly—validation, indexes, migrations, and durability are built in. Ship a single **`.modelvault`** file with your app, or open **`:memory:`** in tests. One engine powers both **Python** and **Rust**.

!!! note "What ModelVault is not"
    ModelVault does **not** aim to replace PostgreSQL, compete with SQLite as a general SQL database, or support **SQLAlchemy** as a primary interface today. It is **model-first**: typed APIs in Rust and Python, plus an experimental read-only SQL subset for DB-API interop. See [Non-goals](#non-goals) below.

<div class="grid cards" markdown>

-   :material-lightbulb-on:{ .lg .middle } **Why ModelVault?**

    ---

    When embedded SQL, JSON files, or TinyDB leave gaps for *application* data.

    [:octicons-arrow-right-24: Why ModelVault](guides/why_modelvault.md)

-   :material-rocket-launch:{ .lg .middle } **Quickstart**

    ---

    Install, define a model, insert your first row—in about five minutes.

    [:octicons-arrow-right-24: Quickstart](guides/quickstart.md)

-   :material-heart-pulse:{ .lg .middle } **Pydantic & FastAPI**

    ---

    Your API types and your storage schema stay aligned.

    [:octicons-arrow-right-24: Pydantic](guides/pydantic.md) · [FastAPI](guides/fastapi.md)

-   :material-scale-balance:{ .lg .middle } **Comparisons**

    ---

    ModelVault vs SQLite, JSON, TinyDB, and DuckDB—with clear boundaries.

    [:octicons-arrow-right-24: Comparisons](comparisons/index.md)

</div>

## Why ModelVault?

Most apps need **local, durable storage** for domain objects—users, settings, inventory lines, project metadata—without running a database server. Common choices each impose a tax:

| Alternative | Limitation | ModelVault |
|-------------|------------|------------|
| **Relational DBs** (SQLite + ORM) | App models must be mapped into tables and SQL migrations; impedance mismatch with Pydantic/dataclasses | **Model-first** schemas; validation on write; nested objects as first-class typed paths |
| **JSON / YAML files** | No indexes, weak queries, integrity and evolution are manual | Typed storage, indexes, queries, and a crash-safe append-only file format |
| **TinyDB** | Lightweight documents without production-grade validation or schema evolution | Strict schemas, migrations, indexes, and operational tooling |
| **DuckDB** | Built for analytics (OLAP), not everyday app CRUD | Embedded **OLTP** for application models—complementary, not competing |

[:octicons-arrow-right-24: Full positioning](guides/why_modelvault.md) · [:octicons-arrow-right-24: Comparison matrix](comparisons/index.md)

## What you get

| Benefit | What it means for your app |
|---------|----------------------------|
| **Store models directly** | Your Python class or Rust struct *is* the schema—no parallel DDL to maintain |
| **Validate on write** | Invalid types and constraint violations fail before data reaches disk |
| **Query with indexes** | Equality, ranges, `AND`/`OR`, `order_by`, and `limit`—without standing up a server |
| **Evolve safely** | Versioned schema catalog, compatibility checks, and migration helpers (`plan` / `apply`) |
| **Deploy as one file** | Copy `app.modelvault` with your binary, installer, or repo—no daemon to operate |

## 60-second example (Python)

Store a Pydantic model—no hand-written schema JSON:

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

[:octicons-arrow-right-24: Full quickstart](guides/quickstart.md) (dataclasses, on-disk files, and Rust)

## Who is it for?

| Persona | What you get |
|---------|----------------|
| **FastAPI developer** | Local persistence without PostgreSQL for prototypes and small services; models map straight to storage |
| **Desktop app** | Ship a `.modelvault` file; validation and indexes built in |
| **CLI tool** | Durable, typed local data—better than ad-hoc JSON |
| **Local-first app** | Offline storage with schema evolution—no database server |

## Non-goals

ModelVault optimizes for **typed application documents in one process**, not every datastore use case:

- **Not SQL-first** — model APIs are primary; full ISO SQL and SQLAlchemy integration are on the [roadmap](https://github.com/eddiethedean/modelvault/blob/main/ROADMAP.md).
- **Not a network database** — embedded single-writer semantics; no replication or server mode in the current release.
- **Not OLAP at scale** — use DuckDB (or export) for heavy analytics; ModelVault is OLTP-oriented for app state.

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

## Capabilities (current release)

| Area | Summary |
|------|---------|
| **Schema-first** | Collections with typed fields, constraints, and multi-segment nested paths |
| **Validation on write** | Engine rejects invalid data before append |
| **Indexes & queries** | Secondary indexes; equality, ranges, `AND`/`OR`, `order_by`, `limit` |
| **Durability** | Transactions, checkpoints, compaction, configurable recovery |
| **Python ergonomics** | `modelvault.models` (recommended) + optional read-only `modelvault.dbapi` |
| **Operations** | `modelvault` CLI—inspect, verify, backup, checkpoint, compact |

Package version **0.14.x** on PyPI and crates.io delivers the **1.0 product milestone** (see [Compatibility → Versioning](reference/compatibility.md#versioning-package-vs-product)).

## Documentation by goal

| I want to… | Start here |
|------------|------------|
| Understand why ModelVault exists | [Why ModelVault](guides/why_modelvault.md) · [Launch essay](https://github.com/eddiethedean/modelvault/blob/main/blog/modelvault-for-application-models.md) |
| Store my first model in five minutes | [Quickstart](guides/quickstart.md) |
| Use Pydantic or FastAPI | [Pydantic](guides/pydantic.md) · [FastAPI](guides/fastapi.md) |
| Learn the mental model | [Core concepts](guides/concepts.md) |
| Run sample apps | [Examples](examples/index.md) |
| Build with Python or Rust | [Python guide](guides/python.md) · [Rust API](reference/rust_api.md) |
| Run backups and recovery | [Operations runbook](ops/operations_and_failure_modes.md) |
| Evaluate production contracts | [Compatibility](reference/compatibility.md) · [Security](reference/security.md) |

## Choose your path

Not sure where to click next? See [Choose your path](map.md)—organized by evaluator, builder, operator, and contributor.
