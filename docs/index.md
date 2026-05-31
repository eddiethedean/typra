# Typra documentation

**SQLite simplicity, with real types.**

Typra is a typed, embedded database for application data: one file, strict schemas, validation on write, and nested objects as first-class citizens. Ship a `.typra` file with your app—or run in memory for tests—with the same APIs in **Rust** and **Python**.

<div class="grid cards" markdown>

-   :material-rocket-launch:{ .lg .middle } **Get started in 5 minutes**

    ---

    Install, open a database, define a schema, insert a row.

    [:octicons-arrow-right-24: Quickstart](guides/quickstart.md)

-   :material-language-python:{ .lg .middle } **Build with Python**

    ---

    Use `typra.models` with dataclasses or Pydantic—the recommended path.

    [:octicons-arrow-right-24: Python guide](guides/python.md)

-   :material-language-rust:{ .lg .middle } **Build with Rust**

    ---

    `Database`, `FieldDef`, optional `#[derive(DbModel)]`.

    [:octicons-arrow-right-24: Rust API](reference/rust_api.md)

-   :material-shield-check:{ .lg .middle } **Production contracts**

    ---

    Compatibility, recovery, types matrix, and security posture.

    [:octicons-arrow-right-24: Compatibility](reference/compatibility.md)

</div>

## What you get in 1.0

| Capability | Summary |
|------------|---------|
| **Schema-first** | Collections with typed fields, constraints, and nested paths |
| **Validation on write** | Invalid data fails before it hits disk |
| **Indexes & queries** | Secondary indexes; equality, ranges, `AND`/`OR`, `order_by`, `limit` |
| **Durability** | Transactions, checkpoints, compaction, recovery modes |
| **Python ergonomics** | `typra.models` + optional read-only DB-API |
| **Operations** | `typra` CLI for inspect, verify, backup, migrate |

!!! note "Not SQL-first (yet)"
    Typra is **model-first**. A minimal read-only SQL subset exists for DB-API; full ISO SQL and SQLAlchemy are on the [roadmap](https://github.com/eddiethedean/typra/blob/main/ROADMAP.md#post-10-isoiec-9075-sql-track).

## Install

=== "Python"

    ```bash
    pip install "typra>=1.0.0,<2"
    ```

=== "Rust"

    ```toml
    [dependencies]
    typra = "1.0"
    ```

## Minimal example (Python)

```python
from dataclasses import dataclass
from typing import Annotated, Optional
import typra

@dataclass
class Book:
    __typra_primary_key__ = "title"
    title: str
    year: Annotated[int, typra.models.constrained(min_i64=0)]
    rating: Optional[float] = None

db = typra.Database.open_in_memory()
books = typra.models.collection(db, Book)
books.insert(Book(title="Hello", year=2020, rating=4.5))
print(books.get("Hello"))
```

## Learn the mental model

1. **Database** — one embedded unit (file or memory).
2. **Collection** — typed container of records (like a table, but schema-driven).
3. **Schema** — field paths, types, constraints, indexes.
4. **Model** — your Rust struct or Python class that maps to a collection.

[:octicons-arrow-right-24: Core concepts](guides/concepts.md)

## Need something specific?

| I want to… | Go to |
|------------|-------|
| Evaluate guarantees | [Compatibility](reference/compatibility.md) · [Types](reference/types.md) · [Security](reference/security.md) |
| Run backups / recover from corruption | [Operations runbook](ops/operations_and_failure_modes.md) |
| Understand the file format | [Specifications](specs/index.md) |
| Contribute or release | [Contributing](dev/contributing_guide.md) |
