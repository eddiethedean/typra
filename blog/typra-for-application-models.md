# Typra: the database for application models

*May 2026 · Typra 1.0*

**SQLite simplicity, with real types.**

If you build FastAPI services, desktop apps, or CLI tools, you already think in Pydantic models, dataclasses, or Rust structs. Typra is an embedded database that stores those models directly — with validation on write, indexes, schema evolution, and a single file you can ship beside your binary.

No database server. No parallel SQL schema to maintain for the common case.

## The question Typra answers

Where do I put my application data locally?

Not warehouse analytics. Not multi-tenant Postgres. **Application state**: users, settings, task lists, inventory lines, project metadata.

Developers usually reach for one of these:

| Choice | What goes wrong |
|--------|-----------------|
| **SQLite + ORM** | SQL migrations and mapping layers sit between your models and disk |
| **JSON / YAML files** | No indexes, weak queries, integrity is DIY |
| **TinyDB** | Fine for scripts; thin on validation and production evolution |
| **DuckDB** | Excellent for OLAP; not the default home for app CRUD |

Typra targets the gap: **typed documents with indexes**, one file, model-first APIs.

## What it feels like in Python

Define a Pydantic model, mark the primary key, open a database:

```python
from pydantic import BaseModel
import typra

class Book(BaseModel):
    __typra_primary_key__ = "title"
    title: str
    year: int

db = typra.Database.open("app.typra")
books = typra.models.collection(db, Book)
books.insert(Book(title="Typra", year=2026))
print(books.get("Typra"))
```

Invalid data fails at `insert` — before it reaches disk. Add indexes on the model when you need fast `where` queries. Ship `app.typra` with your installer or copy it in backups.

The same engine is available in **Rust** (`typra` on crates.io) for native apps and tooling.

## Who it is for

- **FastAPI developers** prototyping without standing up PostgreSQL
- **Desktop apps** that need offline settings and domain data in a user data directory
- **CLI tools** that outgrew `config.json` but do not want ops overhead
- **Local-first apps** that need schema evolution over months of releases

## How it differs from SQLite

SQLite is the right default when you want portable SQL and a universe of tools. Typra is for when your **source of truth is already typed application models** and you want the database to enforce that contract — including nested objects as first-class paths, not JSON blobs in `TEXT` columns.

You can still think of Typra as embedded and single-file, like SQLite. The abstraction is different: **collections bound to schemas**, not tables you describe in SQL.

## How it differs from JSON files

JSON on disk is the fastest prototype. Typra keeps the deployment story (one file) and adds engine-level validation, secondary indexes, transactions, and a versioned format with recovery modes. You stop re-loading and re-parsing entire files for every lookup.

## DuckDB is complementary

Use Typra for authoritative app state. Export snapshots into DuckDB when you need heavy analytics. OLTP vs OLAP — same split many teams already run with SQLite + DuckDB, with a model-native OLTP side.

## What ships in 1.0

- `typra.models` for dataclasses and Pydantic v2
- Secondary indexes and typed queries (`where`, `and_where`, `order_by`, `limit`)
- Transactions, checkpoints, compaction, recovery
- Python and Rust APIs, `typra` CLI for inspect/verify/backup
- Read-only DB-API subset (minimal `SELECT`)

Full ISO SQL and SQLAlchemy integration are on the roadmap; Typra 1.0 is **model-first**.

## Try it in five minutes

```bash
pip install "typra>=1.0.0,<2" pydantic
```

Docs: [typra.readthedocs.io](https://typra.readthedocs.io/en/latest/)

Runnable examples in the repo: [github.com/eddiethedean/typra/tree/main/examples](https://github.com/eddiethedean/typra/tree/main/examples) — todo app, CLI notes, FastAPI, desktop data directory.

Rust:

```toml
[dependencies]
typra = "1.0"
```

## The goal

When you need a database for **application models**, you reach for Typra — the way you reach for SQLite for embedded SQL or DuckDB for analytics.

We built 1.0 to earn that reflex. Feedback and issues welcome on [GitHub](https://github.com/eddiethedean/typra).

---

*Related: [Why Typra](https://typra.readthedocs.io/en/latest/guides/why_typra/) · [Comparisons](https://typra.readthedocs.io/en/latest/comparisons/) · [Quickstart](https://typra.readthedocs.io/en/latest/guides/quickstart/)*
