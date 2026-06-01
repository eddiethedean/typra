# Python guide

**Audience:** application developers using the `modelvault` package on PyPI.

ModelVault is **the database for application models**: store **dataclasses** and **Pydantic v2 models** with engine-level validation, secondary indexes, schema evolution, and single-file deployment—without maintaining a parallel SQL schema.

!!! tip "New to ModelVault?"
    Read [Why ModelVault](why_modelvault.md) for the same positioning as the [README](https://github.com/eddiethedean/modelvault/blob/main/README.md), then [Quickstart](quickstart.md). Building a **FastAPI** service? Start with [FastAPI](fastapi.md) (`AsyncDatabase` + `async def`). Runnable apps: [Examples](../examples/index.md).

!!! tip "Recommended API"
    Prefer **`modelvault.models.collection`** over hand-written `fields_json` unless you need dynamic schemas. Details: [Models & collections](models_and_collections.md).

| Resource | Link |
|----------|------|
| Why ModelVault | [Why ModelVault](why_modelvault.md) · [Comparisons](../comparisons/index.md) |
| Examples | [Examples](../examples/index.md) · [todo_app on GitHub](https://github.com/eddiethedean/modelvault/tree/main/examples/todo_app) |
| Compatibility | [Compatibility matrix](../reference/compatibility.md) |
| Rust usage | [Quickstart](quickstart.md) |

## Install

**Requires CPython 3.9+.** Wheels use the stable ABI (`cp39-abi3`): one wheel per platform, compatible with 3.9+ on that platform.

```bash
pip install "modelvault>=0.15.0,<0.16"
```

Pin the major range you test against. ModelVault 1.x follows SemVer (breaking changes require 2.0).

## Quick start (low-level API)

The snippets below use **`register_collection`** + JSON field definitions — useful for dynamic schemas and tests. For application code, prefer **[`modelvault.models`](#define-schemas-with-classes)** or the [Pydantic guide](pydantic.md).

In-memory (repeatable; no file). For a file, use `Database.open("/path/to/app.modelvault")`.

```python
# Setup: module, in-memory DB, and one collection.
import modelvault

db = modelvault.Database.open_in_memory()
cid, ver = db.register_collection(
    "books",
    '[{"path": ["title"], "type": "string"}]',
    "title",
)
# Example: show path, registration ids, and registered names.
print("path:", db.path())
print("collection_id:", cid, "schema_version:", ver)
print("collection_names:", db.collection_names())
```

Output:

```text
path: :memory:
collection_id: 1 schema_version: 1
collection_names: ['books']
```

`register_collection` returns **`(collection_id, schema_version)`**. New collections start at id **`1`** and schema version **`1`**.

A longer insert/get example with **`modelvault.__version__`** is in [Quickstart](quickstart.md#python-models-insert-get) (verified in CI).

## Backup and restore

| Operation | API |
|-----------|-----|
| **Backup** | `db.export_snapshot("/path/to/backup.modelvault")` — checkpoints then copies (file-backed DBs) |
| **Restore** | `modelvault.Database.restore_snapshot(backup, dest)` — atomic replace of destination |

## Define schemas with classes

The low-level API accepts **`fields_json`**. For application code, use **`modelvault.models`**.

### Rules (1.0)

- Class must be a **`@dataclass`** or **Pydantic `BaseModel`** (Pydantic is optional)
- Declare primary key: `__modelvault_primary_key__ = "id"`
- Collection name defaults to **snake_case plural** (`Book` → `"books"`)
- Override with `__modelvault_collection__ = "my_name"`

### Constraints and indexes

```python
from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, Optional
from uuid import UUID
from datetime import datetime

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
    id: Optional[UUID] = None
    published_at: Optional[datetime] = None


db = modelvault.Database.open_in_memory()
books = modelvault.models.collection(db, Book)

books.insert(Book(title="Hello", year=2020, rating=4.5))
one = books.get("Hello")
rows = books.where("year", 2020).all()

# Field refs injected onto the class:
rows2 = books.where(Book.title, "Hello").all()

# Subset projection:
just_titles = books.where(Book.title, "Hello").select(["title"]).all()
```

### Schema evolution

```python
plan = modelvault.models.plan(db, Book)
new_version = modelvault.models.apply(db, Book, force=False)
```

### Updates (replace by primary key)

```python
books.update("Hello", {"rating": 5.0})
```

## Asyncio API (`AsyncDatabase`)

**Recommended for FastAPI and Starlette:** use **`modelvault.AsyncDatabase`** and **`modelvault.models.async_collection`** so route handlers can `await` storage without blocking the event loop. The sync **`Database`** API below is unchanged and remains the default for scripts, CLIs, and tests that are not asyncio-first.

```python
db = await modelvault.AsyncDatabase.open_in_memory()
books = modelvault.models.async_collection(db, Book)
await books.insert(Book(title="Hello", year=2020))
row = await books.get("Hello")

async with db.transaction():
    await db.insert("books", {"title": "Txn", "year": 2021})
```

Operations run the sync engine on a **thread pool** (GIL released during work)—responsive event loops, same on-disk durability and **single-writer-per-file** rules as sync.

### Concurrency on one handle

| Operation class | Behavior |
|-----------------|----------|
| **Reads** (`get`, `query`, `explain`, `collection_names`, …) | **Shared** lock — multiple `await`s (e.g. `asyncio.gather`) can run read work in parallel |
| **Writes** (`insert`, `delete`, schema changes, compaction, …) | **Exclusive** lock — one mutator at a time |
| **Open transaction** | All operations on that handle serialize until commit/rollback (readers see staged state) |

Cross-process rules are unchanged: one writer per `.modelvault` file; use `read_only=True` for additional reader processes. See [Async policy](../reference/async_policy.md) and the [FastAPI guide](fastapi.md).

### Sync `Database` and threads

`Database` uses the same **read/write lock** in the extension. Many threads may call `get` / `query` concurrently; they share the read lock. For CPU-heavy read batches, **`AsyncDatabase` + `asyncio.gather`** is usually a better fit than raw threads because of lower per-call Python overhead.

## `Database` API

### Open and path

| Method | Behavior |
|--------|----------|
| **`Database.open(path)`** | Open or **create** at `path`. Parent dirs must exist (`OSError` otherwise). |
| **`Database.open_in_memory()`** | Same logical DB in RAM |
| **`path()`** | Path string used to open (OS-normalized) |

Opening a **directory** or other non-file path raises **`OSError`**.

### Register a collection

```python
db.register_collection(name, fields_json, primary_field, indexes_json=None) -> tuple[int, int]
```

- Names are **trimmed**; empty after trim → **`ValueError`**
- **`primary_field`**: single-segment top-level scalar in `fields_json`
- **`indexes_json`**: optional array of `{name, path, kind}` — see [Indexes](#indexes_json)

Duplicate names or invalid JSON → **`ValueError`**. Unique violations on insert → **`ValueError`**.

### Insert and get

| Method | Notes |
|--------|-------|
| **`insert(collection, row)`** | Replace-by-PK. Nested dicts/lists per schema. Required fields required; optionals may be omitted or `None`. |
| **`get(collection, pk)`** | Latest row as `dict`, or `None` |

### Snapshots

| Method | Notes |
|--------|-------|
| **`snapshot_bytes()`** | Full in-memory image (in-memory / snapshot-opened DBs only) |
| **`open_snapshot_bytes(data)`** | Open from bytes |
| **`open_snapshot(path)`** | Open snapshot file in memory |
| **`export_snapshot(dest)`** | Write consistent snapshot file |

### Metadata

**`collection_names()`** — registered names in **sorted** order (not insertion order).

## Queries

### `collection(name) -> Collection`

Non-SQL query builder:

| Method | Purpose |
|--------|---------|
| **`where(path, value)`** | Equality (path: dotted string or tuple) |
| **`and_where(...)`** | Additional conjunct |
| **`limit(n)`** | Cap results |
| **`explain()`** | Simple plan string |
| **`all()`** | Matching rows as `dict` |
| **`all(fields=[...])`** | Subset projection — only listed paths in each result |

Design: [Query planner spec](../specs/query_planner.md).

### Query example

```python
# Setup: in-memory DB, schema, index, and one row.
import modelvault

db = modelvault.Database.open_in_memory()
fields = (
    '[{"path": ["title"], "type": "string"}, {"path": ["year"], "type": "int64"}]'
)
indexes = '[{"name": "title_idx", "path": ["title"], "kind": "index"}]'
db.register_collection("books", fields, "title", indexes)
db.insert("books", {"title": "Hello", "year": 2020})
# Example: indexed equality query with subset projection.
explain = db.collection("books").where("title", "Hello").explain()
rows = db.collection("books").where("title", "Hello").all(fields=["title"])
print("index_lookup:", "IndexLookup" in explain)
print("rows:", rows)
```

Output:

```text
index_lookup: True
rows: [{'title': 'Hello'}]
```

### Realistic workflow: indexed queries on disk

Order-line table: integer PK, indexes on `sku` and `status`, conjunctive filter, subset projection, reopen and `get`.

Row order from `all()` is not guaranteed — sort in app code when needed.

```python
# Setup: temp on-disk file, collection with indexes, and sample rows.
import tempfile
from pathlib import Path

import modelvault

with tempfile.TemporaryDirectory() as d:
    path = Path(d) / "app.modelvault"
    db = modelvault.Database.open(str(path))
    fields = """[
      {"path": ["id"], "type": "int64"},
      {"path": ["sku"], "type": "string"},
      {"path": ["qty"], "type": "int64"},
      {"path": ["status"], "type": "string"}
    ]"""
    indexes = """[
      {"name": "sku_idx", "path": ["sku"], "kind": "index"},
      {"name": "status_idx", "path": ["status"], "kind": "index"}
    ]"""
    db.register_collection("order_lines", fields, "id", indexes)
    for oid, sku, qty, st in [
        (1, "SKU-A", 2, "open"),
        (2, "SKU-B", 1, "shipped"),
        (3, "SKU-A", 4, "open"),
    ]:
        db.insert("order_lines", {"id": oid, "sku": sku, "qty": qty, "status": st})
    # Example: conjunctive query, subset projection, reopen and `get` by PK.
    q = (
        db.collection("order_lines")
        .where("status", "open")
        .and_where("sku", "SKU-A")
        .limit(10)
    )
    rows = sorted(q.all(), key=lambda r: r["id"])
    print("indexed:", "IndexLookup" in q.explain())
    print("matches:", len(rows))
    print("rows:", rows)
    short = sorted(
        db.collection("order_lines").where("status", "open").all(
            fields=["id", "qty"]
        ),
        key=lambda r: r["id"],
    )
    print("subset:", short)
    db2 = modelvault.Database.open(str(path))
    row = db2.get("order_lines", 1)
    print("reopen_qty:", row["qty"] if row else None)
```

Output:

```text
indexed: True
matches: 2
rows: [{'id': 1, 'qty': 2, 'sku': 'SKU-A', 'status': 'open'}, {'id': 3, 'qty': 4, 'sku': 'SKU-A', 'status': 'open'}]
subset: [{'id': 1, 'qty': 2}, {'id': 3, 'qty': 4}]
reopen_qty: 2
```

For tests, use a temp file as above. For fixed paths, create parent directories before `open` and catch **`OSError`**.

## DB-API 2.0 (PEP 249)

Read-only adapter at **`modelvault.dbapi`**. Maps to the typed query AST — not a full SQL engine.

### Supported SQL (1.0)

- **`SELECT` only** (read-only)
- `SELECT cols|* FROM collection`
- `WHERE` with `=`, `AND`, `OR`, ranges (`<`, `<=`, `>`, `>=`) and **`?`** parameters
- `ORDER BY field [ASC|DESC]`
- `LIMIT n`

Anything else → `ValueError`.

```python
import modelvault

conn = modelvault.dbapi.connect("app.modelvault")
cur = conn.cursor()
cur.execute(
    "SELECT id,title FROM books WHERE year >= ? ORDER BY id DESC LIMIT 10",
    (2020,),
)
rows = cur.fetchall()
```

!!! info "SQLAlchemy"
    Full SQLAlchemy integration is planned post-1.0. Use the native query builder for application code today.

## `fields_json` reference

JSON **array** of field objects:

| Key | Meaning |
|-----|---------|
| **`path`** | Segment array, e.g. `["profile", "name"]` |
| **`type`** | Primitive string or nested composite |
| **`constraints`** | Optional array — see [Types matrix](../reference/types.md) |

### Primitives

`"bool"`, `"int64"`, `"uint64"`, `"float64"`, `"string"`, `"bytes"`, `"uuid"`, `"timestamp"`

Unknown names → **`ValueError`**.

### Composites

```json
{"optional": "string"}
{"list": "string"}
{"object": [{"path": ["street"], "type": "string"}]}
{"enum": ["draft", "published"]}
```

More examples: [python/modelvault README on GitHub](https://github.com/eddiethedean/modelvault/blob/main/python/modelvault/README.md).

### Example: multiple top-level fields

```python
# Setup: in-memory DB and a multi-field `books` schema (PK `title`).
import modelvault

db = modelvault.Database.open_in_memory()
fields = """[
  {"path": ["title"], "type": "string"},
  {"path": ["year"], "type": "int64"},
  {"path": ["tags"], "type": {"list": "string"}}
]"""
cid, ver = db.register_collection("books", fields, "title")
# Example: show assigned collection and schema version ids.
print("collection_id:", cid, "schema_version:", ver)
```

Output:

```text
collection_id: 1 schema_version: 1
```

## `indexes_json`

Optional array passed to `register_collection`:

| Key | Meaning |
|-----|---------|
| **`name`** | Stable index name (unique in array) |
| **`path`** | Must match a `path` in `fields_json`; scalar or optional-of-scalar |
| **`kind`** | `"unique"` or `"index"` / `"non_unique"` |

## Persistence

Registrations are **durable**: reopen the same path and `collection_names()` reflects what was registered (same catalog as Rust).

## Errors

| Situation | Exception |
|-----------|-----------|
| Invalid JSON, schema shape, duplicate name | **`ValueError`** |
| I/O (missing parent, permissions, directory path) | **`OSError`** |
| Unsupported engine path | **`RuntimeError`** |

Also see typed subclasses in [Debugging](../ops/debugging.md): `ModelVaultValidationError`, `ModelVaultSchemaError`, etc.

Catch **`ValueError`** and **`OSError`** around `open`, `register_collection`, and `insert` in production.

## Not in Python yet

- Arbitrary SQL (use structured queries)
- Rich migration workflows beyond `plan` / `apply` helpers
- Automatic Pydantic → schema inference without explicit model metadata

See the [roadmap](https://github.com/eddiethedean/modelvault/blob/main/ROADMAP.md).

## Development (from this repo)

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -U pip
.venv/bin/python -m pip install -U "maturin>=1.5,<2" pytest
cd python/modelvault
maturin develop --release
pytest -q
```

Or from repo root: **`make check-full`**. See [python/README on GitHub](https://github.com/eddiethedean/modelvault/blob/main/python/README.md).
