# typra (Python)

[![CI](https://github.com/eddiethedean/typra/actions/workflows/ci.yml/badge.svg)](https://github.com/eddiethedean/typra/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/typra.svg)](https://pypi.org/project/typra/)

Official **CPython** bindings for **Typra** (PyO3 native extension): a typed, embedded database with a Rust core.

## Status

You get a durable **schema catalog**, **validation**, nested **row values** (record **v2** on insert; **v1** segments still replay), and **constraints** in a single **`.typra`** file, plus **in-memory** databases and **snapshot** bytes.

**Queries and secondary indexes (0.7+):** register optional **`indexes_json`** on **`register_collection`**, then use **`db.collection("name").where("field", value).and_where(...).limit(n).explain()`** and **`all()`** / **`all(fields=[...])`** for subset rows. A longer **on-disk + reopen** example lives in the [Python user guide — Realistic workflow](https://github.com/eddiethedean/typra/blob/main/docs/guide_python.md#realistic-workflow-indexed-queries-on-disk). Typra also ships a **read-only DB-API 2.0 adapter** (`typra.dbapi`) with a minimal `SELECT` subset—see the [Python guide](https://github.com/eddiethedean/typra/blob/main/docs/guide_python.md#db-api-20-pep-249-and-sqlalchemy).

| Resource | Link |
|----------|------|
| **Repository** | [github.com/eddiethedean/typra](https://github.com/eddiethedean/typra) |
| **Rust crates** | [`typra` on crates.io](https://crates.io/crates/typra) |
| **Full Python guide** | [docs/guide_python.md](https://github.com/eddiethedean/typra/blob/main/docs/guide_python.md) |
| **Getting started** | [docs/guide_getting_started.md](https://github.com/eddiethedean/typra/blob/main/docs/guide_getting_started.md) |
| **Rust module layout** | [docs/03_rust_crate_and_module_layout.md](https://github.com/eddiethedean/typra/blob/main/docs/03_rust_crate_and_module_layout.md) |
| **Changelog** | [CHANGELOG.md](https://github.com/eddiethedean/typra/blob/main/CHANGELOG.md) |

## Requirements

- **CPython 3.9+**
- Wheels use the stable ABI (**`cp39-abi3`**): one wheel per platform, compatible with Python 3.9+ on that platform.

## Install

```bash
pip install "typra>=1.0.0,<2"
```

Pin the major range you test against; 1.x releases follow SemVer (breaking changes require 2.0).

## Quick start

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

books.insert(Book(title="Typra", year=2020, rating=4.5))
print(books.get("Typra"))
print(typra.__version__)
```

Output (the version line matches the installed wheel):

```text
Book(title='Typra', year=2020, rating=4.5)
1.0.0
```

On disk, use **`Database.open("app.typra")`** instead; registrations are **persisted** across process restarts for that path.

### Indexed query (sketch)

```python
# Setup: in-memory DB, indexed collection, one row.
import typra

db = typra.Database.open_in_memory()
fields = '[{"path": ["id"], "type": "int64"}, {"path": ["sku"], "type": "string"}]'
indexes = '[{"name": "sku_idx", "path": ["sku"], "kind": "index"}]'
db.register_collection("items", fields, "id", indexes)
db.insert("items", {"id": 1, "sku": "abc"})
# Example: equality query on indexed `sku`.
print(db.collection("items").where("sku", "abc").all())
```

Output:

```text
[{'id': 1, 'sku': 'abc'}]
```

See **[`docs/guide_python.md`](https://github.com/eddiethedean/typra/blob/main/docs/guide_python.md)** for `and_where`, `limit`, `explain`, and subset projections.

## API overview

| Member | Description |
|--------|-------------|
| `typra.__version__` | Package version (matches the Rust workspace release). |
| `Database.open(path: str)` | Create or open a database file. Raises `OSError` if the path cannot be opened (e.g. missing parent directory, path is a directory). |
| `db.path() -> str` | Path used to open the database. |
| `db.register_collection(name, fields_json, primary_field, indexes_json=None) -> tuple[int, int]` | Register a **new** collection (schema version **1**). Optional **`indexes_json`**: JSON array of `{"name", "path", "kind"}` objects (`"unique"` or `"index"` / `"non_unique"`). Returns **`(collection_id, schema_version)`**. Names are trimmed; duplicates or bad JSON raise `ValueError`. |
| `db.collection(name) -> Collection` | Query handle: **`where`**, **`and_where`**, **`limit`**, **`explain`**, **`all`** / **`all(fields=[...])`**. |
| `db.insert(collection_name, row: dict) -> None` | Insert or replace the latest row (required fields + optional keys per schema). |
| `db.get(collection_name, pk) -> dict \| None` | Latest row or missing. |
| `Database.open_in_memory()` / `Database.open_snapshot_bytes(data)` / `db.snapshot_bytes()` | In-memory DB and byte snapshots. |
| `db.collection_names() -> list[str]` | All registered names, **sorted** alphabetically. |

For behavior details (errors, edge cases, development), see the **[Python user guide](https://github.com/eddiethedean/typra/blob/main/docs/guide_python.md)**.

## `fields_json` (schema descriptor)

`fields_json` is the lower-level schema descriptor accepted by `Database.register_collection(...)`. Prefer **`typra.models`** unless you need programmatic JSON generation or a dynamic schema.

It must be a JSON **array** of objects. Each object describes one field:

- **`path`**: JSON array of strings (path segments), e.g. `["profile", "name"]`.
- **`type`**: either a **primitive** name or a **composite** object.

**Primitives:** `"bool"`, `"int64"`, `"uint64"`, `"float64"`, `"string"`, `"bytes"`, `"uuid"`, `"timestamp"`.

**Composites:**

- Optional: `{"optional": <inner>}`
- List: `{"list": <inner>}`
- Object: `{"object": [ … same shape as top-level field objects … ]}`
- Enum: `{"enum": ["a", "b"]}` (variants must be strings)
- **`constraints`** (optional): JSON array of constraint objects, e.g. `{"min_i64": 0}`, `{"max_length": 100}`, `{"email": true}`, `{"regex": "^[a-z]+$"}`.

### Example (nested)

```python
# Setup: in-memory DB and a collection whose PK uses an optional int field.
import typra

db = typra.Database.open_in_memory()
db.register_collection(
    "items",
    '[{"path": ["x"], "type": {"optional": "int64"}}]',
    "x",
)
# Example: confirm registration.
print("nested:", db.collection_names())
```

Output:

```text
nested: ['items']
```

### Example (multiple fields)

```python
# Setup: in-memory DB and a multi-field `books` schema (PK `title`).
import typra

db = typra.Database.open_in_memory()
schema = """[
  {"path": ["title"], "type": "string"},
  {"path": ["year"], "type": "int64"},
  {"path": ["tags"], "type": {"list": "string"}}
]"""
db.register_collection("books", schema, "title")
# Example: confirm registration.
print("multi:", db.collection_names())
```

Output:

```text
multi: ['books']
```

## Exceptions

- **`ValueError`**: invalid JSON, wrong shape, unknown type, invalid collection name, duplicate collection name, validation failures, or format/schema errors from the engine when registering.
- **`OSError`**: I/O failures when opening the database file.
- **`RuntimeError`**: reserved for engine “not implemented” paths (unexpected for supported API paths).

## Building from source

You need **Rust**, **Python 3.9+**, and **[maturin](https://www.maturin.rs/)**. From the repo’s **`python/typra`** directory:

```bash
maturin develop --release
pytest -q
```

From the repository root, **`make check-full`** runs Rust + Python checks, tests, and **`make verify-doc-examples`** (validates documented command output). See also **[python/README.md](https://github.com/eddiethedean/typra/blob/main/python/README.md)** (workspace layout for contributors).
