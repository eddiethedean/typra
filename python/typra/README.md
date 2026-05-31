# typra (Python)

[![CI](https://github.com/eddiethedean/typra/actions/workflows/ci.yml/badge.svg)](https://github.com/eddiethedean/typra/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/typra.svg)](https://pypi.org/project/typra/)
[![Docs](https://readthedocs.org/projects/typra/badge/?version=latest)](https://typra.readthedocs.io/en/latest/?badge=latest)

> **SQLite simplicity, with real types.**

Official **CPython** bindings for **Typra** (PyO3 native extension): a typed, embedded database with a Rust core.

**Primary API:** **`typra.models`** — define schemas with dataclasses or Pydantic v2, then use typed collections and queries. Lower-level **`fields_json`** remains available for dynamic schemas.

| Resource | Link |
|----------|------|
| **Repository** | [github.com/eddiethedean/typra](https://github.com/eddiethedean/typra) |
| **Rust crate** | [`typra` on crates.io](https://crates.io/crates/typra) |
| **Python guide** | [docs/guides/python.md](https://github.com/eddiethedean/typra/blob/main/docs/guides/python.md) |
| **Quickstart** | [docs/guides/quickstart.md](https://github.com/eddiethedean/typra/blob/main/docs/guides/quickstart.md) |
| **API reference** | [docs/reference/python_api.md](https://github.com/eddiethedean/typra/blob/main/docs/reference/python_api.md) |
| **Types matrix** | [docs/reference/types.md](https://github.com/eddiethedean/typra/blob/main/docs/reference/types.md) |
| **Changelog** | [CHANGELOG.md](https://github.com/eddiethedean/typra/blob/main/CHANGELOG.md) |
| **Roadmap** | [ROADMAP.md](https://github.com/eddiethedean/typra/blob/main/ROADMAP.md) |

## What ships (v1.0.x)

- **Single-file** (`.typra`) and **in-memory** databases with snapshot import/export
- **Schema catalog** with validation, constraints, and **multi-segment nested paths** (record v3)
- **Transactions**, migrations (`plan` / `apply`, backfill), compaction, backup snapshots
- **Secondary indexes** and typed **query builder** (`where`, `and_where`, `limit`, `explain`, subset `all(fields=[...])`)
- **`typra.models`** (dataclass / Pydantic) as the recommended application API
- **`typra.dbapi`**: read-only PEP 249 adapter with a minimal `SELECT` subset
- Structured errors: **`TypraFormatError`**, **`TypraSchemaError`**, **`TypraValidationError`**, **`TypraQueryError`**, **`TypraTransactionError`**

Full SQL / SQLAlchemy are **planned post-1.0** — see [ROADMAP](https://github.com/eddiethedean/typra/blob/main/ROADMAP.md#post-10-isoiec-9075-sql-track).

## Requirements

- **CPython 3.9+**
- Wheels use the stable ABI (**`cp39-abi3`**): one wheel per platform

## Install

```bash
pip install "typra>=1.0.0,<2"
```

Pin the major range you test against; **1.x** follows SemVer (breaking changes require **2.0**).

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

On disk, use **`Database.open("app.typra")`** instead; registrations persist across restarts.

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

More examples: [Python guide — Realistic workflow](https://github.com/eddiethedean/typra/blob/main/docs/guides/python.md#realistic-workflow-indexed-queries-on-disk) · [DB-API subset](https://github.com/eddiethedean/typra/blob/main/docs/guides/python.md#db-api-20-pep-249-and-sqlalchemy)

## API overview

| Member | Description |
|--------|-------------|
| `typra.__version__` | Package version (matches the Rust workspace release). |
| `Database.open(path: str)` | Create or open a database file. Raises `OSError` if the path cannot be opened (e.g. missing parent directory, path is a directory). |
| `db.path() -> str` | Path used to open the database. |
| `db.register_collection(name, fields_json, primary_field, indexes_json=None) -> tuple[int, int]` | Register a **new** collection (schema version **1**). Optional **`indexes_json`**. |
| `db.register_schema_version(name, fields_json, indexes_json=None, *, force=False) -> int` | Bump schema version (migration-aware; use **`plan_schema_version`** first). |
| `db.plan_schema_version(name, fields_json, indexes_json=None) -> dict` | Plan migration steps before registering a new schema version. |
| `db.backfill_top_level_field(name, field, value) -> None` | Backfill a missing top-level field for all rows. |
| `db.backfill_field_at_path(name, path, value) -> None` | Backfill a missing nested field (`path` is a list of segments). |
| `db.delete(collection_name, pk) -> None` | Delete a row by primary key. |
| `db.compact()` / `db.compact_to(path)` | Rewrite the database to drop dead log segments. |
| `db.export_snapshot(path)` / `Database.open_snapshot(path)` | Backup/restore via snapshot files. |
| `db.collection(name) -> Collection` | Query handle: **`where`**, **`and_where`**, **`limit`**, **`explain`**, **`all`** / **`all(fields=[...])`**. |
| `db.insert(collection_name, row: dict) -> None` | Insert or replace the latest row (required fields + optional keys per schema). |
| `db.get(collection_name, pk) -> dict \| None` | Latest row or missing. |
| `with db.transaction():` | Multi-write transaction (read-your-writes). |
| `typra.models` | **Primary API**: class-defined schemas (dataclass/Pydantic). |
| `Database.open_in_memory()` / `Database.open_snapshot_bytes(data)` / `db.snapshot_bytes()` | In-memory DB and byte snapshots. |
| `db.collection_names() -> list[str]` | All registered names, **sorted** alphabetically. |

Behavior details (errors, edge cases, migrations): **[Python guide](https://github.com/eddiethedean/typra/blob/main/docs/guides/python.md)**.

## `fields_json` (advanced)

JSON array schema descriptor for **`register_collection`**. Prefer **`typra.models`** unless you need programmatic or dynamic schemas.

Each field object:

- **`path`**: JSON array of strings, e.g. `["profile", "name"]`
- **`type`**: primitive name or composite object
- **`constraints`** (optional): e.g. `{"min_i64": 0}`, `{"max_length": 100}`, `{"email": true}`

**Primitives:** `"bool"`, `"int64"`, `"uint64"`, `"float64"`, `"string"`, `"bytes"`, `"uuid"`, `"timestamp"`.

**Composites:** `{"optional": …}`, `{"list": …}`, `{"object": […]}`, `{"enum": ["a", "b"]}`.

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

| Exception | Typical cause |
|-----------|----------------|
| **`ValueError`** | Invalid JSON/shape, unknown types, duplicate collection, validation failures |
| **`OSError`** | I/O failures opening the database file |
| **`TypraFormatError`** | Corrupt or unsupported on-disk format |
| **`TypraSchemaError`** | Schema mismatch, unknown collection, migration required |
| **`TypraValidationError`** | Constraint or type validation on write |
| **`TypraQueryError`** | Query construction or SQL adapter errors |
| **`TypraTransactionError`** | Transaction boundary violations |
| **`RuntimeError`** | Unexpected engine paths (should not occur on supported APIs) |

## Building from source

Requires **Rust**, **Python 3.9+**, and **[maturin](https://www.maturin.rs/)**.

```bash
cd python/typra
maturin develop --release
pytest -q
```

From the repository root, **`make check-full`** runs the full pipeline including **`scripts/verify-doc-examples.sh`**. Contributor layout: **[python/README.md](https://github.com/eddiethedean/typra/blob/main/python/README.md)**.

## License

MIT — see [LICENSE](https://github.com/eddiethedean/typra/blob/main/LICENSE).
