# Typra User Guide: Python

This guide covers the **`typra`** PyPI package: installation, the **`Database`** API, optional **`indexes_json`**, the **query** builder on **`db.collection(...)`** (`where`, `and_where`, `limit`, `explain`, `all`, subset **`all(fields=[...])`**), the **`fields_json`** schema format, error behavior, and local development.

For project-wide status and roadmap, see [`ROADMAP.md`](../ROADMAP.md). For Rust-first usage, see [`guide_getting_started.md`](guide_getting_started.md). For how the engine is organized in Rust, see [`03_rust_crate_and_module_layout.md`](03_rust_crate_and_module_layout.md).

## Install

**Requires CPython 3.9+.** Wheels use the stable ABI (`cp39-abi3`): one wheel per platform, compatible with 3.9 and newer on that platform.

```bash
pip install "typra>=0.10.0,<0.11"
```

Pin the minor range you test against; pre-1.0 minors may include API or format changes.

## Quick start

In-memory (repeatable; no file). To use a file instead, replace `open_in_memory()` with `open("/path/to/app.typra")`.

```python
# Setup: module, in-memory DB, and one collection.
import typra

db = typra.Database.open_in_memory()
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

`register_collection` returns **`(collection_id, schema_version)`**. For a new collection, ids start at **`1`** and the first schema version is **`1`**.

A longer insert/get snippet with **`typra.__version__`** is in **[`guide_getting_started.md` — Minimal Python example](guide_getting_started.md#minimal-python-example)** (also verified in CI).

## `Database`

### `Database.open(path: str) -> Database`

Opens an existing file or **creates** a new database at `path`. Parent directories must already exist; otherwise an **`OSError`** is raised (same as creating a regular file in that location).

Opening a **directory** path (or another non-file that cannot be used as a database file) results in an **`OSError`**.

### `path() -> str`

Returns the path string used to open the database (normalized by the OS path handling underlying the Rust core).

### `register_collection(name: str, fields_json: str, primary_field: str, indexes_json: str | None = None) -> tuple[int, int]`

Registers a **new** collection named `name` with schema version **1**. Collection names are **trimmed** of leading/trailing whitespace; empty names after trimming raise **`ValueError`**.

`fields_json` must be a JSON **array** of field objects (see below). **`primary_field`** must name a **single-segment** top-level field present in that array; the primary key must be a **primitive** scalar (see [`migration_0.5_to_0.6.md`](migration_0.5_to_0.6.md)).

Optional **`indexes_json`** is a JSON **array** of secondary index objects:

| Key | Type | Meaning |
|-----|------|--------|
| **`name`** | string | Stable index name within the collection (non-empty, unique in the array). |
| **`path`** | array of strings | Must **exactly** match a `path` entry in `fields_json`; the field must be a scalar or optional-of-scalar (not a list or object root). |
| **`kind`** | string | `"unique"` for a uniqueness index, or `"index"` / `"non_unique"` for a non-unique index. |

If parsing or typing fails, **`ValueError`** is raised with a message describing the problem. If the name is already registered, **`ValueError`** is raised. Unique index violations on **`insert`** also surface as **`ValueError`**.

### `insert(collection_name: str, row: dict) -> None`

Inserts or replaces the latest row for that collection. **`row`** values are converted to the engine’s **`RowValue`** model (nested dicts/lists, optional omission / null per schema). Required fields must be present; **`Optional<T>`** fields may be omitted or set to **`None`**. Invalid types or **constraint** failures raise **`ValueError`** (same rules as Rust).

### `get(collection_name: str, pk: object) -> dict | None`

Returns the latest row as a **`dict`** of JSON-like values, or **`None`** if no row exists for that primary key.

### `Database.open_in_memory() -> Database` / `Database.open_snapshot_bytes(data: bytes) -> Database` / `snapshot_bytes() -> bytes`

In-memory databases use the same logical format as files. **`snapshot_bytes`** copies the full image (only for in-memory / snapshot-opened databases).

### `collection_names() -> list[str]`

Returns registered collection names in **sorted order** (not insertion order).

## Queries and the `Collection` handle

### `collection(name: str) -> Collection`

Returns a handle for **non-SQL** queries on `name`. Use **`where(path, value)`** for equality (path as a dotted string or tuple of segments), **`and_where`** for additional conjuncts, **`limit(n)`**, **`explain()`** for a simple plan string, and **`all()`** for matching rows as **`dict`** values.

**`all(fields=...)`** optionally takes a list (or tuple) of paths; each path must match a field in `fields_json`. Only those fields are copied into each result dict (subset projection for large rows).

Design reference: [`docs/05_query_planner_and_execution_spec.md`](05_query_planner_and_execution_spec.md).

### Query example

```python
# Setup: in-memory DB, schema, index, and one row.
import typra

db = typra.Database.open_in_memory()
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

This pattern matches a small **order line** table: **integer primary key**, **non-unique indexes** on `sku` and `status`, several inserts, a conjunctive filter (`where` + `and_where`), **subset projection**, then **reopen** the same file and read back by primary key.

Row order from `all()` is not guaranteed to be sorted; sort in application code when you need a stable listing.

```python
# Setup: temp on-disk file, collection with indexes, and sample rows.
import tempfile
from pathlib import Path

import typra

with tempfile.TemporaryDirectory() as d:
    path = Path(d) / "app.typra"
    db = typra.Database.open(str(path))
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
    db2 = typra.Database.open(str(path))
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

For **ephemeral** integration tests (CI, notebooks), prefer a temp file as above. For a fixed path in an application, ensure parent directories exist before `open`, and catch **`OSError`** around file creation.

## DB-API 2.0 (PEP 249) and SQLAlchemy

Typra ships an **experimental, read-only** DB-API 2.0 adapter (PEP 249) starting in **0.10.0**, exposed as **`typra.dbapi`**. The SQL surface is intentionally small and maps onto the engine’s typed query AST.

### Supported SQL subset (0.10.0)

- **Only `SELECT`** is supported (read-only).
- `SELECT <cols|*> FROM <collection>`
- Optional `WHERE` with `=` / `AND` / `OR` and range predicates (`<`, `<=`, `>`, `>=`) using **`?` positional parameters**.
- Optional `ORDER BY <field> [ASC|DESC]` (default `ASC`)
- Optional `LIMIT n`

Anything outside this subset raises `ValueError`.

### DB-API usage (0.10.0)

```python
import typra

conn = typra.dbapi.connect("app.typra")
cur = conn.cursor()
cur.execute("SELECT id,title FROM books WHERE year >= ? ORDER BY id DESC LIMIT 10", (2020,))
rows = cur.fetchall()
```

### SQLAlchemy

SQLAlchemy integration remains **planned**. For now, prefer the native non-SQL query builder (`collection(...).where(...)`) for application code.

## `fields_json` (schema descriptor)

`fields_json` is a JSON **array**. Each element is an object with:

| Key | Type | Meaning |
|-----|------|--------|
| **`path`** | array of strings | Field path segments, e.g. `["profile", "name"]`. Each segment must be a JSON string. |
| **`type`** | string or object | Primitive name, or a nested composite (optional, list, object, enum). |
| **`constraints`** | array (optional) | Constraint objects persisted in the catalog (e.g. `{"min_i64": 0}`, `{"max_length": 100}`, `{"regex": "^[a-z]+$"}`, `{"email": true}`). See [`python/typra/README.md`](../python/typra/README.md). |

### Primitives

Use a string literal:

`"bool"`, `"int64"`, `"uint64"`, `"float64"`, `"string"`, `"bytes"`, `"uuid"`, `"timestamp"`

Unknown names produce a **`ValueError`** mentioning the unknown primitive.

### Optional

```json
{"optional": "string"}
```

Nested arbitrarily: `{"optional": {"list": "int64"}}`.

### List

```json
{"list": "string"}
```

### Object (nested fields)

```json
{"object": [
  {"path": ["street"], "type": "string"},
  {"path": ["zip"], "type": "string"}
]}
```

### Enum

```json
{"enum": ["draft", "published"]}
```

Each variant must be a JSON string.

### Example: multiple top-level fields

```python
# Setup: in-memory DB and a multi-field `books` schema (PK `title`).
import typra

db = typra.Database.open_in_memory()
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

## Persistence

Registrations are **durable**: after you close the process and open the same path again, `collection_names()` reflects what was registered. This uses the same on-disk catalog as the Rust API (schema segments + superblocks).

## Errors

| Situation | Typical exception |
|-----------|-------------------|
| Invalid JSON, wrong JSON shape, unknown type, duplicate collection name, invalid collection name | **`ValueError`** |
| I/O problems opening the file (missing parent dir, permission, is a directory, etc.) | **`OSError`** |
| Engine reports “not implemented” (should not occur for supported API paths) | **`RuntimeError`** |

Always catch **`ValueError`** and **`OSError`** around `open`, `register_collection`, and **`insert`** in production code.

## What is not exposed in Python yet

- Arbitrary **SQL** (use the structured query builder; see [Queries and the `Collection` handle](#queries-and-the-collection-handle) above).
- **Schema migrations beyond basic helpers** (the Python surface includes `plan_schema_version`, `register_schema_version(..., force=...)`, and `backfill_top_level_field`, but richer migration workflows are still evolving).
- Pydantic model inference (you pass explicit `fields_json`; the Rust engine still validates on insert).

See [`ROADMAP.md`](../ROADMAP.md) for upcoming milestones.

## Development (build from this repo)

From the repository root, with Python 3.9+:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -U pip
.venv/bin/python -m pip install -U "maturin>=1.5,<2" pytest
cd python/typra
maturin develop --release
pytest -q
```

Or run **`make check-full`** from the repo root (Rust + Python checks and tests). See also [`python/README.md`](../python/README.md).
