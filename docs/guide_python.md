# Typra User Guide: Python

This guide covers the **`typra`** PyPI package: installation, the **`Database`** API, the **`fields_json`** schema format, error behavior, and local development.

For project-wide status and roadmap, see [`ROADMAP.md`](../ROADMAP.md). For Rust-first usage, see [`guide_getting_started.md`](guide_getting_started.md).

## Install

**Requires CPython 3.9+.** Wheels use the stable ABI (`cp39-abi3`): one wheel per platform, compatible with 3.9 and newer on that platform.

```bash
pip install "typra>=0.5.0,<0.6"
```

Pin the minor range you test against; pre-1.0 minors may include API or format changes.

## Quick start

```python
import typra

db = typra.Database.open("app.typra")
cid, ver = db.register_collection(
    "books",
    '[{"path": ["title"], "type": "string"}]',
    "title",
)
assert db.path().endswith("app.typra")
assert db.collection_names() == ["books"]
```

`register_collection` returns **`(collection_id, schema_version)`**. For a new collection, ids start at **`1`** and the first schema version is **`1`**.

## `Database`

### `Database.open(path: str) -> Database`

Opens an existing file or **creates** a new database at `path`. Parent directories must already exist; otherwise an **`OSError`** is raised (same as creating a regular file in that location).

Opening a **directory** path (or another non-file that cannot be used as a database file) results in an **`OSError`**.

### `path() -> str`

Returns the path string used to open the database (normalized by the OS path handling underlying the Rust core).

### `register_collection(name: str, fields_json: str, primary_field: str) -> tuple[int, int]`

Registers a **new** collection named `name` with schema version **1**. Collection names are **trimmed** of leading/trailing whitespace; empty names after trimming raise **`ValueError`**.

`fields_json` must be a JSON **array** of field objects (see below). **`primary_field`** must name a **single-segment** top-level field present in that array (record encoding v1).

If parsing or typing fails, **`ValueError`** is raised with a message describing the problem. If the name is already registered, **`ValueError`** is raised.

### `insert(collection_name: str, row: dict) -> None`

Inserts or replaces the latest row for that collection. **`row`** must include every schema field, including the primary key.

### `get(collection_name: str, pk: object) -> dict | None`

Returns the latest row as a **`dict`** of JSON-like values, or **`None`** if no row exists for that primary key.

### `Database.open_in_memory() -> Database` / `Database.open_snapshot_bytes(data: bytes) -> Database` / `snapshot_bytes() -> bytes`

In-memory databases use the same logical format as files. **`snapshot_bytes`** copies the full image (only for in-memory / snapshot-opened databases).

### `collection_names() -> list[str]`

Returns registered collection names in **sorted order** (not insertion order).

## `fields_json` (v1)

`fields_json` is a JSON **array**. Each element is an object with:

| Key | Type | Meaning |
|-----|------|--------|
| **`path`** | array of strings | Field path segments, e.g. `["profile", "name"]`. Each segment must be a JSON string. |
| **`type`** | string or object | Primitive name, or a nested composite (optional, list, object, enum). |

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
fields = """[
  {"path": ["title"], "type": "string"},
  {"path": ["year"], "type": "int64"},
  {"path": ["tags"], "type": {"list": "string"}}
]"""
db.register_collection("books", fields, "title")
```

## Persistence

Registrations are **durable**: after you close the process and open the same path again, `collection_names()` reflects what was registered. This uses the same on-disk catalog as the Rust API (schema segments + superblocks).

## Errors

| Situation | Typical exception |
|-----------|-------------------|
| Invalid JSON, wrong JSON shape, unknown type, duplicate collection name, invalid collection name | **`ValueError`** |
| I/O problems opening the file (missing parent dir, permission, is a directory, etc.) | **`OSError`** |
| Engine reports “not implemented” (should not occur for supported 0.5.x calls) | **`RuntimeError`** |

Always catch **`ValueError`** and **`OSError`** around `open`, `register_collection`, and **`insert`** in production code.

## What is not in 0.5.x yet

- SQL / rich **queries**
- **`register_schema_version`** from Python (Rust only for now)
- Pydantic model inference (you pass explicit `fields_json`)

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
