# typra (Python)

[![CI](https://github.com/eddiethedean/typra/actions/workflows/ci.yml/badge.svg)](https://github.com/eddiethedean/typra/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/typra.svg)](https://pypi.org/project/typra/)

Official **CPython** bindings for **Typra** (PyO3 native extension): a typed, embedded database with a Rust core.

## Status (v0.6.x)

You get a durable **schema catalog**, **validation**, nested **row values** (record **v2** on insert; **v1** segments still replay), and **constraints** in a single **`.typra`** file, plus **in-memory** databases and **snapshot** bytes. **SQL / rich queries** are still planned—see the [roadmap](https://github.com/eddiethedean/typra/blob/main/ROADMAP.md).

| Resource | Link |
|----------|------|
| **Repository** | [github.com/eddiethedean/typra](https://github.com/eddiethedean/typra) |
| **Rust crates** | [`typra` on crates.io](https://crates.io/crates/typra) |
| **Full Python guide** | [docs/guide_python.md](https://github.com/eddiethedean/typra/blob/main/docs/guide_python.md) |
| **Getting started** | [docs/guide_getting_started.md](https://github.com/eddiethedean/typra/blob/main/docs/guide_getting_started.md) |
| **Migrating 0.4 → 0.5** | [docs/migration_0.4_to_0.5.md](https://github.com/eddiethedean/typra/blob/main/docs/migration_0.4_to_0.5.md) |
| **Migrating 0.5 → 0.6** | [docs/migration_0.5_to_0.6.md](https://github.com/eddiethedean/typra/blob/main/docs/migration_0.5_to_0.6.md) |
| **Rust module layout** | [docs/03_rust_crate_and_module_layout.md](https://github.com/eddiethedean/typra/blob/main/docs/03_rust_crate_and_module_layout.md) |
| **Changelog** | [CHANGELOG.md](https://github.com/eddiethedean/typra/blob/main/CHANGELOG.md) |

## Requirements

- **CPython 3.9+**
- Wheels use the stable ABI (**`cp39-abi3`**): one wheel per platform, compatible with Python 3.9+ on that platform.

## Install

```bash
pip install "typra>=0.6.0,<0.7"
```

Pin the minor range you test against; pre-1.0 releases may still change APIs or the on-disk format between minors.

## Quick start

```python
import typra

db = typra.Database.open_in_memory()
cid, ver = db.register_collection(
    "books",
    '[{"path": ["title"], "type": "string"}]',
    "title",
)
print("registered", cid, ver)
db.insert("books", {"title": "Typra"})
print(db.get("books", "Typra"))
print(typra.__version__)
```

Output (the version line matches the installed wheel):

```text
registered 1 1
{'title': 'Typra'}
0.6.0
```

On disk, use **`Database.open("app.typra")`** instead; registrations are **persisted** across process restarts for that path.

## API overview

| Member | Description |
|--------|-------------|
| `typra.__version__` | Package version (matches the Rust workspace release). |
| `Database.open(path: str)` | Create or open a database file. Raises `OSError` if the path cannot be opened (e.g. missing parent directory, path is a directory). |
| `db.path() -> str` | Path used to open the database. |
| `db.register_collection(name, fields_json, primary_field) -> tuple[int, int]` | Register a **new** collection (schema version **1**). **`primary_field`** is the top-level field name for the PK. Returns **`(collection_id, schema_version)`**. Names are trimmed; duplicates or bad `fields_json` raise `ValueError`. |
| `db.insert(collection_name, row: dict) -> None` | Insert or replace the latest row (required fields + optional keys per schema). |
| `db.get(collection_name, pk) -> dict \| None` | Latest row or missing. |
| `Database.open_in_memory()` / `Database.open_snapshot_bytes(data)` / `db.snapshot_bytes()` | In-memory DB and byte snapshots. |
| `db.collection_names() -> list[str]` | All registered names, **sorted** alphabetically. |

For behavior details (errors, edge cases, development), see the **[Python user guide](https://github.com/eddiethedean/typra/blob/main/docs/guide_python.md)**.

## `fields_json` (schema descriptor)

`register_collection` expects `fields_json` to be a JSON **array** of objects. Each object describes one field:

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
db.register_collection(
    "items",
    '[{"path": ["x"], "type": {"optional": "int64"}}]',
    "x",
)
```

### Example (multiple fields)

```python
schema = """[
  {"path": ["title"], "type": "string"},
  {"path": ["year"], "type": "int64"},
  {"path": ["tags"], "type": {"list": "string"}}
]"""
db.register_collection("books", schema, "title")
```

## Exceptions

- **`ValueError`**: invalid JSON, wrong shape, unknown type, invalid collection name, duplicate collection name, validation failures, or format/schema errors from the engine when registering.
- **`OSError`**: I/O failures when opening the database file.
- **`RuntimeError`**: reserved for engine “not implemented” paths (unexpected for supported 0.6.x calls).

## Building from source

You need **Rust**, **Python 3.9+**, and **[maturin](https://www.maturin.rs/)**. From the repo’s **`python/typra`** directory:

```bash
maturin develop --release
pytest -q
```

From the repository root, **`make check-full`** runs Rust + Python checks, tests, and **`make verify-doc-examples`** (validates documented command output). See also **[python/README.md](https://github.com/eddiethedean/typra/blob/main/python/README.md)** (workspace layout for contributors).
