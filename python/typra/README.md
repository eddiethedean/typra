# typra (Python)

[![CI](https://github.com/eddiethedean/typra/actions/workflows/ci.yml/badge.svg)](https://github.com/eddiethedean/typra/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/typra.svg)](https://pypi.org/project/typra/)

**Typra** is a typed, embedded database with a Rust core. This package is the official **CPython** bindings (PyO3, native extension).

**In 0.4.x** you get a durable **schema catalog** in a single `.typra` file: open a database, **register collections** with a JSON field schema (`fields_json`), and list names. **Record insert/get and queries** are not available yet; they are planned for later releases ([roadmap](https://github.com/eddiethedean/typra/blob/main/ROADMAP.md)).

| | |
|--|--|
| **Repository** | [github.com/eddiethedean/typra](https://github.com/eddiethedean/typra) |
| **Full Python guide** | [docs/guide_python.md](https://github.com/eddiethedean/typra/blob/main/docs/guide_python.md) |
| **Changelog** | [CHANGELOG.md](https://github.com/eddiethedean/typra/blob/main/CHANGELOG.md) |

## Requirements

- **CPython 3.9+**
- Wheels use the stable ABI (**`cp39-abi3`**): one wheel per platform, compatible with Python 3.9 and newer on that platform.

## Install

```bash
pip install "typra>=0.4.0,<0.5"
```

Pin the minor range you test against; pre-1.0 releases may still change APIs or the on-disk format between minors.

## Quick start

```python
import typra

db = typra.Database.open("app.typra")
cid, ver = db.register_collection(
    "books",
    '[{"path": ["title"], "type": "string"}]',
)
assert cid == 1 and ver == 1
assert db.collection_names() == ["books"]
```

Registrations are **persisted**: reopening the same path shows the same catalog.

## API overview

| Member | Description |
|--------|-------------|
| `typra.__version__` | Package version (matches the Rust workspace release). |
| `Database.open(path: str)` | Create or open a database file. Raises `OSError` if the path cannot be opened (e.g. missing parent directory, path is a directory). |
| `db.path() -> str` | Path used to open the database. |
| `db.register_collection(name, fields_json) -> tuple[int, int]` | Register a **new** collection (schema version **1**). Returns **`(collection_id, schema_version)`**. Names are trimmed; duplicates or bad `fields_json` raise `ValueError`. |
| `db.collection_names() -> list[str]` | All registered names, **sorted** alphabetically. |

For behavior details (errors, edge cases, development), see the **[Python user guide](https://github.com/eddiethedean/typra/blob/main/docs/guide_python.md)**.

## `fields_json` (v1)

`register_collection` expects `fields_json` to be a JSON **array** of objects. Each object describes one field:

- **`path`**: JSON array of strings (path segments), e.g. `["profile", "name"]`.
- **`type`**: either a **primitive** name or a **composite** object.

**Primitives:** `"bool"`, `"int64"`, `"uint64"`, `"float64"`, `"string"`, `"bytes"`, `"uuid"`, `"timestamp"`.

**Composites:**

- Optional: `{"optional": <inner>}`
- List: `{"list": <inner>}`
- Object: `{"object": [ … same shape as top-level field objects … ]}`
- Enum: `{"enum": ["a", "b"]}` (variants must be strings)

### Example (nested)

```python
db.register_collection(
    "items",
    '[{"path": ["x"], "type": {"optional": "int64"}}]',
)
```

### Example (multiple fields)

```python
schema = """[
  {"path": ["title"], "type": "string"},
  {"path": ["year"], "type": "int64"},
  {"path": ["tags"], "type": {"list": "string"}}
]"""
db.register_collection("books", schema)
```

## Exceptions

- **`ValueError`**: invalid JSON, wrong shape, unknown type, invalid collection name, duplicate collection name, or format/schema errors from the engine when registering.
- **`OSError`**: I/O failures when opening the database file.
- **`RuntimeError`**: reserved for engine “not implemented” paths (unexpected for supported 0.4.x calls).

## Building from source

You need **Rust**, **Python 3.9+**, and **[maturin](https://www.maturin.rs/)**. From the repo’s `python/typra` directory:

```bash
maturin develop --release
pytest -q
```

Or from the repository root, run **`make check-full`** (Rust + Python checks and tests). See also **[python/README.md](https://github.com/eddiethedean/typra/blob/main/python/README.md)**.
