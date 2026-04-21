# typra (Python)

[![CI](https://github.com/eddiethedean/typra/actions/workflows/ci.yml/badge.svg)](https://github.com/eddiethedean/typra/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/typra.svg)](https://pypi.org/project/typra/)

Python bindings for **Typra**. **0.4.0** exposes `typra.__version__`, `Database.open`, `register_collection`, and `collection_names`. Record APIs will ship in later releases.

## Install

```bash
pip install "typra>=0.4.0,<0.5"
```

Supports **CPython 3.9+**. Wheels are published as **`cp39-abi3`** (one per platform for CPython 3.9+).

## Example

```python
import typra

db = typra.Database.open("app.typra")
fields = '[{"path": ["title"], "type": "string"}]'
cid, ver = db.register_collection("books", fields)
assert cid == 1 and ver == 1
assert db.collection_names() == ["books"]
```

### `fields_json` (v1)

`register_collection` takes a JSON **array** of objects, each with:

- **`path`**: array of UTF-8 strings (field path segments), e.g. `["profile", "name"]`.
- **`type`**: either a primitive name or a nested object:
  - Primitives: `"bool"`, `"int64"`, `"uint64"`, `"float64"`, `"string"`, `"bytes"`, `"uuid"`, `"timestamp"`.
  - Optional: `{"optional": <inner type value>}`.
  - List: `{"list": <inner type value>}`.
  - Object: `{"object": [ ... same field objects as top-level ... ]}`.
  - Enum: `{"enum": ["a", "b"]}`.

```python
db.register_collection(
    "items",
    '[{"path": ["x"], "type": {"optional": "int64"}}]',
)
```
