# Python API (curated)

This page is a curated reference for Typra’s Python surface (`typra` on PyPI).

For worked examples and the SQL/DB-API subset, see the [Python guide](../guides/python.md).

## Install

```bash
pip install "typra>=0.13.0,<0.14"
```

## Core objects

- **`typra.Database`**
  - `open(path: str, *, read_only: bool = False) -> Database`
  - `open_in_memory() -> Database`
  - `open_snapshot_bytes(data: bytes) -> Database`
  - `open_snapshot(path: str) -> Database`
  - `path() -> str`
  - `register_collection(name, fields_json, primary_field, indexes_json=None) -> (collection_id, schema_version)`
  - `insert(collection, row: dict) -> None`
  - `get(collection, pk) -> dict | None`
  - `delete(collection, pk) -> None`
  - `export_snapshot(dest_path: str) -> None`
  - `transaction()` context manager (`with db.transaction(): ...`)
  - `collection_names() -> list[str]`
  - `collection(name) -> Collection` (typed query builder)

## Errors

Typra maps engine errors to standard Python exceptions (`ValueError`, `OSError`, `RuntimeError`), and also provides **more specific subclasses** you can match on:

- `typra.TypraFormatError` (subclass of `ValueError`)
- `typra.TypraSchemaError` (subclass of `ValueError`)
- `typra.TypraValidationError` (subclass of `ValueError`)
- `typra.TypraQueryError` (subclass of `ValueError`)
- `typra.TypraTransactionError` (subclass of `RuntimeError`)

## Query builder (`Collection`)

- `where(path, value)` (equality)
- `and_where(path, value)`
- `limit(n)`
- `explain() -> str`
- `all(fields: list[str] | None = None) -> list[dict]` (subset projection)

## DB-API (`typra.dbapi`)

Typra ships a **read-only** DB-API 2.0 adapter for a minimal `SELECT` subset.

- Supported subset is documented in [Python guide → DB-API](../guides/python.md#db-api-20-pep-249-and-sqlalchemy).
- Non-`SELECT` SQL raises `ValueError`.

## Typing truth

The canonical typing surface for the package lives in:

- `python/typra/typra.pyi` (`https://github.com/eddiethedean/typra/blob/main/python/typra/typra.pyi`)

