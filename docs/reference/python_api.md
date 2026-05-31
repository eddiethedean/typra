# Python API

Curated reference for the **`typra`** PyPI package. For tutorials and DB-API examples, see the [Python guide](../guides/python.md).

## Install

```bash
pip install "typra>=1.0.0,<2"
```

## Primary API: `typra.models`

Recommended for applications: define schemas with dataclasses or Pydantic, then use typed collections.

- **`typra.models.collection(db, ModelClass) -> ModelCollection`**
- **`typra.models.index(path)`**, **`typra.models.unique(path)`**, **`typra.models.constrained(...)`**
- **`ModelCollection.insert`**, **`get`**, **`delete`**, query builder via **`ModelCollection.where`**
- **`typra.models.plan`**, **`typra.models.apply`** for migration workflows

See the [Python guide → Models](../guides/python.md) and the package README on GitHub (`python/typra/README.md`).

## Core objects

- **`typra.Database`**
  - `open(path: str, *, read_only: bool = False) -> Database`
  - `open_in_memory() -> Database`
  - `open_snapshot_bytes(data: bytes) -> Database`
  - `open_snapshot(path: str) -> Database`
  - `restore_snapshot(path: str) -> None`
  - `path() -> str`
  - `register_collection(name, fields_json, primary_field, indexes_json=None) -> (collection_id, schema_version)`
  - `register_schema_version(name, fields_json, indexes_json=None, *, force=False) -> schema_version`
  - `plan_schema_version(name, fields_json, indexes_json=None) -> dict`
  - `backfill_top_level_field(name, field, value) -> None`
  - `backfill_field_at_path(name, path, value) -> None` — multi-segment path as `list[str]`
  - `rebuild_indexes(name) -> None`
  - `insert(collection, row: dict) -> None`
  - `get(collection, pk) -> dict | None`
  - `delete(collection, pk) -> None`
  - `export_snapshot(dest_path: str) -> None`
  - `compact() -> None`, `compact_to(dest_path: str) -> None`
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

- Supported subset is documented in [Python guide → DB-API](../guides/python.md#db-api-20-pep-249).
- Non-`SELECT` SQL raises `ValueError`.

## Typing truth

The canonical typing surface for the package lives in:

- `python/typra/typra.pyi` (`https://github.com/eddiethedean/typra/blob/main/python/typra/typra.pyi`)
