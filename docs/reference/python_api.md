# Python API

Curated reference for the **`modelvault`** PyPI package — **store Pydantic and dataclass models** via `modelvault.models`.

Tutorials: [Pydantic guide](../guides/pydantic.md) · [Python guide](../guides/python.md) · [Examples](../examples/index.md)

## Install

```bash
pip install "modelvault>=0.14.0,<0.15"
```

## Primary API: `modelvault.models`

Recommended for applications: define schemas with dataclasses or Pydantic, then use typed collections.

- **`modelvault.models.collection(db, ModelClass) -> ModelCollection`**
- **`modelvault.models.index(path)`**, **`modelvault.models.unique(path)`**, **`modelvault.models.constrained(...)`**
- **`ModelCollection.insert`**, **`get`**, **`delete`**, query builder via **`ModelCollection.where`**
- **`modelvault.models.plan`**, **`modelvault.models.apply`** for migration workflows

See the [Python guide → Models](../guides/python.md) and the package README on GitHub (`python/modelvault/README.md`).

## Core objects

- **`modelvault.Database`**
  - `open(path: str, *, read_only: bool = False) -> Database`
  - `open_in_memory() -> Database`
  - `open_snapshot_bytes(data: bytes) -> Database`
  - `open_snapshot(path: str) -> Database`
  - `restore_snapshot(snapshot_path: str, dest_path: str) -> None`
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

### Concurrency (`Database`)

On one handle, **reads** take a shared lock and may overlap across threads; **writes** and **open transactions** take an exclusive lock. This matches the on-disk **single-writer-per-file** policy (advisory locks across processes are separate). Details: [Async policy](async_policy.md#concurrency) (same rules apply to sync handles).

## `AsyncDatabase` (experimental)

Parallel asyncio surface — `await AsyncDatabase.open(...)`, `await db.get(...)`, `async with db.transaction():`, plus `modelvault.models.async_collection` and `AsyncCollection` / `AsyncQuery`.

- Same method names as `Database` where applicable (returns awaitables).
- Engine work runs on a **thread pool**; **concurrent read** `await`s overlap; writes serialize.
- Policy and limits: [Async policy](async_policy.md).

## Errors

ModelVault maps engine errors to standard Python exceptions (`ValueError`, `OSError`, `RuntimeError`), and also provides **more specific subclasses** you can match on:

- `modelvault.ModelVaultFormatError` (subclass of `ValueError`)
- `modelvault.ModelVaultSchemaError` (subclass of `ValueError`)
- `modelvault.ModelVaultValidationError` (subclass of `ValueError`)
- `modelvault.ModelVaultQueryError` (subclass of `ValueError`)
- `modelvault.ModelVaultTransactionError` (subclass of `RuntimeError`)

## Query builder (`Collection`)

- `where(path, value)` (equality)
- `and_where(path, value)`
- `lt_where(path, value)`, `lte_where`, `gt_where`, `gte_where`
- `or_where(other_query)`
- `order_by(path, *, desc=False)`
- `limit(n)`
- `explain() -> str`
- `all(fields: list[str] | None = None) -> list[dict]` (subset projection)

## DB-API (`modelvault.dbapi`)

ModelVault ships a **read-only** DB-API 2.0 adapter for a minimal `SELECT` subset.

- Supported subset is documented in [Python guide → DB-API](../guides/python.md#db-api-20-pep-249).
- Non-`SELECT` SQL raises `ValueError`.

## Typing truth

The canonical typing surface for the package lives in:

- `python/modelvault/modelvault.pyi` (`https://github.com/eddiethedean/modelvault/blob/main/python/modelvault/modelvault.pyi`)
