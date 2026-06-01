# Pydantic and ModelVault

**Audience:** Python developers who already model domain data with Pydantic v2.

ModelVault is **the database for application models**. If your **FastAPI** handlers and services already speak Pydantic, storage should not force a second schema in SQL or untyped JSON. This guide shows how to make **your `BaseModel` the source of truth** for what may be persisted. For **`async def` routes**, pair models with [`AsyncDatabase`](fastapi.md) and `modelvault.models.async_collection`.

## Problem

You define request bodies, settings, and domain objects with Pydantic. Copying that shape into SQL migrations, ORM models, or `data.json` invites drift—especially as fields and constraints evolve.

## Solution

Add ModelVault markers to your `BaseModel` and open a typed collection with `modelvault.models.collection`:

```python
from pydantic import BaseModel, Field
import modelvault

class User(BaseModel):
    __modelvault_primary_key__ = "id"
    __modelvault_indexes__ = [
        modelvault.models.unique("id"),
        modelvault.models.index("email"),
    ]

    id: int
    email: str
    age: int = Field(ge=0, le=150)

db = modelvault.Database.open("app.modelvault")
users = modelvault.models.collection(db, User)
users.insert(User(id=1, email="ada@example.com", age=30))
```

**Result:** Pydantic construction stays familiar; ModelVault enforces storage-level types and engine constraints on write.

## Model definitions

| Marker | Purpose |
|--------|---------|
| `__modelvault_primary_key__` | Field name used as primary key (required) |
| `__modelvault_indexes__` | List of `modelvault.models.index(...)` / `unique(...)` |
| `__modelvault_collection__` | Optional explicit collection name (default: snake_case plural of class name) |

Requires **Pydantic v2** (`BaseModel` subclass). See `python/modelvault/tests/test_models.py` for parity tests in CI.

## Constraints

Combine Pydantic `Field` with ModelVault engine constraints where you need disk-level guarantees:

```python
from typing import Annotated
from pydantic import BaseModel
import modelvault

class Product(BaseModel):
    __modelvault_primary_key__ = "sku"
    sku: str
    qty: Annotated[int, modelvault.models.constrained(min_i64=0)]
```

Invalid `qty` raises on `insert` / `update` before persistence.

## Nested models

Nested Pydantic models map to **object** fields in the catalog (multi-segment paths). Register the parent model with `modelvault.models.collection`; nested types are inferred from annotations.

For advanced path control, see [Models & collections](models_and_collections.md) and [Python guide](python.md).

## Async apps (FastAPI, Starlette)

For asyncio web frameworks, use the same Pydantic model with **`async_collection`** and **`await`** on each call:

```python
db = await modelvault.AsyncDatabase.open("app.modelvault")
users = modelvault.models.async_collection(db, User)
await users.insert(User(id=1, email="ada@example.com", age=30))
row = await users.where(User.email, "ada@example.com").all()
```

Wire `db` and collections through FastAPI lifespan and `Depends` — see the [FastAPI guide](fastapi.md) and [`examples/fastapi_app/main.py`](https://github.com/eddiethedean/modelvault/tree/main/examples/fastapi_app/main.py).

## Queries

Use field names or model attributes on the query builder:

```python
rows = users.where("email", "ada@example.com").all()
rows = users.where(User.email, "ada@example.com").all()
```

## Migration workflows

When you change a model, use planning helpers before applying:

```python
plan = modelvault.models.plan(db, User)  # inspect compatibility
ver = modelvault.models.apply(db, User, force=False)
```

See [Python guide → schema migrations](python.md) for `force`, breaking changes, and collection versioning.

## Optional and union types

Use **`typing.Optional[T]`** (or `T | None` only where your ModelVault version maps unions correctly) for nullable fields. Pydantic `float | None` on a model field may not infer for catalog registration in all versions — prefer:

```python
from typing import Optional
from pydantic import BaseModel

class Book(BaseModel):
    __modelvault_primary_key__ = "title"
    title: str
    rating: Optional[float] = None
```

If registration fails with `issubclass()` errors, simplify optional fields or use a dataclass for the storage model.

## Best practices

1. **One model class per collection** — keeps schema registration predictable.
2. **Declare indexes on the model** — match query patterns (`where`, `and_where`).
3. **Use on-disk paths in production** — `Database.open("app.modelvault")` with backups from the [operations runbook](../ops/operations_and_failure_modes.md).
4. **Keep Pydantic as API validation** — ModelVault adds storage validation; both layers are complementary.
5. **Prefer `modelvault.models` over raw `fields_json`** unless you are generating schemas dynamically.
6. **FastAPI and Starlette** — use `AsyncDatabase` + `async_collection` so route handlers do not block the event loop ([FastAPI guide](fastapi.md)).

## Dataclasses

The same `modelvault.models` API works with `@dataclass` if you prefer no Pydantic dependency. See [Quickstart](quickstart.md).

## Next steps

- [FastAPI](fastapi.md) — `AsyncDatabase`, lifespan, and `async def` dependencies
- [Models & collections](models_and_collections.md) — projections and patches
- [Types matrix](../reference/types.md)
