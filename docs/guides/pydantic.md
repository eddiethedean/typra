# Pydantic and Typra

**Goal:** Typra should feel like a natural extension of Pydantic — your models are your database schema.

## Problem

You already define API and domain types with Pydantic. Duplicating that shape in SQL or JSON schema is error-prone.

## Solution

Add Typra markers to your `BaseModel` and use `typra.models.collection`:

```python
from pydantic import BaseModel, Field
import typra

class User(BaseModel):
    __typra_primary_key__ = "id"
    __typra_indexes__ = [
        typra.models.unique("id"),
        typra.models.index("email"),
    ]

    id: int
    email: str
    age: int = Field(ge=0, le=150)

db = typra.Database.open("app.typra")
users = typra.models.collection(db, User)
users.insert(User(id=1, email="ada@example.com", age=30))
```

**Result:** Pydantic construction stays familiar; Typra enforces storage-level types and engine constraints on write.

## Model definitions

| Marker | Purpose |
|--------|---------|
| `__typra_primary_key__` | Field name used as primary key (required) |
| `__typra_indexes__` | List of `typra.models.index(...)` / `unique(...)` |
| `__typra_collection__` | Optional explicit collection name (default: snake_case plural of class name) |

Requires **Pydantic v2** (`BaseModel` subclass). See `python/typra/tests/test_models.py` for parity tests in CI.

## Constraints

Combine Pydantic `Field` with Typra engine constraints where you need disk-level guarantees:

```python
from typing import Annotated
from pydantic import BaseModel
import typra

class Product(BaseModel):
    __typra_primary_key__ = "sku"
    sku: str
    qty: Annotated[int, typra.models.constrained(min_i64=0)]
```

Invalid `qty` raises on `insert` / `update` before persistence.

## Nested models

Nested Pydantic models map to **object** fields in the catalog (multi-segment paths). Register the parent model with `typra.models.collection`; nested types are inferred from annotations.

For advanced path control, see [Models & collections](models_and_collections.md) and [Python guide](python.md).

## Queries

Use field names or model attributes on the query builder:

```python
rows = users.where("email", "ada@example.com").all()
rows = users.where(User.email, "ada@example.com").all()
```

## Migration workflows

When you change a model, use planning helpers before applying:

```python
plan = typra.models.plan(db, User)  # inspect compatibility
ver = typra.models.apply(db, User, force=False)
```

See [Python guide → schema migrations](python.md) for `force`, breaking changes, and collection versioning.

## Optional and union types

Use **`typing.Optional[T]`** (or `T | None` only where your Typra version maps unions correctly) for nullable fields. Pydantic `float | None` on a model field may not infer for catalog registration in all versions — prefer:

```python
from typing import Optional
from pydantic import BaseModel

class Book(BaseModel):
    __typra_primary_key__ = "title"
    title: str
    rating: Optional[float] = None
```

If registration fails with `issubclass()` errors, simplify optional fields or use a dataclass for the storage model.

## Best practices

1. **One model class per collection** — keeps schema registration predictable.
2. **Declare indexes on the model** — match query patterns (`where`, `and_where`).
3. **Use on-disk paths in production** — `Database.open("app.typra")` with backups from the [operations runbook](../ops/operations_and_failure_modes.md).
4. **Keep Pydantic as API validation** — Typra adds storage validation; both layers are complementary.
5. **Prefer `typra.models` over raw `fields_json`** unless you are generating schemas dynamically.

## Dataclasses

The same `typra.models` API works with `@dataclass` if you prefer no Pydantic dependency. See [Quickstart](quickstart.md).

## Next steps

- [FastAPI](fastapi.md) — wire collections into dependencies
- [Models & collections](models_and_collections.md) — projections and patches
- [Types matrix](../reference/types.md)
