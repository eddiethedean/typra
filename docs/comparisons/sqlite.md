# Typra vs SQLite

Both are **embedded, single-file, zero-server** databases. The difference is what you optimize for: **SQL tables** vs **application models**.

## Problem

You have Pydantic or dataclass models in Python (or structs in Rust). With SQLite you typically:

1. Design SQL tables separately from your models
2. Maintain migrations (Alembic, hand-written SQL, etc.)
3. Map rows ↔ models in application code
4. Encode nested objects as JSON blobs or flattened columns

That works — it is the industry default — but it is not the fastest path when your source of truth is already **typed application data**.

## Solution

Typra registers a **collection** from your model class. Inserts validate against the schema catalog; nested paths are typed in the engine.

```python
from pydantic import BaseModel
import typra

class User(BaseModel):
    __typra_primary_key__ = "id"
    id: int
    name: str

db = typra.Database.open("app.typra")
users = typra.models.collection(db, User)
users.insert(User(id=1, name="Ada"))
```

**Result:** one file, no separate migration DSL for the common case, validation before persistence.

## Side-by-side

| Topic | SQLite | Typra |
|-------|--------|-------|
| Schema | `CREATE TABLE` + migrations | Model class + catalog versions |
| Nested data | Often JSON in `TEXT` | Typed object paths in catalog |
| Validation | `CHECK`, app layer | Engine on every write |
| Queries | Full SQL | Model query builder + growing SQL subset |
| Ecosystem | Universal | Growing; model-first |

## When SQLite still wins

- You need **portable SQL** every DBA already knows
- Heavy **ad-hoc reporting** with joins across dozens of legacy tables
- Existing **SQLite** tooling and drivers are mandatory

## When Typra wins

- Your app is **model-first** (Pydantic, dataclasses, Rust structs)
- You want **validation and indexes** without ORM ceremony
- You ship a **single typed file** with the application

## Example: nested object

=== "SQLite (typical)"

    ```sql
    CREATE TABLE orders (
      id INTEGER PRIMARY KEY,
      customer_json TEXT NOT NULL
    );
    -- App serializes Customer pydantic model to JSON string
    ```

=== "Typra"

    ```python
    class Customer(BaseModel):
        name: str
        tier: str

    class Order(BaseModel):
        __typra_primary_key__ = "id"
        id: int
        customer: Customer  # nested object in schema
    ```

## Related

- [Why Typra](../guides/why_typra.md)
- [Comparisons overview](index.md)
- [Compatibility](../reference/compatibility.md)
