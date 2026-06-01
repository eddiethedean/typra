# ModelVault vs JSON files

JSON (or YAML) on disk is the fastest way to persist data — until you need **structure, queries, and integrity**.

## Problem

A `data.json` file works for prototypes:

```json
{"users": [{"id": 1, "name": "Ada"}, {"id": "two", "name": "Bob"}]}
```

Pain appears quickly:

- **No validation** — bad types slip in silently
- **No indexes** — every lookup scans the file
- **No transactions** — partial writes corrupt state
- **Schema drift** — old rows lack new fields; migrations are manual merges
- **Concurrency** — whole-file rewrite races

## Solution

ModelVault keeps the **single-file** ergonomics but adds engine guarantees:

```python
from pydantic import BaseModel
import modelvault

class User(BaseModel):
    __modelvault_primary_key__ = "id"
    __modelvault_indexes__ = [modelvault.models.index("name")]
    id: int
    name: str

db = modelvault.Database.open("app.modelvault")
users = modelvault.models.collection(db, User)
users.insert(User(id=1, name="Ada"))
# Invalid: users.insert(User(id="x", name="Ada"))  # fails on write
```

**Result:** same deployment story (one file), with validation, indexes, and crash-safe segments.

## Comparison

| Topic | JSON files | ModelVault |
|-------|------------|-------|
| Validation | Manual | On write |
| Indexes | None | Declared on model |
| Queries | Load + filter in Python | Index-backed `where` |
| Durability | Rewrite whole file | Transactions + checkpoints |
| Schema evolution | Custom scripts | `plan` / `apply` + compatibility |
| Maintenance cost | Grows with size | Engine handles encoding |

## When JSON still wins

- Truly **append-only logs** where schema never changes
- Human-edited config you want in git verbatim
- Interchange with tools that only speak JSON

## When ModelVault wins

- Application **domain data** that must stay consistent
- You need **indexed lookups** without loading everything
- You want **one file** but outgrew `json.load`

## Related

- [Quickstart](../guides/quickstart.md)
- [Operations runbook](../ops/operations_and_failure_modes.md) — backup and recovery
