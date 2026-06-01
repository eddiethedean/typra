# FastAPI and ModelVault

**Audience:** developers building FastAPI services with Pydantic models.

ModelVault is **the database for application models**—a natural fit when your API layer already uses Pydantic. You can persist the same types you validate on the wire, without standing up PostgreSQL for prototypes and small deployments.

## Problem

Typical FastAPI projects need:

- Pydantic models for request and response bodies
- Durable storage on disk for domain data
- Straightforward CRUD without heavy migration tooling
- Fast tests via an in-memory database

## Solution

Use **one ModelVault file per environment** and inject a `Database` (or typed `modelvault.models` collections) through FastAPI dependencies. Request handlers stay thin; storage enforces types and constraints on write.

### Application setup

```python
from contextlib import asynccontextmanager
from pathlib import Path

import modelvault
from fastapi import FastAPI

DB_PATH = Path("data/app.modelvault")

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db = modelvault.Database.open(str(DB_PATH))
    yield
    # Database is closed when process exits; for explicit close, add cleanup here.

app = FastAPI(lifespan=lifespan)
```

### Models and collection

```python
from pydantic import BaseModel
import modelvault

class Item(BaseModel):
    __modelvault_primary_key__ = "id"
    __modelvault_indexes__ = [modelvault.models.index("name")]
    id: int
    name: str
    qty: int

def items_repo(db: modelvault.Database):
    return modelvault.models.collection(db, Item)
```

### Dependency injection

```python
from fastapi import Depends, Request

def get_db(request: Request) -> modelvault.Database:
    return request.app.state.db

def get_items(db: modelvault.Database = Depends(get_db)):
    return items_repo(db)
```

### CRUD endpoints

```python
from fastapi import APIRouter, HTTPException

router = APIRouter(prefix="/items")

@router.post("")
def create_item(body: Item, items=Depends(get_items)):
    items.insert(body)
    return body

@router.get("/{item_id}")
def read_item(item_id: int, items=Depends(get_items)):
    row = items.get(item_id)
    if row is None:
        raise HTTPException(status_code=404, detail="not found")
    return row

@router.get("")
def list_items(items=Depends(get_items)):
    return items.all()
```

**Result:** HTTP layer stays Pydantic-native; persistence uses the same types.

## Transactions

For multi-step writes, use the database transaction context manager:

```python
with db.transaction():
    items.insert(Item(id=1, name="a", qty=1))
    items.insert(Item(id=2, name="b", qty=2))
```

See [Python guide](python.md) for semantics and error mapping.

## Testing

Use in-memory databases in tests — no temp files required:

```python
import modelvault

def test_create_item():
    db = modelvault.Database.open_in_memory()
    items = modelvault.models.collection(db, Item)
    items.insert(Item(id=1, name="test", qty=1))
    assert items.get(1) is not None
```

Run pytest the same way as the rest of the repo (`make test` from root after `maturin develop`).

## Recommended architecture

| Layer | Responsibility |
|-------|----------------|
| FastAPI routes | HTTP, auth, request validation |
| Pydantic models | API + storage schema (or separate DTOs if you prefer) |
| `modelvault.models.collection` | CRUD and queries |
| `app.modelvault` file | Durable state per environment |

!!! tip "When to add PostgreSQL"
    Move to a server database when you need multi-process writers, network replicas, or complex cross-service SQL. Until then, ModelVault keeps prototypes deployable as **one binary + one data file**.

## Production checklist

- Open a **file-backed** path (`Database.open`), not `:memory:`
- Configure **backups** — [Operations runbook](../ops/operations_and_failure_modes.md)
- Declare **indexes** for filter fields used in `where`
- Review [Compatibility](../reference/compatibility.md) before upgrades

## Next steps

- [Pydantic guide](pydantic.md)
- [Python guide](python.md) — queries, migrations, errors
- [Why ModelVault](why_modelvault.md)
