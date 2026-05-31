# FastAPI and Typra

**Goal:** make Typra the easiest database for **small FastAPI services** that should not require PostgreSQL for early deployments.

## Problem

You want:

- Pydantic models for request/response bodies
- Persistent storage on disk
- No migration framework boilerplate for simple CRUD
- Easy testing with an in-memory database

## Solution

Use a **single Typra file** per environment and inject a `Database` (or typed collections) via FastAPI dependencies.

### Application setup

```python
from contextlib import asynccontextmanager
from pathlib import Path

import typra
from fastapi import FastAPI

DB_PATH = Path("data/app.typra")

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db = typra.Database.open(str(DB_PATH))
    yield
    # Database is closed when process exits; for explicit close, add cleanup here.

app = FastAPI(lifespan=lifespan)
```

### Models and collection

```python
from pydantic import BaseModel
import typra

class Item(BaseModel):
    __typra_primary_key__ = "id"
    __typra_indexes__ = [typra.models.index("name")]
    id: int
    name: str
    qty: int

def items_repo(db: typra.Database):
    return typra.models.collection(db, Item)
```

### Dependency injection

```python
from fastapi import Depends, Request

def get_db(request: Request) -> typra.Database:
    return request.app.state.db

def get_items(db: typra.Database = Depends(get_db)):
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
import typra

def test_create_item():
    db = typra.Database.open_in_memory()
    items = typra.models.collection(db, Item)
    items.insert(Item(id=1, name="test", qty=1))
    assert items.get(1) is not None
```

Run pytest the same way as the rest of the repo (`make test` from root after `maturin develop`).

## Recommended architecture

| Layer | Responsibility |
|-------|----------------|
| FastAPI routes | HTTP, auth, request validation |
| Pydantic models | API + storage schema (or separate DTOs if you prefer) |
| `typra.models.collection` | CRUD and queries |
| `app.typra` file | Durable state per environment |

!!! tip "When to add PostgreSQL"
    Move to a server database when you need multi-process writers, network replicas, or complex cross-service SQL. Until then, Typra keeps prototypes deployable as **one binary + one data file**.

## Production checklist

- Open a **file-backed** path (`Database.open`), not `:memory:`
- Configure **backups** — [Operations runbook](../ops/operations_and_failure_modes.md)
- Declare **indexes** for filter fields used in `where`
- Review [Compatibility](../reference/compatibility.md) before upgrades

## Next steps

- [Pydantic guide](pydantic.md)
- [Python guide](python.md) — queries, migrations, errors
- [Why Typra](why_typra.md)
