# FastAPI and ModelVault

**Audience:** developers building FastAPI services with Pydantic models.

ModelVault is **the database for application models**—a natural fit when your API layer already uses Pydantic. You can persist the same types you validate on the wire, without standing up PostgreSQL for prototypes and small deployments.

## Problem

Typical FastAPI projects need:

- Pydantic models for request and response bodies
- Durable storage on disk for domain data
- Straightforward CRUD without heavy migration tooling
- Fast tests via an in-memory database
- **`async def` route handlers** without blocking the event loop on I/O

## Solution

Use **one ModelVault file per environment**, open **`AsyncDatabase`** in the app lifespan, and inject typed **`async_collection`** handles through FastAPI dependencies. Request handlers stay thin; storage enforces types and constraints on write.

### Application setup

```python
from contextlib import asynccontextmanager
from pathlib import Path

import modelvault
from fastapi import FastAPI

DB_PATH = Path("data/app.modelvault")

@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.db = await modelvault.AsyncDatabase.open(str(DB_PATH))
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

def items_repo(db: modelvault.AsyncDatabase):
    return modelvault.models.async_collection(db, Item)
```

### Dependency injection

```python
from fastapi import Depends, Request

def get_db(request: Request) -> modelvault.AsyncDatabase:
    return request.app.state.db

def get_items(db: modelvault.AsyncDatabase = Depends(get_db)):
    return items_repo(db)
```

Or attach the collection on `app.state` in lifespan and return it from a single dependency (see the runnable example).

### CRUD endpoints

```python
from fastapi import APIRouter, HTTPException

router = APIRouter(prefix="/items")

@router.post("")
async def create_item(body: Item, items=Depends(get_items)):
    await items.insert(body)
    return body

@router.get("/{item_id}")
async def read_item(item_id: int, items=Depends(get_items)):
    row = await items.get(item_id)
    if row is None:
        raise HTTPException(status_code=404, detail="not found")
    return row

@router.get("")
async def list_items(items=Depends(get_items)):
    return await items.all()
```

**Result:** HTTP layer stays Pydantic-native; persistence uses the same types without blocking other requests on database work.

### Parallel reads

On one `AsyncDatabase` handle, multiple **`await`** reads can run concurrently (for example batch lookups):

```python
import asyncio

rows = await asyncio.gather(*(items.get(i) for i in item_ids))
```

Writes remain exclusive per handle. See [Async vs sync](../reference/async_policy.md).

## Transactions

For multi-step writes, use the async transaction context manager:

```python
async with db.transaction():
    await items.insert(Item(id=1, name="a", qty=1))
    await items.insert(Item(id=2, name="b", qty=2))
```

See [Python guide](python.md) for semantics and error mapping.

## Testing

Use an in-memory database in async tests (pytest-asyncio or `anyio`):

```python
import pytest
import modelvault

@pytest.mark.asyncio
async def test_create_item():
    db = await modelvault.AsyncDatabase.open_in_memory()
    items = modelvault.models.async_collection(db, Item)
    await items.insert(Item(id=1, name="test", qty=1))
    assert await items.get(1) is not None
```

Run pytest the same way as the rest of the repo (`make test` from root after `maturin develop`).

## Recommended architecture

| Layer | Responsibility |
|-------|----------------|
| FastAPI routes | HTTP, auth, request validation (`async def`) |
| Pydantic models | API + storage schema (or separate DTOs if you prefer) |
| `modelvault.models.async_collection` | CRUD and queries |
| `app.modelvault` file | Durable state per environment |

!!! tip "When to add PostgreSQL"
    Move to a server database when you need multi-process writers, network replicas, or complex cross-service SQL. Until then, ModelVault keeps prototypes deployable as **one binary + one data file**.

## Production checklist

- Open a **file-backed** path (`await AsyncDatabase.open(...)`), not in-memory
- Configure **backups** — [Operations runbook](../ops/operations_and_failure_modes.md)
- Declare **indexes** for filter fields used in `where`
- Review [Compatibility](../reference/compatibility.md) before upgrades

## Runnable example

[`examples/fastapi_app/main.py`](https://github.com/eddiethedean/modelvault/tree/main/examples/fastapi_app/main.py) — lifespan, dependencies, indexed search, and async CRUD.

## Next steps

- [Pydantic guide](pydantic.md)
- [Python guide](python.md) — queries, migrations, errors
- [Why ModelVault](why_modelvault.md)
