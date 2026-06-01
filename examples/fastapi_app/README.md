# FastAPI example

**Problem:** A small API needs persistence without PostgreSQL for early deployments.

**Solution:** ModelVault file on disk + Pydantic models shared between HTTP bodies and storage.

## Setup

From the repository root:

```bash
make python-develop
.venv/bin/pip install "fastapi>=0.100" "uvicorn>=0.23"
```

## Run

**Sync routes** (default):

```bash
.venv/bin/uvicorn examples.fastapi_app.main:app --reload
```

**Async routes** (`AsyncDatabase`, concurrent reads on one handle):

```bash
.venv/bin/uvicorn examples.fastapi_app.main_async:app --reload
```

Try:

```bash
curl -s -X POST localhost:8000/items \
  -H 'content-type: application/json' \
  -d '{"id":1,"name":"widget","qty":3}'
curl -s localhost:8000/items/1
curl -s localhost:8000/items/search/widget
```

**Result:** data persists in `examples/fastapi_app/items.modelvault`.

## What it demonstrates

- Lifespan hook opens ModelVault once per process
- FastAPI `Depends` injects the model collection
- Indexed lookup via `where(Item.name, ...)`
- **`main_async.py`**: `async def` handlers, `AsyncDatabase`, optional `asyncio.gather` for parallel reads

Docs: [FastAPI guide](https://modelvault.readthedocs.io/en/latest/guides/fastapi/) · [Async policy](https://modelvault.readthedocs.io/en/latest/reference/async_policy/)
