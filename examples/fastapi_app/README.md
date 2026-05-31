# FastAPI example

**Problem:** A small API needs persistence without PostgreSQL for early deployments.

**Solution:** Typra file on disk + Pydantic models shared between HTTP bodies and storage.

## Setup

From the repository root:

```bash
make python-develop
.venv/bin/pip install "fastapi>=0.100" "uvicorn>=0.23"
```

## Run

```bash
.venv/bin/uvicorn examples.fastapi_app.main:app --reload
```

Try:

```bash
curl -s -X POST localhost:8000/items \
  -H 'content-type: application/json' \
  -d '{"id":1,"name":"widget","qty":3}'
curl -s localhost:8000/items/1
curl -s localhost:8000/items/search/widget
```

**Result:** data persists in `examples/fastapi_app/items.typra`.

## What it demonstrates

- Lifespan hook opens Typra once per process
- FastAPI `Depends` injects the model collection
- Indexed lookup via `where(Item.name, ...)`

Docs: [FastAPI guide](https://typra.readthedocs.io/en/latest/guides/fastapi/)
