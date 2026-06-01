# Async vs sync API policy

## Production contract

**The synchronous `Database` API remains the default production surface** for Rust and Python:

- Rust: `modelvault::Database` (re-exported from `modelvault-core`)
- Python: `modelvault.Database`

Getting-started guides, CLI/desktop examples, and operational runbooks assume sync open → insert → query → transaction → checkpoint → compact.

**Exception:** the [FastAPI guide](../guides/fastapi.md) and [`examples/fastapi_app/`](../examples/index.md) use **`AsyncDatabase`** and `async def` handlers — that is the recommended path for asyncio web apps.

## Python asyncio API

Python exposes a **parallel** asyncio surface that does not change the sync `Database` type:

- `modelvault.AsyncDatabase` — `await AsyncDatabase.open(...)`, `await db.insert(...)`, etc.
- `modelvault.AsyncTransaction` — `async with db.transaction():`
- `modelvault.models.async_collection`, `async_plan`, `async_apply`
- `AsyncCollection` / `AsyncQuery` — query builder with `await ...all()`

```python
import modelvault

async def main() -> None:
    db = await modelvault.AsyncDatabase.open_in_memory()
    await db.register_collection(
        "books",
        '[{"path": ["title"], "type": "string"}]',
        "title",
    )
    await db.insert("books", {"title": "Hello"})
    row = await db.get("books", "Hello")
```

### Execution model

- Operations run the **same sync engine** on a **thread pool** (via Tokio `spawn_blocking` inside the extension).
- This **releases the GIL** during engine work so asyncio event loops stay responsive.
- This is **not** native async file I/O in `modelvault-core`; durability and locking semantics match the sync API.

### Concurrency

Bindings wrap the engine in an **`RwLock`** (not a single `Mutex`):

| Lock | Operations | Effect |
|------|------------|--------|
| **Shared (read)** | `get`, `query`, `explain`, `collection_names`, `plan_schema_version`, `snapshot_bytes`, `path`, … | Multiple tasks or threads can run read work **in parallel** on the same handle |
| **Exclusive (write)** | `insert`, `delete`, schema/migration writes, compaction, `export_snapshot`, transaction begin/commit | One mutator at a time |

Additional rules:

- While a **transaction** is open on a handle, all operations on that handle take the **exclusive** lock so reads observe the transaction’s staged snapshot.
- The **on-disk file** is still **single-writer** per `.modelvault` path (advisory `*.writer.lock`); cross-process scaling uses separate processes (e.g. `read_only=True`), not multiple writers in one file.
- Use **one `AsyncDatabase` (or `Database`) per app** (e.g. FastAPI lifespan), same as before.

**Practical guidance:** For read-heavy async handlers, prefer `await asyncio.gather(...)` over sequential `await` loops — probes on in-memory workloads often show **~2×** wall-clock improvement for large read batches. Single `get` latency is unchanged. `gather` on **writes** does not parallelize engine work (scheduling may still reduce asyncio overhead).

**Probe:** `python/modelvault/tests/bench_async_concurrency.py` (after `make python-develop`).

### Example

See [`examples/fastapi_app/main.py`](https://github.com/eddiethedean/modelvault/tree/main/examples/fastapi_app/main.py) for a FastAPI service using `AsyncDatabase`.

## Optional Rust async facade

The `modelvault` crate also exposes an **optional** Rust wrapper behind the **`async`** feature:

```toml
[dependencies]
modelvault = { version = "0.15", features = ["async"] }
```

```rust
use modelvault::AsyncDatabase;

#[tokio::main]
async fn main() -> Result<(), modelvault::DbError> {
    let db = AsyncDatabase::open("app.modelvault").await?;
    let names = db.collection_names().await?;
    Ok(())
}
```

The Rust `AsyncDatabase` uses the same **read/write lock** model (`RwLock` + transaction depth) as the Python bindings.

CI runs `cargo test -p modelvault --features async` via `make check-2p0-ready` to keep the Rust feature compiling.

## What we are not committing to yet

- Native async I/O throughout the storage layer (`async trait Store`, etc.)
- Async `modelvault.dbapi` (read-only DB-API remains sync)
- Automatic downgrade / migration tooling tied to async-only APIs

## Future direction

Internal storage and query execution remain structured so **true async I/O** can land later without rewriting the catalog or file format. Any change beyond thread-pool wrappers will be semver-visible and documented before becoming the recommended default.
