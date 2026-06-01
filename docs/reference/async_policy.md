# Async vs sync API policy (1.0)

## Production contract

**ModelVault 1.0 treats the synchronous `Database` API as the supported production surface** for both Rust and Python:

- Rust: `modelvault::Database` (re-exported from `modelvault-core`)
- Python: `modelvault.Database`

All documented getting-started flows, E2E tests, and operational guidance assume sync open → insert → query → transaction → checkpoint → compact.

## Optional Rust async facade

The `modelvault` crate exposes an **optional**, **experimental** async wrapper behind the **`async`** feature:

```toml
[dependencies]
modelvault = { version = "0.14", features = ["async"] }
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

Characteristics:

- Wraps the same sync engine with `spawn_blocking` (or equivalent) for IO-heavy paths.
- **Not** exposed in Python bindings.
- **Not** required for 1.0 readiness; CI runs `cargo test -p modelvault --features async` via `make check-2p0-ready` to keep the feature compiling.

## What we are not committing to in 1.0

- Native async IO throughout the storage layer
- Python `async def` methods on `Database`
- Dual sync/async parity guarantees beyond compile-time coverage for the Rust feature

## Future direction (1.1+)

Internal storage and query execution remain structured so a first-class async story can land without rewriting the catalog or file format. Any expansion beyond the current `AsyncDatabase` wrapper will be semver-visible and documented before becoming the recommended default.
