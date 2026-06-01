# Rust API

Application-focused embedded database for Rust — model-driven schemas, validation, migrations, and single-file deploy.

What to import and what’s stable in ModelVault’s Rust crates. Product context: [Why ModelVault](../guides/why_modelvault.md) · [Quickstart](../guides/quickstart.md).

| Crate | docs.rs |
|-------|---------|
| **`modelvault`** (facade — use this) | [docs.rs/modelvault](https://docs.rs/modelvault) |
| **`modelvault-core`** (engine) | [docs.rs/modelvault-core](https://docs.rs/modelvault-core) |

## Recommended crate

Most applications should depend on **`modelvault`**:

```toml
[dependencies]
modelvault = "0.15"
```

It re-exports the engine and (by default) enables `#[derive(DbModel)]`.

## Core types

- **`Database`**: open/create, register collections/schema versions, CRUD, transactions, queries, snapshots/compaction.
- **`OpenOptions`**: open configuration (including recovery mode).
- **`RecoveryMode`**: `AutoTruncate` (best-effort salvage) vs `Strict` (fail-fast).

## Schema and values

- **`FieldDef`**: field declaration (path, type, constraints).
- **`Type`**: schema type (scalars + optional/list/object/enum).
- **`schema::FieldPath`**: field path segments.
- **`RowValue`**, **`ScalarValue`**: runtime values validated against `Type`.

## Errors

- **`DbError`**: top-level error for I/O, decode/replay, and validation failures.
- **`ValidationError`**: structured validation failures (paths, expected vs actual).

## Query surface (typed)

ModelVault’s primary Rust query surface is typed (non-SQL):

- Predicate composition (`Eq`, `And`, `Or`, ranges)
- `limit`, `order_by`, `explain`
- Streaming iteration (`query_iter`) for bounded-memory execution shapes

For detailed semantics, see [Query planner and execution spec](../specs/query_planner.md) and [Types matrix](types.md).

## Concurrency

- **`Database`** in one thread (or external synchronization): the engine exposes read methods on `&self` and writes on `&mut self`.
- **Python bindings** and **`AsyncDatabase`** (`features = ["async"]`): an `RwLock` enforces **concurrent reads** and **exclusive writes** on one shared handle; open transactions serialize all operations on that handle.
- **On-disk file**: still **single-writer** per path (advisory lock); use `open_read_only` in other processes for additional readers.

See [Async policy](async_policy.md) for Python asyncio and the experimental Rust async facade.

