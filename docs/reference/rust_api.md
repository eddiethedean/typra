# Rust API (curated)

This page is a curated reference for Typra’s Rust surface, optimized for “what do I import?” and “what’s stable?”.

For the full, authoritative API docs, use rustdoc:

- `typra` (facade): `https://docs.rs/typra`
- `typra-core` (engine): `https://docs.rs/typra-core`

## Recommended crate

Most applications should depend on **`typra`**:

```toml
[dependencies]
typra = "1.0"
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

Typra’s primary Rust query surface is typed (non-SQL):

- Predicate composition (`Eq`, `And`, `Or`, ranges)
- `limit`, `order_by`, `explain`
- Streaming iteration (`query_iter`) for bounded-memory execution shapes

For detailed semantics, see [Query planner and execution spec](../specs/query_planner.md) and [Types matrix](types.md).

