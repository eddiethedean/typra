# Why ModelVault?

ModelVault exists to answer one question:

> **Where do I put my application models locally?**

Not analytics warehouses. Not multi-tenant servers. **Application data** — users, settings, inventory lines, project metadata — with the same types you already use in Python or Rust.

## The problem

Many apps need local persistence without operating a database server:

- A FastAPI service that should not require PostgreSQL for a prototype
- A desktop app that must work offline
- A CLI that outgrows `config.json`
- A local-first sync client with evolving schemas

Common choices each leave gaps:

| Approach | Gap |
|----------|-----|
| **SQLite + ORM** | SQL migrations and impedance mismatch with Pydantic/dataclasses |
| **JSON / YAML files** | No indexes, weak queries, integrity is DIY |
| **TinyDB** | Lightweight, but not built for strict validation and production evolution |
| **DuckDB** | Excellent for analytics; not the default OLTP story for app CRUD |

ModelVault targets the **application model** niche directly.

## Design philosophy

1. **Schema-first** — declare types and constraints up front; invalid writes fail at the boundary.
2. **Model-native** — collections map to classes/structs, not hand-maintained SQL.
3. **Single-file deployment** — one `.modelvault` file (or in-memory for tests).
4. **Safe evolution** — catalog versions, compatibility checks, migration helpers.
5. **Rust core, ergonomic bindings** — one engine; Python and Rust share semantics.

## Target audience

| Persona | Messaging |
|---------|-----------|
| FastAPI developer | *Store your Pydantic models directly.* |
| Desktop developer | *Ship a database as a single file.* |
| CLI developer | *Keep application data durable and typed.* |
| Local-first developer | *Application-focused storage with no infrastructure.* |

## Benefits (outcomes, not internals)

| Outcome | What you get |
|---------|----------------|
| Store application models directly | `modelvault.models.collection(db, YourModel)` |
| Validation automatically | Engine rejects invalid types and constraint violations |
| No database server | Embedded file or memory — zero ops |
| Single-file deploy | Copy `app.modelvault` with your binary or installer |
| Schema evolution over time | `plan` / `apply` and compatibility classification |
| Familiar types | Dataclasses, Pydantic v2, Rust structs + `DbModel` |

## Tradeoffs

ModelVault is a strong fit when you want **typed documents with indexes** in one file. Be aware of current limits:

- **Not a SQL database** — model-first APIs are primary; SQL is a growing read-only subset today.
- **Not for analytics at scale** — use DuckDB (or export) for heavy OLAP; ModelVault is OLTP-oriented.
- **Not distributed** — single process, embedded; no replication or network server mode in 1.0.
- **Not for cross-process write storms** — like SQLite, assume one writer or app-level coordination.

## When not to use ModelVault

| Need | Better choice |
|------|----------------|
| Ad-hoc SQL reporting across many tables | PostgreSQL, SQLite |
| Columnar analytics on huge datasets | DuckDB |
| Shared multi-user server with network ACLs | PostgreSQL / MySQL |
| Key-value cache with TTL | Redis |
| Browser-only storage without native code | IndexedDB / WASM story (future for ModelVault) |

## Next steps

- [Quickstart](quickstart.md) — first model in five minutes
- [Comparisons](../comparisons/index.md) — ModelVault vs SQLite, JSON, TinyDB, DuckDB
- [Core concepts](concepts.md) — database, collection, schema, record
