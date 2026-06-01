# Why ModelVault?

**Audience:** evaluators, tech leads, and developers choosing local persistence.

ModelVault answers a focused question:

> **Where do I put my application models when I do not want to run a database server?**

Not a data warehouse. Not a multi-tenant cloud service. **Application data**—the objects your code already models with Pydantic, dataclasses, or Rust structs: users, settings, catalog items, project metadata, CLI state.

## The gap in common choices

Teams that need **embedded, local persistence** usually reach for one of these:

| Approach | What goes wrong for *application models* |
|----------|------------------------------------------|
| **SQLite + ORM** | You maintain SQL schema, migrations, and mapping layers that duplicate your app types |
| **JSON / YAML on disk** | Fast to start; indexes, queries, validation, and safe evolution become your problem |
| **TinyDB** | Pleasant document API; weak guarantees for strict schemas and production evolution |
| **DuckDB** | Excellent for analytics and SQL over files; not the default choice for everyday app CRUD |

ModelVault exists to be **purpose-built** for that middle ground: **typed documents with indexes**, **validation at the storage boundary**, and **single-file deployment**—without inventing a new query language for every app.

## How ModelVault is different

This is the same framing as the project README—use it when explaining ModelVault to stakeholders:

| Alternative | Limitation | ModelVault |
|-------------|------------|------------|
| **Relational DBs** | App models mapped into tables/rows; schema friction | Model-first schemas; validation on write; nested objects native |
| **JSON files** | No indexes, weak queries, manual integrity | Typed storage, indexes, queries, durability |
| **TinyDB** | Document store without production-grade validation or evolution | Strict schemas, migrations, indexes, crash-safe file format |
| **DuckDB** | Built for analytics (OLAP), not app CRUD | Embedded OLTP for application models (complementary, not competing) |

[:octicons-arrow-right-24: Feature-level comparison matrix](../comparisons/index.md)

## Design principles

1. **Schema-first** — Types and constraints are declared up front; invalid writes fail at the boundary, not in a distant report.
2. **Model-native** — Collections align with classes and structs, not hand-maintained SQL that drifts from your types.
3. **Single-file deployment** — One `.modelvault` file (or in-memory for tests); no separate server process.
4. **Safe evolution** — Versioned catalog, compatibility classification, and migration helpers (`plan` / `apply` in Python).
5. **One engine, two languages** — Rust core; Python bindings share semantics for open, validation, and replay.

## Outcomes you can expect

| Outcome | Mechanism |
|---------|-----------|
| Store application models directly | `modelvault.models.collection(db, YourModel)` or Rust `FieldDef` / `DbModel` |
| Catch bad data early | Engine validation and unique indexes on write |
| Ship without ops overhead | Embedded file or memory; optional CLI for inspect/backup/verify |
| Evolve schemas over releases | `register_schema_version`, compatibility checks, migration steps |
| Stay close to your types | Dataclasses, Pydantic v2, Rust structs—no parallel schema language required for the happy path |

## Who should use it

| Persona | Typical fit |
|---------|-------------|
| **FastAPI developer** | Prototypes and small services that should not require PostgreSQL on day one |
| **Desktop developer** | Offline-capable apps with a file users can back up or sync |
| **CLI author** | Structured, durable state beyond `config.json` |
| **Local-first builder** | Offline storage with indexes and schema evolution, no sync server in the box |

## Honest tradeoffs

ModelVault is a strong fit when you want **typed OLTP documents in one file**. Be explicit about limits:

| Topic | Reality today |
|-------|----------------|
| **SQL** | Model-first APIs are primary; read-only SQL exists for DB-API—not a full relational surface |
| **Analytics** | Use DuckDB or export snapshots for heavy OLAP; ModelVault targets app CRUD |
| **Distribution** | Single process, embedded; no built-in replication or network server |
| **Concurrency** | Advisory locking; plan for one writer or coordinate at the app layer (like SQLite) |

## When to choose something else

| Your need | Better starting point |
|-----------|------------------------|
| Ad-hoc SQL across many relational tables | PostgreSQL, SQLite |
| Columnar analytics on huge datasets | DuckDB |
| Multi-user network database with ACLs | PostgreSQL / MySQL |
| Ephemeral cache with TTL | Redis |
| Browser-only storage without native code | IndexedDB (ModelVault is native-embedded today) |

## Non-goals (product boundary)

ModelVault does **not** aim to:

- Replace general-purpose SQL databases for arbitrary relational modeling
- Ship SQLAlchemy as a supported write path in the current release
- Compete with DuckDB on analytics workloads

Those boundaries keep the product focused. Broader SQL work is tracked on the [roadmap](https://github.com/eddiethedean/modelvault/blob/main/ROADMAP.md).

## Next steps

- [Quickstart](quickstart.md) — install and first insert
- [Core concepts](concepts.md) — database, collection, schema, validation, queries
- [Comparisons](../comparisons/index.md) — deep dives vs SQLite, JSON, TinyDB, DuckDB
- [Compatibility](../reference/compatibility.md) — file format and API stability pledges
