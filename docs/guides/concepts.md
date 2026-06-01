# Core concepts

**Audience:** developers who want the mental model before diving into APIs.

For *why* ModelVault exists and how it compares to SQLite or JSON files, read [Why ModelVault](why_modelvault.md) and [Comparisons](../comparisons/index.md). This page explains **how the pieces fit together**. Implementation detail lives under [Specifications](../specs/index.md).

## The big picture

ModelVault is an **embedded database for application models**:

- You open **one database** (a file or memory image) inside your process.
- You define **collections**—typed containers analogous to tables, but governed by a **schema**, not free-form documents.
- Every **write** is validated; durable state is an append-only **log** inside a single **`.modelvault`** file.

There is no separate server to install or operate.

## 1. Database — one embedded unit

A **database** is the handle you open in your application:

| Mode | API | Persistence |
|------|-----|-------------|
| **On-disk** | `Database.open("app.modelvault")` | Durable single file; ship with your app |
| **In-memory** | `Database.open_in_memory()` | Fast tests; use snapshots to export/import |

On open, ModelVault validates the file header, replays the **schema catalog**, reconstructs the latest row map from **record** segments, and restores **index** state. Recovery behavior depends on [`RecoveryMode`](../reference/compatibility.md).

## 2. Collection — typed container

A **collection** holds records that share one schema version. It is the unit you name in APIs (`"books"`, `"users"`).

- Each collection has a **name**, monotonic **schema versions**, and optional **secondary indexes**.
- Records are keyed by a declared **primary key** field.
- Inserts are **replace-by-primary-key** (last write wins for a given key).

Think “table,” but the contract is a **typed schema**, not arbitrary rows.

## 3. Schema — the contract

A schema defines what may be stored:

| Element | Role |
|---------|------|
| **Field paths** | Identifiers such as `title` or nested `profile.timezone` |
| **Types** | Primitives, optionals, lists, objects, enums |
| **Constraints** | Min/max, length, regex, email, and similar checks |
| **Primary key** | Unique identifier per record within the collection |
| **Indexes** | Secondary lookups (unique or non-unique) |

ModelVault is **schema-first**: the engine rejects invalid states **on write**, so you do not discover type errors only after data is on disk.

## 4. Models — how you author schemas

| Language | Recommended approach |
|----------|----------------------|
| **Python** | `@dataclass` or Pydantic v2 + **`modelvault.models.collection`** |
| **Rust** | `register_collection` with `FieldDef`, or **`#[derive(DbModel)]`** where applicable |

Markers such as `__modelvault_primary_key__` and `__modelvault_indexes__` connect your class to storage. **Subset models** let you read or write projections of a larger schema—see [Models & collections](models_and_collections.md).

## 5. Validation — fail fast on write

Before a row is appended, ModelVault checks:

1. **Types** — primitives, optionals, lists, objects, enums, and nested paths
2. **Constraints** — engine rules on declared fields
3. **Unique indexes** — no duplicate keys where uniqueness is required

Failures are structured with **field paths** and clear messages. In Python: `ModelVaultValidationError` / `ValueError`. In Rust: `DbError::Validation`.

## 6. Queries — typed, not SQL-first

Primary interfaces are **typed**, not ad-hoc SQL strings:

| Operation | Surface |
|-----------|---------|
| Point read | `get` by primary key |
| Filters | Equality, ranges, `AND` / `OR` |
| Ordering | `order_by`, `limit` |
| Python | `db.collection("name").where(...).all()` |
| Rust | Query AST + `query_iter` |

A minimal **read-only SQL** subset exists for `modelvault.dbapi` interop. Broader SQL is on the [roadmap](https://github.com/eddiethedean/modelvault/blob/main/ROADMAP.md).

## File format — one versioned file

All durable state lives in a single **`.modelvault`** file: header, superblocks, append-only **segments** (schema, records, indexes, transactions, checkpoints), and recovery metadata. Operators and contributors should read [On-disk format](../specs/on_disk_file_format.md).

## Storage modes (summary)

| Mode | When to use |
|------|-------------|
| **On-disk** | Production embedded apps; default |
| **In-memory** | Unit tests, scratch workflows, explicit snapshot export |
| **Hybrid / streaming** | Roadmap — bounded-memory operators for very large queries |

Details: [Storage modes](storage_modes.md).

## Where to go next

- [Quickstart](quickstart.md) — install and first insert
- [Python guide](python.md) — full Python surface
- [Types matrix](../reference/types.md) — supported types and query shapes
- [Operations runbook](../ops/operations_and_failure_modes.md) — backup, recovery, locking
