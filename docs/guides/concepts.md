# Core concepts

For *why* Typra exists and how it compares to SQLite or JSON files, start with [Why Typra](why_typra.md) and [Comparisons](../comparisons/index.md).

Typra’s model in six ideas. For implementation detail, see [Specifications](../specs/index.md). For release status, see the [roadmap](https://github.com/eddiethedean/typra/blob/main/ROADMAP.md).

## 1. Database — one embedded unit

A **database** is what you open in your process:

| Mode | API | Persistence |
|------|-----|-------------|
| **On-disk** | `Database.open("app.typra")` | Durable single file |
| **In-memory** | `Database.open_in_memory()` | Explicit snapshots only |

On open, Typra validates the file header, replays the **schema catalog**, rebuilds the latest row map from **record** segments, and restores **index** state.

## 2. Collection — typed container

A **collection** holds records that share one schema. Think “table,” but the contract is a **schema**, not free-form rows.

- Each collection has a **name** and a **schema version**
- Every record must validate against the active schema

## 3. Schema — the contract

A schema defines:

- **Field paths** and **types** (including nested paths like `profile.timezone`)
- **Constraints** (min/max, length, regex, email, …)
- **Primary key** and **indexes**

Typra is **schema-first**: invalid states should be rejected **on write**, not discovered later in production.

## 4. Models — how you author schemas

| Language | Typical approach |
|----------|------------------|
| **Python** | `@dataclass` or Pydantic + **`typra.models`** (recommended) |
| **Rust** | `FieldDef` registration or **`#[derive(DbModel)]`** (top-level fields in 1.0) |

**Subset models** let you read only part of a large schema—see [Models & collections](models_and_collections.md).

## 5. Validation — fail fast on write

Before a row is appended:

1. **Types** are checked (primitives, optionals, lists, objects, enums)
2. **Constraints** run on declared fields
3. **Unique indexes** are enforced

Errors are structured: nested **paths**, expected vs actual where applicable. Rust: `DbError::Validation`. Python: `TypraValidationError` / `ValueError`.

## 6. Queries — typed, not SQL-first

Shipped today:

- Primary-key **`get`**
- **Equality**, **`AND`/`OR`**, **ranges**
- **`limit`**, **`order_by`**, **`explain`**
- Python: `db.collection("name").where(...).all()`
- Rust: typed query AST + **`query_iter`**

A minimal read-only **SQL** subset exists for DB-API interop. Broader SQL is on the [roadmap](https://github.com/eddiethedean/typra/blob/main/ROADMAP.md#post-10-isoiec-9075-sql-track).

## File format (single file, versioned)

Everything lives in one **`.typra`** file: header, superblocks, append-only **segments**, checkpoints. Design reference: [On-disk format](../specs/on_disk_file_format.md).

## Storage modes (summary)

| Mode | When to use |
|------|-------------|
| **On-disk** | Default — durable embedded apps |
| **In-memory** | Tests, prototypes, explicit save/load |
| **Hybrid / streaming** | Roadmap — large-than-RAM queries with spill |

Details: [Storage modes](storage_modes.md).

## Where to go next

- [Quickstart](quickstart.md) — install and first insert
- [Python guide](python.md) — full Python surface
- [Types matrix](../reference/types.md) — supported types and queries
