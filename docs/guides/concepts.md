# Concepts

This guide explains Typra’s core concepts at a user level. For deeper design details, see the specs and the project [`ROADMAP.md`](https://github.com/eddiethedean/typra/blob/main/ROADMAP.md). For Rust crate and `typra-core` module layout (including `db/` and storage), see [Rust crate/module layout](../specs/rust_crate_layout.md).

## Database

A **database** is a single embedded unit you open in your application.

- **On-disk**: a `.typra` file (single file, zero-admin deployment)
- **In-memory**: same logical API backed by RAM; use **`open_in_memory`** (Rust/Python) with **explicit snapshot** export/import to persist

On open, `Database::open(path)` (or **`open_in_memory`**) creates or opens storage, validates the header, replays the **persisted schema catalog** from **Schema** segments, rebuilds the **latest row map** from **Record** segments (**v1** and **v2** payloads), replays **secondary index** state from **`SegmentType::Index`** segments (**0.7.0+**), and exposes **`register_collection`** / **`register_schema_version`** (with compatibility checks in **0.9.0**), **`insert`**, **`get`**, **`delete`** (**0.9.0**), and **transactions** (**0.8.0+**) for collections that declare a **primary field**. Inserts run **type + constraint validation** (and **index uniqueness** checks where applicable) before a durable write (**0.6.0+** constraints, **0.7.0+** indexes). Python also ships a small **DB-API 2.0** adapter with a minimal read-only `SELECT` subset.

## Collections

A **collection** is the primary persistent container of records (similar to a “table”, but model-first and schema-driven).

- Each collection has a **name** and a **schema**
- Each record in a collection must conform to that schema

## Schema

A **schema** is the contract that defines:

- field names and types
- nested object shapes and typed lists
- constraints (primary keys, uniqueness, indexes)
- validation rules (min/max/regex/email, etc.)

Typra’s guiding philosophy is **schema-first**: schemas exist to make invalid states unrepresentable, and to guarantee write-time correctness.

## Models (Rust/Python)

Typra uses **models** (Rust structs / Python classes) as the ergonomic way for developers to define and interact with schemas.

- In **Rust**, models are intended to be derived via `#[derive(DbModel)]`.
- In **Python**, models are expected to be defined with a typed model system (likely Pydantic-compatible), then registered with the database.

Models can also be used as **subset models / projections**: Typra ships **field-path projections** on query results (Python **`all(fields=[...])`**, Rust iterator APIs). **Typed** subset handles (e.g. a smaller **`DbModel`** or Pydantic class mapped to a subset of fields) remain planned—see [Models & collections](models_and_collections.md).

## Validation (write-time correctness)

The engine enforces **validation on write** for:

- **Types** (including nested objects, lists, enums, and optionals)
- **Constraints** declared on fields (min/max numerics, string length, regex, email/url heuristics, nonempty, etc.—see [`CHANGELOG.md`](https://github.com/eddiethedean/typra/blob/main/CHANGELOG.md) and [Schema DSL spec](../specs/schema_dsl.md))

Secondary indexes (non-unique and **unique**) are declared on collections, maintained on **insert**, persisted in the log, and used for **equality** lookups in the minimal query planner. Compound indexes and richer constraint/index integration beyond uniqueness remain planned.

Invalid writes fail with structured **`ValidationError`** information in Rust (**`DbError::Validation`**) and map to **`ValueError`** in Python, including nested field paths and expected-vs-actual where applicable.

## Queries (typed, non-SQL in v1)

Typra aims for typed query building rather than SQL parsing in v1.

Shipped: primary-key **`get`**, equality filters, `AND`/`OR`, ranges, **`limit`**, **`order_by`**, heuristic **`explain`**, pull-based **`Database::query_iter`** (Rust), and the Python `db.collection(...).where(...).and_where(...).limit(...)` builder (see [Python guide](python.md)).

Still planned: joins, aggregations, and richer SQL/SQLAlchemy surfaces beyond the minimal DB-API adapter (see [Query planner/execution spec](../specs/query_planner.md)).

## File format (single-file, versioned)

Typra is a **single-file** database format designed to be crash-safe, versioned, forward-evolvable, and append-friendly.

Design reference: [On-disk file format spec](../specs/on_disk_file_format.md).

## Storage modes (disk, memory, hybrid/streaming)

Typra’s roadmap includes:

- **On-disk** as the default embedded mode
- **In-memory with explicit snapshots** for speed and simplified testing/dev
- **Hybrid buffered + streaming** so large datasets can be processed without needing to fit entirely in RAM

See [Storage modes](storage_modes.md).

