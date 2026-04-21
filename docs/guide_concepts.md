# Typra User Guide: Concepts

This guide explains Typra’s core concepts at a user level. For deeper design details, see the specs under `docs/` and the project [`ROADMAP.md`](/Users/odosmatthews/Documents/coding/typra/ROADMAP.md).

## Database

A **database** is a single embedded unit you open in your application.

- **On-disk**: a `.typra` file (single file, zero-admin deployment)
- **In-memory** (planned): fast ephemeral mode with explicit snapshot save/load

In **0.4.x**, `Database::open(path)` creates or opens the file, validates the header, replays the **persisted schema catalog** from **Schema** segments, and exposes **`register_collection`** / **`register_schema_version`** for new catalog writes. Record data is not stored yet.

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

Models can also be used as **subset models / projections** (planned): a model that declares only some fields of a larger collection schema for more convenient reads.

## Validation (write-time correctness)

Typra’s target behavior is **validation on write**:

- type validation (including nested objects and lists)
- constraint validation (min/max/length/regex, etc.)
- uniqueness/index-backed constraints

If a record is invalid, the write should fail with a structured error that includes the **field path** (including nested paths) and what went wrong.

## Queries (typed, non-SQL in v1)

The design aims for typed query building rather than SQL parsing in v1:

- `get(pk)`
- equality filters on fields and nested paths
- `order_by`, `limit`, and later richer predicates

For query planning/execution design, see [`docs/05_query_planner_and_execution_spec.md`](/Users/odosmatthews/Documents/coding/typra/docs/05_query_planner_and_execution_spec.md).

## File format (single-file, versioned)

Typra is a **single-file** database format. The on-disk format is designed to be:

- crash-safe
- versioned and forward-evolvable
- append-friendly

The design spec includes a header, **dual superblocks**, checksummed **append-only segments**, indexes, and checkpoints. See [`docs/02_on_disk_file_format.md`](/Users/odosmatthews/Documents/coding/typra/docs/02_on_disk_file_format.md).

As of `0.3.0`, Typra publishes a minimal **MANIFEST** pointer by alternating superblocks, so open can follow the newest manifest generation (with a safe scan fallback if the pointer is invalid). As of **`0.4.0`**, **schema catalog** entries are persisted in **Schema** segments and replayed on open.

## Storage modes (disk, memory, hybrid/streaming)

Typra’s roadmap includes:

- **On-disk** as the default embedded mode
- **In-memory with explicit snapshots** for speed and simplified testing/dev
- **Hybrid buffered + streaming** so large datasets can be processed without needing to fit entirely in RAM

See [`docs/guide_storage_modes.md`](/Users/odosmatthews/Documents/coding/typra/docs/guide_storage_modes.md).

