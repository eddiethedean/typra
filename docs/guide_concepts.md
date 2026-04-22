# Typra User Guide: Concepts

This guide explains Typra’s core concepts at a user level. For deeper design details, see the specs under `docs/` and the project [`ROADMAP.md`](../ROADMAP.md).

## Database

A **database** is a single embedded unit you open in your application.

- **On-disk**: a `.typra` file (single file, zero-admin deployment)
- **In-memory** (0.5.x): same logical API backed by RAM; use **`open_in_memory`** (Rust/Python) with **explicit snapshot** export/import to persist

In **0.5.x**, `Database::open(path)` (or **`open_in_memory`**) creates or opens storage, validates the header, replays the **persisted schema catalog** from **Schema** segments, rebuilds the **latest row map** from **Record** segments, and exposes **`register_collection`** / **`register_schema_version`**, **`insert`**, and **`get`** (primary-key lookup) for collections that declare a **primary field**.

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

For query planning/execution design, see [`05_query_planner_and_execution_spec.md`](05_query_planner_and_execution_spec.md).

## File format (single-file, versioned)

Typra is a **single-file** database format. The on-disk format is designed to be:

- crash-safe
- versioned and forward-evolvable
- append-friendly

The design spec includes a header, **dual superblocks**, checksummed **append-only segments**, indexes, and checkpoints. See [`02_on_disk_file_format.md`](02_on_disk_file_format.md).

As of `0.3.0`, Typra publishes a minimal **MANIFEST** pointer by alternating superblocks, so open can follow the newest manifest generation (with a safe scan fallback if the pointer is invalid). As of **`0.4.0`**, **schema catalog** entries are persisted in **Schema** segments and replayed on open. As of **`0.5.0`**, **record** payloads use **`SegmentType::Record`** (see [`06_record_encoding_v1.md`](06_record_encoding_v1.md)); new databases use format minor **5**, with lazy upgrades from older minors.

## Storage modes (disk, memory, hybrid/streaming)

Typra’s roadmap includes:

- **On-disk** as the default embedded mode
- **In-memory with explicit snapshots** for speed and simplified testing/dev
- **Hybrid buffered + streaming** so large datasets can be processed without needing to fit entirely in RAM

See [`guide_storage_modes.md`](guide_storage_modes.md).

