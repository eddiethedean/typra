# Typra Roadmap

This document is the **project roadmap** for Typra: a typed, embedded, single-file database with Rust-first core and ergonomic Python bindings.

- **Current release**: `0.1.0` (see [`CHANGELOG.md`](/Users/odosmatthews/Documents/coding/typra/CHANGELOG.md))
- **Roadmap style**: release-based milestones (SemVer). Minor versions (`0.x`) may still contain breaking changes.

## Guiding principles (from the specs)

Typra is designed around:

- **Schema-first** collections of validated records (not SQL tables)
- **Validation on write** (type + constraints + indexes)
- **Nested objects and typed lists** as first-class
- **Single-file, zero-config deployment**
- **Safe schema evolution** over time

In addition, Typra should support **multiple storage/compute modes** (SQLite-like ergonomics):

- **In-memory mode**: fast operations without implicit durability; supports **explicit snapshot export/import** (save/load).
- **On-disk mode**: durable single-file operation (the default embedded deployment story).
- **Future hybrid + streaming mode**: keep a normal file-backed database, but use a **buffer pool** plus **streaming/bounded-memory operators** so queries (notably joins and groupby/aggregations) can run when data exceeds RAM.

Quick links:
- **Mode semantics & architecture**: see [In-memory, hybrid, and streaming execution (refined plan)](#in-memory-hybrid-and-streaming-execution-refined-plan)
- **Release milestones**: see [Roadmap by release](#roadmap-by-release)

Primary design references:
- [`docs/01_full_architecture_spec.md`](/Users/odosmatthews/Documents/coding/typra/docs/01_full_architecture_spec.md)
- [`docs/02_on_disk_file_format.md`](/Users/odosmatthews/Documents/coding/typra/docs/02_on_disk_file_format.md)
- [`docs/04_schema_dsl_spec.md`](/Users/odosmatthews/Documents/coding/typra/docs/04_schema_dsl_spec.md)
- [`docs/05_query_planner_and_execution_spec.md`](/Users/odosmatthews/Documents/coding/typra/docs/05_query_planner_and_execution_spec.md)
- [`docs/typed_embedded_db_spec.md`](/Users/odosmatthews/Documents/coding/typra/docs/typed_embedded_db_spec.md)

## Status snapshot (0.1.x)

Delivered in `0.1.0`:
- **Rust**: `Database::open(path)` creates/opens a file; `DbError`; `DbModel` marker trait; facade crate `typra`; derive `#[derive(DbModel)]`.
- **Python**: native extension module `typra` with `__version__` (API is intentionally minimal in `0.1.0`).

Non-goals already in place for `0.1.x`:
- No persisted schema catalog, record storage, indexes, validation engine, query engine, or transactions yet.

## Roadmap by release

Each milestone lists:
- **Rust (core + public API)**: what lands in `typra-core` / `typra` / `typra-derive`
- **Python**: what lands in `python/typra` (bindings and (later) pure-Python helpers)
- **Definition of done**: tests, docs, and behavioral guarantees

### 0.2.0 — On-disk foundation (header + minimal manifest) and schema metadata types

**Goal**: move from “file exists” to “file has a recognized format,” plus internal types to represent schemas.

- **Rust**
  - Define the **file header** (magic/version/feature flags) and validate it on open.
  - Introduce core schema metadata structures (collection IDs, field paths/types, version IDs).
  - Add an internal “catalog record” model (even if not yet fully persisted).
  - Expand error taxonomy beyond `Io` / `NotImplemented` as needed for format/schema issues.
  - Introduce an internal storage abstraction boundary (e.g. a “backing store” interface) so future **in-memory vs on-disk** can share the same logical engine code.
- **Python**
  - Keep surface area small; ensure packaging/release remains stable.
  - Add docstring/module docs clarifying current maturity and planned APIs.
- **Definition of done**
  - New/old file open behaviors are tested (new file creation writes header; existing file validates header).
  - Format versioning strategy documented (what changes imply major/minor bump).
  - Crash-safety story for the header/superblock approach is explicitly stated (even if not fully implemented yet).

Design anchor: [`docs/02_on_disk_file_format.md`](/Users/odosmatthews/Documents/coding/typra/docs/02_on_disk_file_format.md)

### 0.3.0 — Append-only segment writer/reader + minimal recovery checks

**Goal**: have a real “append-only segment” primitive for writing structured events to the database file.

- **Rust**
  - Implement **segment headers** and segment types (at minimum: schema + record + manifest/checkpoint).
  - Implement a minimal **segment writer/reader** with checksums.
  - Add a “scan segments” debug utility (internal API) to support testing and future tooling.
  - Start a minimal “manifest/checkpoint pointer” mechanism (even if simplistic at first).
  - Define the first **snapshot export/import** interfaces (API only if needed; implementation can land once record encoding exists).
- **Python**
  - No major new public API required; optionally expose a `typra.debug_*` module for introspection (if desired).
- **Definition of done**
  - Roundtrip tests for writing/reading segments.
  - Corruption detection tests (bad checksum/bad magic yields deterministic error).
  - Backwards compatibility behavior documented for `0.2.x` files.

Design anchor: segment model + checksums in [`docs/02_on_disk_file_format.md`](/Users/odosmatthews/Documents/coding/typra/docs/02_on_disk_file_format.md)

### 0.4.0 — Persisted schema catalog + collection registration

**Goal**: persist schema definitions in the file and support registering collections.

- **Rust**
  - Implement a persisted **schema catalog** segment type and catalog entry encoding.
  - Add `Database::register_collection` or equivalent API shape.
  - Maintain schema versions per collection; support “create collection” + “new schema version” entries.
  - Introduce stable IDs for collections and schema versions.
  - Add an **in-memory catalog** implementation that matches the persisted catalog semantics (enables memory-only databases to behave the same as disk-backed ones).
- **Python**
  - Add a thin `Database` object that can **open** and **register** a model/schema (even if writes aren’t ready yet).
  - Decide the first Python model story: Pydantic-based inference vs explicit schema objects vs a lightweight `Model` base.
- **Definition of done**
  - Registering a collection persists the catalog entry and survives reopen.
  - Duplicate name handling and versioning behavior specified and tested.

Design anchor: catalog requirements in [`docs/01_full_architecture_spec.md`](/Users/odosmatthews/Documents/coding/typra/docs/01_full_architecture_spec.md)

### 0.5.0 — Record encoding v1 + insert/get by primary key

**Goal**: store records and retrieve them; establish the first durable record encoding.

- **Rust**
  - Implement record event encoding/decoding (insert/replace/delete; starting with insert + get).
  - Add `Collection<T>` typed handle and `insert` + `get(pk)` APIs (exact shape may evolve).
  - Implement primary-key indexing mechanism sufficient for `get(pk)` (may be an embedded index or minimal index segment).
  - Establish record visibility rules for “latest version” (even before full MVCC).
  - Implement **in-memory database** mode with the same logical APIs:
    - fast insert/get
    - no durability guarantees
    - **explicit snapshot export/import** to/from the on-disk format (initially as a whole-db snapshot)
- **Python**
  - Expose `db.collection("User")` / `db.users` + `insert` + `get` for the first supported model type.
  - Return validated model instances (or dicts) in a predictable way; document trade-offs.
  - Add Python surface for in-memory usage (e.g. `Database.in_memory()` or `Database(":memory:")`) plus snapshot save/load entrypoints.
- **Definition of done**
  - Records written survive process restart and reopen (durability validated via tests).
  - Schema mismatch errors are crisp (wrong schema version cannot decode silently).
  - Encoding documented (at least at the conceptual level and stability guarantees).
  - In-memory insert/get and snapshot export/import are covered by integration tests (Rust and Python).

Design anchor: record log + encoding strategy in [`docs/02_on_disk_file_format.md`](/Users/odosmatthews/Documents/coding/typra/docs/02_on_disk_file_format.md)

### 0.6.0 — Validation engine (types + constraints) and better errors

**Goal**: enforce schema contracts at write time with high-quality error reporting.

- **Rust**
  - Implement type validation for primitives/composites (optional/list/object/enum per spec).
  - Add constraint validators (min/max/length/regex/email/url, etc.) with structured errors.
  - Decide strictness/coercion policy for v1 (prefer strict by default).
  - Ensure validation runs before any durable commit step.
- **Python**
  - Integrate Python model validation story cleanly with engine validation:
    - Either rely on Pydantic (Python-side) + engine validation (authoritative), or
    - Use engine validation as the source of truth and keep Python lightweight.
  - Provide actionable exception types/messages for invalid writes.
- **Definition of done**
  - Deterministic validation semantics across Rust/Python (same rule, same outcome).
  - Error messages include field paths (including nested paths) and expected/actual types.

Design anchor: validation semantics in [`docs/typed_embedded_db_spec.md`](/Users/odosmatthews/Documents/coding/typra/docs/typed_embedded_db_spec.md)

### 0.7.0 — Secondary indexes (unique + non-unique) and simple filters

**Goal**: make real queries practical: equality filters on indexed fields and nested paths.

- **Rust**
  - Implement secondary index definitions in schema catalog (`@unique`, `@index`).
  - Add index maintenance on insert (and later update/delete).
  - Implement a minimal query AST and execution for:
    - `get(pk)`
    - equality filter on scalar field
    - equality filter on nested scalar path
    - `limit`
  - Add an “explain plan” output (even if heuristic).
  - Start the **streaming execution** shape for scans and filters (iterator-based or pull-based operators) so later joins/aggregations can spill.
  - Introduce **subset models / projections** (UX feature): allow users to define a model that is a *subset* of an existing collection schema, and have query results materialize into that subset type.
- **Python**
  - Introduce a first query builder API (non-SQL) aligned to the spec (`where(...)`, `limit(...)`, `all()`).
  - Ensure nested-path querying feels natural in Python.
  - Add a Python-facing story for subset models (e.g. defining a model with fewer fields than the registered collection) so large/nested collections are less cumbersome to work with.
- **Definition of done**
  - Index correctness tests (unique constraint enforcement, index lookup matches scan).
  - Performance sanity checks/benchmarks for `get` and indexed equality.
  - Subset projection tests: querying the same records into a “full” model vs a subset model yields consistent values for shared fields, and subset materialization does not require decoding unused fields.

Design anchor: query planner + AST in [`docs/05_query_planner_and_execution_spec.md`](/Users/odosmatthews/Documents/coding/typra/docs/05_query_planner_and_execution_spec.md)

### 0.8.0 — Transactions v1 (single-writer) + crash safety checkpoints

**Goal**: add atomicity/durability semantics beyond “best effort append.”

- **Rust**
  - Introduce transaction boundaries and commit markers in the log.
  - Implement a single-writer lock and multi-reader semantics (within a process).
  - Implement checkpointing / manifest updates using a crash-safe approach (A/B superblock strategy).
  - Recovery on open: choose last valid checkpoint; replay or validate trailing segments.
  - Introduce a real **buffer pool / pager** for the file-backed database so data can be pulled into RAM on demand and written back when dirty (hybrid buffered execution groundwork).
- **Python**
  - Expose `with db.transaction(): ...` (or similar) for batching writes.
  - Ensure exceptions correctly abort/rollback the in-progress transaction.
- **Definition of done**
  - Crash-simulation tests (kill mid-write / partial segment) with recovery correctness.
  - Document transaction semantics and concurrency expectations for v1.

Design anchor: superblocks + commit markers in [`docs/02_on_disk_file_format.md`](/Users/odosmatthews/Documents/coding/typra/docs/02_on_disk_file_format.md)

### 0.9.0 — Schema evolution & migrations (safe changes), plus compaction prototype

**Goal**: make it maintainable for real apps: evolve schemas safely and keep files healthy.

- **Rust**
  - Add schema compatibility rules (safe vs breaking changes) and enforce them on schema update.
  - Provide a migration mechanism:
    - record schema version history
    - support “read old, write new” strategies where feasible
  - Add compaction prototype (rewrite segments, rebuild indexes) with a basic policy.
  - Implement first **bounded-memory operators** needed for large-than-RAM workloads:
    - external sort (if required for groupby/order-by paths)
    - hash aggregation with spill (where feasible)
    - join strategy that can spill (e.g. grace hash join) for large inputs
- **Python**
  - Provide migration ergonomics and clear user guidance (“what changed, what breaks, what’s safe”).
  - Add admin-style utilities (compact/vacuum, inspect, stats).
- **Definition of done**
  - Migration tests on representative schemas (add optional field, add enum value, add index).
  - Compaction correctness tests (no data loss; indexes rebuilt).
  - Large-than-RAM query tests in CI using constrained memory settings (best-effort, platform dependent).

Design anchor: evolution rules in [`docs/01_full_architecture_spec.md`](/Users/odosmatthews/Documents/coding/typra/docs/01_full_architecture_spec.md)

### 1.0.0 — Stable public API + format guarantees

**Goal**: commit to stability: “you can ship this in applications” with documented guarantees.

- **Rust**
  - Stabilize the public API surface (`typra` facade + `typra-core`) and feature flags.
  - Guarantee file format compatibility policy (what is forward/back compatible).
  - Establish a clear “supported types” matrix and behavior for nullability/optionality.
  - Hardening: fuzzing targets for decoding, property tests for index invariants, benchmark suite.
  - Clearly documented **mode semantics**:
    - in-memory (ephemeral) vs in-memory-with-snapshot (explicit save/load)
    - on-disk (durable)
    - hybrid/streaming (what is guaranteed to work beyond RAM, and what may still require memory)
- **Python**
  - Stabilize the Python API surface and type hints/stubs.
  - Guarantee compatibility policy for the Python package vs the underlying file format.
  - Provide “good defaults” for local app usage patterns.
  - Document streaming/hybrid behavior and trade-offs (performance, temporary disk usage, determinism).
- **Definition of done**
  - End-to-end story works: register schema → insert → get/query → reopen → migrate → compact.
  - Documentation: “Getting Started”, “Schema”, “Queries”, “Migrations”, “Operational tooling”, “Failure modes”.

## Cross-cutting initiatives (land throughout 0.2–1.0)

- **Testing**
  - File-format roundtrips; corruption detection; crash recovery simulations.
  - Invariant testing: uniqueness indexes, record visibility, schema compatibility.
- **Tooling**
  - “Inspect”/debug dump of file structures (header, superblocks, segments).
  - Benchmarks and profiling harness for `get(pk)` and indexed equality queries.
- **Docs**
  - Keep design specs aligned with actual implementation as versions ship.
  - Provide explicit “what’s implemented” sections (to avoid spec drift confusion).
- **DX**
  - Make errors structured and actionable (field paths, expected/actual, hints).
  - Keep Rust and Python behavior consistent wherever possible.

## Non-goals (for 1.0 unless explicitly revisited)

From the architecture spec’s v1 non-goals:
- Distributed operation or replication
- Full SQL compatibility / network server mode
- Full-text search, vector search
- DuckDB-style analytics focus
- Cross-process high-write concurrency

## Open questions (to resolve before 1.0)

- **Record encoding**: confirm the v1 encoding and its long-term evolution strategy.
- **Optionality semantics**: required vs nullable vs defaulted (keep v1 simple as per spec).
- **Python model story**: Pydantic-first vs lightweight models vs engine-first validation.
- **Index physical layout**: embedded in record log vs separate index segments and rebuild strategies.

## In-memory, hybrid, and streaming execution (refined plan)

This section refines the roadmap to ensure Typra can operate **in memory and on disk** with SQLite-like ergonomics, and later support a **hybrid + streaming** approach for workloads that exceed RAM.

### Target mode semantics

- **In-memory database (fast path)**:
  - Primary goal: speed and low latency.
  - Persistence: **explicit snapshot export/import** (save/load), not implicit durability.
- **On-disk database (default embedded mode)**:
  - Durable single-file operation.
- **Hybrid buffered execution (future)**:
  - The database remains a normal **file-backed** Typra database.
  - Data is **pulled into RAM as needed** (buffer pool / pager) and **written back** when dirty/required.
  - For large joins and groupby/aggregations, execution should use **bounded-memory operators** that can spill/work in chunks.

### Proposed internal architecture

- **Storage boundary** (in `typra-core`):
  - Separate logical engine code (schema/validation/query planning) from physical persistence and caching.
  - Provide at least two store implementations:
    - `FileStore`: segment/page IO over a `.typra` file.
    - `MemStore`: an in-memory representation with the same semantics.
- **BufferPool/Pager** (for `FileStore`):
  - Cache unit: segments or pages (decision to lock down early).
  - Eviction policy: LRU/clock with dirty tracking.
  - Configurable memory limit; deterministic flush behavior.
- **Streaming operator model** (execution engine):
  - Use a pull-based pipeline (iterator-like operators) so scans/filters/limits can stream.
  - Later add bounded-memory implementations for:
    - groupby/aggregations (hash agg with spill; external sort where needed)
    - joins (spillable hash join / grace hash join strategies)

### Rust-first public API direction

- Keep `Database::open(path)` for disk-backed databases.
- Add an explicit in-memory constructor (name TBD, but prefer clarity over magic strings):
  - `Database::open_in_memory()` / `Database::in_memory()`
- Add snapshot APIs (names TBD):
  - `db.export_snapshot_to_path(path)`
  - `Database::import_snapshot_from_path(path)`

Python should mirror these later as thin wrappers (once Rust stabilizes).

### Acceptance tests (what “done” means for these features)

- **Mode parity**:
  - same schema + inserts => same reads/results in `MemStore` vs `FileStore`.
- **Snapshot roundtrip**:
  - in-memory → export snapshot → reopen on-disk → identical reads
  - on-disk → import snapshot → in-memory reads match
- **Buffered correctness** (hybrid mode):
  - dirty data flushes correctly
  - checksum/corruption detection is deterministic
  - crash/reopen recovery maintains invariants (as durability features land)
- **Large-than-RAM correctness** (later):
  - joins and groupby/aggregations complete with bounded memory (with spill/chunking as required)

### Early decisions to lock down (avoid rework)

- **Cache unit**: segment-sized vs page-sized IO (affects buffer pool + on-disk layout).
- **Spill location**: default to **internal temporary segments** within the same `.typra` file (so “hybrid” still looks like a normal file-backed database to users). Consider a sidecar only if there is a compelling operational reason.
- **Snapshot format guarantee**: snapshots should be valid `.typra` files whenever possible (so the snapshot is not a special-case format).

## Subset models / projections (UI ergonomics)

For developer ergonomics—especially for collections with large, deeply nested schemas—Typra should support **subset models** (also known as projections or views).

### What it means

- A collection may have a “full” schema (e.g. 20 fields with nested objects).
- A user can define a **subset model** that declares only a subset of those fields, and/or only some nested paths.
- Queries (and `get`) can materialize results into that subset model so the interface behaves as if the collection “only had” those declared fields.

### Intended semantics

- **Subset types are read projections**:
  - They must be **compatible** with the registered collection schema.
  - They do not redefine storage; they redefine **materialization**.
- **Compatibility rules** (v1):
  - Subset fields must exist in the collection schema (including nested paths).
  - Field types must be equal or safely coercible under the engine’s strictness policy (prefer equality in early versions).
  - Missing fields in the subset model are simply not materialized.
- **Performance expectation**:
  - When possible, the engine should avoid decoding/deserializing unused fields (projection-aware decoding).

### API direction (Rust-first)

- Provide a way to request a typed handle for a subset model over a collection, conceptually:
  - `db.collection::<FullUser>()` vs `db.collection::<UserSummary>()`
  - or explicit `project::<UserSummary>()` on a collection handle
- Query planning should carry a **projection** so the execution engine knows which paths to materialize.

### Python ergonomics

- Support defining a model class with fewer fields than the underlying collection.
- Results returned from queries against that model type should match the subset shape (including partial nested objects).

### Acceptance tests

- Materializing a subset model returns exactly the declared fields (and only those fields).
- Shared fields between full and subset models match exactly for the same record.
- Nested partial projection works (e.g. `profile.timezone` without decoding all of `profile` when the encoding allows it).
- Invalid subset definitions fail early with clear errors (unknown field path, type mismatch).

