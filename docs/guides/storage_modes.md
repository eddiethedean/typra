# Storage modes

Typra is designed to feel SQLite-like in ergonomics while providing stronger typing and validation.

This guide explains storage modes and what they mean for performance and persistence.

## Current status (important)

Today, the **on-disk** path supports open/create, superblocks, checksummed segments, manifest publication, a persisted schema catalog (including secondary index definitions), record insert/get (v1 and v2 payloads), index segment replay, and typed queries (equality + `And`/`Or` + ranges + `limit` + `order_by` + streaming `query_iter`).

**In-memory** mode is implemented via the same APIs (`Database::open_in_memory` / Python `open_in_memory`) with snapshot export/import.

The “hybrid/streaming” story is still evolving—see [`ROADMAP.md`](https://github.com/eddiethedean/typra/blob/main/ROADMAP.md). For where `Store`, `FileStore`, and `VecStore` sit in the Rust workspace, see [Rust crate/module layout](../specs/rust_crate_layout.md).

## Mode 1: On-disk (default)

**What it is**: a single `.typra` file on disk.

**What it’s for**:

- durable embedded persistence
- “ship a file with your app” simplicity

Design reference: [On-disk file format spec](../specs/on_disk_file_format.md).

## Mode 2: In-memory (fast, explicit snapshot)

**What it is**: the same logical database API, but state is held in RAM.

**Persistence model**:

- no implicit durability
- explicit snapshot save/load to a normal `.typra` file

**Why it matters**:

- faster operations (no IO latency for steady-state work)
- great for tests, prototypes, and UI flows that want speed

## Mode 3: Hybrid buffered + streaming (file-backed, beyond-RAM workloads)

This is the long-term mode for datasets too large to fit in memory while still being an embedded database.

### Hybrid buffered execution (buffer pool / pager)

What it means:

- the database is still the normal on-disk `.typra` database
- the engine maintains an internal buffer pool that pulls pages/segments into RAM on demand
- dirty data is written back to the same file when needed

This provides SQLite-style behavior: fast hot working set in RAM with durable backing storage.

### Streaming / bounded-memory execution (query engine)

For operations like joins and groupby/aggregations, naive implementations require all input to fit in RAM. Typra’s query execution should evolve toward:

- pull-based streaming operators for scans/filters/limits
- bounded-memory algorithms for aggregations and joins (spillable/grace hash join strategies)

### Spill location (planned default)

To keep the “single-file database” mental model intact, spilling should default to internal temporary segments inside the same `.typra` file (unless there’s a compelling operational reason to use a sidecar).

## Practical guidance

- If you need durability: use **on-disk**.
- If you need speed and can snapshot at boundaries: use **in-memory + explicit snapshots**.
- If you need to work beyond RAM: plan for **hybrid buffered + streaming**, once the query engine and buffer pool are implemented.

