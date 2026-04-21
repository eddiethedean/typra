# Typra User Guide: Storage Modes

Typra is designed to feel SQLite-like in ergonomics while providing stronger typing and validation.

This guide explains the planned **storage modes** and what they mean for performance and persistence.

## Current status (important)

Today, the **on-disk** path supports open/create, superblocks, checksummed segments, manifest publication, and a **persisted schema catalog** (`register_collection` / `register_schema_version` on disk). **In-memory** and **hybrid/streaming** modes below are not implemented yet; see [`ROADMAP.md`](/Users/odosmatthews/Documents/coding/typra/ROADMAP.md) for timing.

## Mode 1: On-disk (default)

**What it is**: a single `.typra` file on disk.

**What it’s for**:
- durable embedded persistence
- “ship a file with your app” simplicity

**Key properties** (target):
- crash safety
- versioned format
- append-friendly segments + checkpoints

Design reference: [`docs/02_on_disk_file_format.md`](/Users/odosmatthews/Documents/coding/typra/docs/02_on_disk_file_format.md).

## Mode 2: In-memory (fast, explicit snapshot)

**What it is**: the same logical database API, but state is held in RAM.

**Persistence model**:
- no implicit durability
- **explicit snapshot save/load** to a normal `.typra` file

**Why it matters**:
- faster operations (no IO latency for steady-state work)
- great for tests, prototypes, and UI flows that want speed

**Snapshot expectations** (target):
- exported snapshots should be readable as a standard Typra on-disk database
- import should rehydrate schemas and records into RAM

## Mode 3: Hybrid buffered + streaming (file-backed, beyond-RAM workloads)

This is the long-term mode for “datasets too large to fit in memory” while still being an embedded database.

### Hybrid buffered execution (buffer pool / pager)

**What it means**:
- the database is still the normal on-disk `.typra` database
- the engine maintains an internal **buffer pool** that pulls pages/segments into RAM on demand
- dirty data is written back to the same file when needed

This provides the familiar SQLite-style behavior: fast hot working set in RAM with durable backing storage.

### Streaming / bounded-memory execution (query engine)

For operations like **joins** and **groupby/aggregations**, a naive implementation requires all input to fit in RAM. Typra’s query execution should evolve toward:

- pull-based streaming operators for scans/filters/limits
- bounded-memory algorithms for:
  - aggregations (hash aggregation with spill; external sort where needed)
  - joins (spillable/grace hash join strategies)

### Spill location (planned default)

To keep the “single-file database” mental model intact, spilling should default to **internal temporary segments inside the same `.typra` file**, unless there’s a compelling operational reason to use a sidecar.

## Practical guidance

- If you need durability: use **on-disk**.
- If you need speed and can snapshot at boundaries: use **in-memory + explicit snapshots**.
- If you need to work beyond RAM: plan for **hybrid buffered + streaming**, once the query engine and buffer pool are implemented.

