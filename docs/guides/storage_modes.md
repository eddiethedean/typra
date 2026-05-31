# Storage modes

Typra feels SQLite-like in ergonomics with stronger typing and validation. This page explains **on-disk**, **in-memory**, and the planned **hybrid/streaming** modes.

## Current status (1.0)

| Mode | Status |
|------|--------|
| **On-disk** | ✅ Open/create, superblocks, checksummed segments, schema catalog, indexes, typed queries, transactions |
| **In-memory** | ✅ Same APIs (`open_in_memory` / `Database::open_in_memory`) + snapshot export/import |
| **Hybrid / streaming** | 🔜 Roadmap — buffer pool + bounded-memory query operators |

Engine layout: [Rust crate layout](../specs/rust_crate_layout.md) · Plans: [ROADMAP](https://github.com/eddiethedean/typra/blob/main/ROADMAP.md)

## On-disk (default)

**One `.typra` file** — durable embedded persistence, “ship a file with your app.”

```python
db = typra.Database.open("app.typra")
```

```rust
let db = Database::open("app.typra")?;
```

Format details: [On-disk file format](../specs/on_disk_file_format.md).

!!! tip "When to choose this"
    Any production app that needs durability without a separate database server.

## In-memory (fast, explicit snapshot)

Same logical API; state lives in RAM.

| Property | Behavior |
|----------|----------|
| Durability | None implicit — snapshot when you choose |
| Speed | No steady-state I/O latency |
| Use cases | Tests, prototypes, ephemeral UI state |

```python
db = typra.Database.open_in_memory()
data = db.snapshot_bytes()          # save
db2 = typra.Database.open_snapshot_bytes(data)  # restore
```

```rust
let db = Database::open_in_memory()?;
```

## Hybrid & streaming (planned)

For datasets larger than RAM while staying embedded.

### Hybrid buffered (pager / buffer pool)

- Database remains a normal on-disk `.typra` file
- Internal buffer pool loads pages/segments on demand
- Dirty data written back to the same file

SQLite-style: hot working set in RAM, durable backing storage.

### Streaming query execution

Pull-based operators for scans/filters/limits; bounded-memory aggregations and joins (spillable strategies).

### Spill location (planned default)

Internal temporary segments **inside the same `.typra` file** — preserves the single-file mental model.

## Decision guide

```mermaid
flowchart TD
    A[Need durability?] -->|Yes| B[On-disk]
    A -->|No| C[In-memory + snapshots]
    D[Dataset >> RAM?] --> E[Hybrid/streaming when available]
    B --> F[Production apps]
    C --> G[Tests & prototypes]
```

| Need | Mode |
|------|------|
| Durability | **On-disk** |
| Speed + explicit save points | **In-memory + snapshots** |
| Beyond-RAM workloads | **Hybrid** (when shipped) |
