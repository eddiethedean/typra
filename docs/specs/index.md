# Specifications

**Audience:** advanced (engine contributors, format tooling)

Design and **wire-format** documents for the ModelVault engine. Application developers should start with [Why ModelVault](../guides/why_modelvault.md), [Quickstart](../guides/quickstart.md), and [Reference](../reference/types.md).

## Normative on-disk specification (1.0.x)

These pages describe the **implemented** `.modelvault` file format. [Compatibility](../reference/compatibility.md) defines read/write and recovery policy.

| Document | Contents |
|----------|----------|
| [Format evolution (1.x)](format_evolution.md) | Backwards-compat rules for 1.y releases |
| [On-disk file format](on_disk_file_format.md) | Header, superblocks, segment framing, manifest, txn markers, checkpoint, replay order |
| [Catalog encoding](catalog_encoding.md) | `Schema` segment payloads (collections, fields, constraints, indexes) |
| [Index segment encoding](index_segment_encoding.md) | `Index` segment payloads |
| [Record encoding v1](record_encoding_v1.md) | Primitive record payloads (read compat) |
| [Record encoding v2](record_encoding_v2.md) | `RowValue` record payloads (default for flat schemas) |
| [Record encoding v3](record_encoding_v3.md) | Multi-segment `FieldPath` record payloads |

**Code references:** `crates/modelvault-core/src/file_format.rs`, `superblock.rs`, `segments/`, `catalog/codec.rs`, `record/`, `index.rs`.

## Architecture & product

| Document | Contents |
|----------|----------|
| [Full architecture](full_architecture.md) | System shape, components, non-goals |
| [Typed embedded DB vision](typed_embedded_db.md) | Product thesis and validation model |
| [Rust crate layout](rust_crate_layout.md) | Workspace modules and boundaries |

## Schema & queries

| Document | Contents |
|----------|----------|
| [Schema DSL](schema_dsl.md) | Field-path invariants; link to catalog wire format |
| [Query planner & execution](query_planner.md) | Planner, indexes, operators |

## Historical / design-only (not on this site)

Numbered files under `docs/` (e.g. `02_on_disk_file_format.md`, `04_schema_dsl_spec.md`, `06_record_encoding_v1.md`) are **legacy or exploratory** copies. Prefer the **`specs/`** pages above for the published contract.

## Related reference (user-facing)

- [Compatibility matrix](../reference/compatibility.md) — read/write policy by format minor
- [Types matrix](../reference/types.md) — supported types and query predicates today
