# Specifications

**Audience:** advanced (engine contributors, format tooling)

Design and format documents for the Typra engine. Application developers should start with [Why Typra](../guides/why_typra.md), [Quickstart](../guides/quickstart.md), and [Reference](../reference/types.md) — use this section when you need **on-disk layout, encodings, or planner semantics**.

!!! note "Legacy copies"
    Numbered files at the repo root (e.g. `docs/01_full_architecture_spec.md`) are excluded from this site; the **`specs/`** copies below are what we publish.

## Architecture & product

| Document | Contents |
|----------|----------|
| [Full architecture](full_architecture.md) | System shape, components, non-goals |
| [Typed embedded DB vision](typed_embedded_db.md) | Product thesis and validation model |
| [Rust crate layout](rust_crate_layout.md) | Workspace modules and boundaries |

## On-disk & encoding

| Document | Contents |
|----------|----------|
| [On-disk file format](on_disk_file_format.md) | Header, superblocks, segments, recovery |
| [Record encoding v1](record_encoding_v1.md) | Legacy primitive payloads |
| [Record encoding v2](record_encoding_v2.md) | Nested `RowValue` encoding |
| [Record encoding v3](record_encoding_v3.md) | Multi-segment field paths |

## Schema & queries

| Document | Contents |
|----------|----------|
| [Schema DSL](schema_dsl.md) | Catalog types, constraints, evolution |
| [Query planner & execution](query_planner.md) | Planner, indexes, operators |

## Related reference (user-facing)

- [Compatibility matrix](../reference/compatibility.md) — read/write policy by format minor
- [Types matrix](../reference/types.md) — supported types and query predicates today
