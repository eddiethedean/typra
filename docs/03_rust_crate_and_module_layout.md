# Typed Embedded Database – Rust Crate and Module Layout

## Goals
The Rust project layout should:
- separate storage internals from public API
- forbid `unsafe` at the workspace level (root [`Cargo.toml`](../Cargo.toml) **`[workspace.lints.rust]`**)
- support future Python bindings cleanly
- make testing isolated and practical
- allow incremental engine growth

## Implementation note (current)

The **current** Cargo workspace members are **`typra`**, **`typra-core`**, **`typra-derive`**, and **`typra-python`** (PyO3 package under `python/typra/`). Names like **`typra-storage`**, **`typra-query`**, and **`typra-migrate`** describe **planned** crate splits that are **not** separate directories or published crates yet; file I/O, segments, catalog, and record encoding live inside **`typra-core`** today.

## Workspace layout

```text
typra/
├── Cargo.toml                 # [workspace.package] version; workspace.lints: unsafe_code = forbid
├── crates/
│   ├── typra-core/            # engine (Database, Store, segments, catalog, records)
│   ├── typra-derive/          # #[derive(DbModel)]
│   └── typra/                 # facade: re-exports typra-core + optional derive
├── python/
│   └── typra/                 # PyPI package (Cargo package name: typra-python)
├── docs/
├── scripts/
└── ...
```

### Future crate splits (planned, not in this tree yet)

Design docs may refer to **`typra-storage`**, **`typra-schema`**, **`typra-query`**, **`typra-migrate`**, **`typra-cli`**, and **`typra-bench`** as eventual extracted crates. Until then, treat them as **logical boundaries** inside **`typra-core`** (or as future work), not as workspace members.

## Crate Responsibilities

### `typra-core`
Public engine façade and shared primitives: **`Database<S: Store>`**, persisted **catalog** (decode **v1–v4**; new registrations write **v4** with per-field **constraints** and optional **`indexes`**), **record** payload **v1 + v2** (decode both; new inserts use **v2**), **`SegmentType::Index`** append + replay into **`IndexState`**, minimal **`query`** AST + **planner** + **`query_iter`**, **segment** I/O, **superblock** / **manifest** publication, **`validation`** (types + constraints before write), and **error** types. **`config`** remains a small placeholder for future engine configuration ([`ROADMAP.md`](../ROADMAP.md)).

### `typra-storage`
Low-level storage engine.
Contains:
- file open/close
- segment writer/reader
- manifest
- superblock management
- durability and recovery
- compaction

### `typra-schema`
Schema system.
Contains:
- type definitions
- collection schemas
- field constraints
- validator metadata
- schema registry structures
- schema hashing / compatibility comparisons

### `typra-query`
Query IR and execution planning.
Contains:
- filter expression AST
- typed path expressions
- sort spec
- query planner
- executor coordination with indexes / scans

### `typra-migrate`
Schema evolution support.
Contains:
- migration plan model
- compatibility classifier
- backfill / transform orchestration
- schema diff engine

### `typra-derive`
Procedural macros.
Contains:
- derive macros for Rust structs/enums
- schema generation helpers
- field attribute parsing
- compile-time diagnostics

### Python package (`python/typra`)
PyO3 bindings (Cargo package name may remain `typra-python` for crates.io).
Contains:
- Python module entrypoint
- model registration bridge
- dict/model conversion
- **query** builder wrappers (`collection`, `where`, …)
- exception mapping

### `typra-cli`
Debug/admin/developer tool.
Contains:
- inspect file header
- print schema catalog
- list collections
- verify checksums
- dump records
- rebuild indexes
- compact database

### `typra-bench`
Benchmarks and profiling harness.
Contains:
- criterion benches
- dataset generators
- performance comparison scripts

## Internal modules

### `typra-core` (current `src/` layout)

The engine is organized around **`db/`** (open, replay, append writes), **`catalog/`**, **`record/`**, **`query/`**, **`segments/`**, plus **`index.rs`**, and shared **`storage`**, **`file_format`**, **`superblock`**, **`manifest`**, and **`publish`**.

```text
src/
├── lib.rs
├── db/
│   ├── mod.rs          # Database<S: Store>, public API
│   ├── open.rs
│   ├── replay.rs
│   ├── write.rs
│   └── helpers.rs
├── catalog/
│   ├── mod.rs
│   ├── codec.rs
│   └── state.rs
├── query/
│   ├── mod.rs
│   ├── ast.rs          # Query, Predicate
│   └── planner.rs      # plan_query, execute_query, execute_query_iter, explain
├── record/
│   ├── mod.rs
│   ├── payload_v1.rs
│   ├── payload_v2.rs
│   ├── row_value.rs
│   └── scalar.rs
├── segments/
│   ├── mod.rs
│   ├── header.rs       # SegmentType includes Index (0.7.0+)
│   ├── reader.rs
│   └── writer.rs
├── index.rs            # IndexState, index segment payload codec (0.7.0+)
├── storage.rs          # Store trait, FileStore, VecStore
├── schema.rs           # FieldDef, IndexDef, DbModel marker, …
├── error.rs
├── file_format.rs
├── superblock.rs
├── manifest.rs
├── publish.rs
├── checksum.rs
├── config.rs           # placeholder / reserved
└── validation.rs       # validate_value + constraints (0.6.0+)
```

#### Key types (shipped today)

- **`Database<S: Store>`** — default `Database` = on-disk **`FileStore`**; **`open_in_memory`** uses **`VecStore`**
- **`Store`**, **`FileStore`**, **`VecStore`**
- **`Catalog`**, **`CollectionInfo`**, catalog replay records
- **`DbError`**, **`SchemaError`**, **`ValidationError`** (**`DbError::Validation`**), format/manifest/superblock errors as in **`error.rs`**
- **`ScalarValue`**, **`RowValue`**, **`Constraint`**, **`CollectionSchema`**, **`FieldDef`**, **`IndexDef`**, **`Type`**, **`SchemaVersion`**, **`CollectionId`**
- **`Query`**, **`Predicate`**, **`QueryRowIter`**, **`IndexState`**, **`row_subset_by_field_defs`**
- **`DbModel`** marker trait (derive lives in **`typra-derive`**)

Not yet in the public API: typed **`CollectionHandle<T>`**, rich SQL text / full DB-API compatibility (see [`ROADMAP.md`](../ROADMAP.md)). Minimal **non-SQL** queries and **secondary indexes** ship (**0.7.0**); multi-write **transactions** ship (**0.8.0**); `OR` / range predicates / `order_by` ship (**0.9.0**); an experimental, read-only **DB-API 2.0** adapter with a minimal `SELECT` subset ships in **0.10.0** (Python `typra.dbapi`); pager/checkpointed open ships in **0.11.0**; initial bounded-memory scaffolding (ephemeral `Temp` segments, external sort plumbing) lands in **0.12.0**; fuzz/property tests and spillable agg/join foundations land in **0.13.0**; multi-segment schema field paths are supported as of **1.0.0**.

### `typra-storage`
```text
src/
├── lib.rs
├── file.rs
├── header.rs
├── superblock.rs
├── segment/
│   ├── mod.rs
│   ├── writer.rs
│   ├── reader.rs
│   ├── kinds.rs
│   └── checksum.rs
├── log/
│   ├── mod.rs
│   ├── append.rs
│   ├── commit.rs
│   └── recovery.rs
├── index/
│   ├── mod.rs
│   ├── primary.rs
│   ├── unique.rs
│   ├── field.rs
│   └── nested.rs
├── manifest.rs
├── checkpoint.rs
├── compaction.rs
└── cache.rs
```

### `typra-schema`
```text
src/
├── lib.rs
├── schema.rs
├── field.rs
├── types.rs
├── constraints.rs
├── validators.rs
├── catalog.rs
├── compatibility.rs
└── encode.rs
```

### `typra-query`
```text
src/
├── lib.rs
├── ast.rs
├── expr.rs
├── path.rs
├── sort.rs
├── planner.rs
├── plan.rs
├── executor.rs
└── optimize.rs
```

### `typra-migrate`
```text
src/
├── lib.rs
├── diff.rs
├── classify.rs
├── plan.rs
├── backfill.rs
├── transform.rs
└── apply.rs
```

## Core Traits and Interfaces

### Schema Derivation
```rust
pub trait DbModel {
    fn schema() -> CollectionSchema;
    fn collection_name() -> &'static str;
}
```

### Serialization
```rust
pub trait RecordCodec<T> {
    fn encode(value: &T, schema: &CollectionSchema) -> Result<Vec<u8>, DbError>;
    fn decode(bytes: &[u8], schema: &CollectionSchema) -> Result<T, DbError>;
}
```

### Validation
```rust
pub trait Validator {
    fn validate(&self, value: &RecordValue) -> Result<(), ValidationError>;
}
```

### Migration Transform
```rust
pub trait RecordTransform {
    fn transform(&self, old: RecordValue) -> Result<RecordValue, MigrationError>;
}
```

## Recommended Error Hierarchy
```rust
pub enum DbError {
    Io(IoError),
    Format(FormatError),
    Schema(SchemaError),
    Validation(ValidationError),
    Constraint(ConstraintError),
    Query(QueryError),
    Migration(MigrationError),
    Conflict(ConflictError),
}
```

## Public API structure (today)

**`typra-core`** re-exports **`Database`**, schema/catalog/record types, and **`prelude`** (see `lib.rs`). **`typra`** re-exports **`typra_core`** and, with the default **`derive`** feature, **`#[derive(DbModel)]`**.

Longer term, queries, migrations, and richer handles may join the stable surface; keep storage segments and checksum details private unless tooling requires them.

## Testing Strategy
Each crate should have:
- unit tests for local logic
- integration tests against public API
- corruption/recovery tests for storage
- snapshot tests for schema derivation
- migration compatibility tests

### Dedicated test groups
1. validation tests
2. nested object tests
3. list encoding tests
4. uniqueness and index tests
5. crash recovery tests
6. compaction correctness tests
7. migration classification tests

## Unsafe code policy

The workspace sets **`unsafe_code = forbid`** in the root **`Cargo.toml`**. Any future exception would require lifting that lint explicitly and documenting invariants and test coverage.

## Feature Flags
Potential cargo features:
- `python`
- `zstd`
- `cli`
- `serde`
- `history`
- `tracing`
- `strict`

## Dependency Philosophy
Keep core dependencies lean.
Likely useful:
- `serde`
- `uuid`
- `thiserror`
- `crc32c`
- `bytes`
- `parking_lot`
- `pyo3` in Python crate
- `criterion` in bench crate

## Build and Release Strategy
- workspace versioning aligned initially
- semver per crate
- Python bindings released from pinned workspace version
- format compatibility tested across versions

## Recommended Naming Notes
If you later brand the project, keep internal crate names stable and close to the project name.
Example:
- `aurora-core`
- `aurora-storage`
- `aurora-schema`

## Minimal MVP Workspace
For an MVP, you can start with fewer crates:

```text
typra/
├── crates/
│   ├── typra/          # application facade (depends on typra-core + typra-derive)
│   ├── typra-core/
│   └── typra-derive/
└── python/
    └── typra/
```

And keep `storage`, `schema`, and `query` as internal modules inside `typra-core` until they grow enough to split out.
