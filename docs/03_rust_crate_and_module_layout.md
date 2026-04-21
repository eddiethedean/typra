# Typed Embedded Database – Rust Crate and Module Layout

## Goals
The Rust project layout should:
- separate storage internals from public API
- keep unsafe code minimal or zero if possible
- support future Python bindings cleanly
- make testing isolated and practical
- allow incremental engine growth

## Workspace Layout

```text
typra/
├── Cargo.toml
├── crates/
│   ├── typra-core/
│   ├── typra-storage/
│   ├── typra-schema/
│   ├── typra-query/
│   ├── typra-migrate/
│   ├── typra-derive/
│   ├── typra-cli/
│   └── typra-bench/
├── python/
│   └── typra/          # PyPI package (maturin + PyO3); kept out of crates/
├── examples/
├── docs/
└── scripts/
```

## Crate Responsibilities

### `typra-core`
Public engine façade and shared primitives.
Contains:
- `Database`
- `Transaction`
- engine configuration
- error types
- collection handles
- trait glue across subsystems

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
- query builder wrappers
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

## Internal Modules

### `typra-core`
```text
src/
├── lib.rs
├── db.rs
├── config.rs
├── error.rs
├── collection.rs
├── transaction.rs
├── snapshot.rs
├── value.rs
└── stats.rs
```

#### Key Types
- `Database`
- `DatabaseBuilder`
- `CollectionHandle<T>`
- `UntypedCollectionHandle`
- `Transaction`
- `Snapshot`
- `DbError`

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

## Public API Structure
The public API should mainly re-export:
- `Database`
- `DatabaseBuilder`
- `DbModel`
- `Transaction`
- `Query`
- `MigrationPlan`
- `DbError`

Keep storage internals private unless explicitly needed for tooling.

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

## Unsafe Code Policy
Prefer safe Rust.
If unsafe is required:
- isolate in one module
- document invariants
- fuzz aggressively

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
