# Typed Embedded Database – Full Architecture Specification

## Vision
A lightweight embedded database written in Rust for **typed application data**. It should preserve the ergonomics that make SQLite appealing—single-file storage, zero-admin deployment, portability, and reliability—while replacing loose row-oriented semantics with a first-class typed schema model.

This database is not intended to be a general “Postgres replacement.” Its purpose is to be the best local persistence engine for applications whose developers already think in:
- Rust structs
- Pydantic models
- TypeScript schemas
- validated nested objects

## Product Thesis
The native abstraction of the system is not a table; it is a **schema contract** attached to a **collection of records**.

Each collection contains records validated against a schema that defines:
- logical field types
- nullability / optionality
- defaults
- validators
- index metadata
- compatibility rules for schema evolution
- object nesting rules
- list element typing

## Product Goals
1. Single-file embedded database.
2. Strict schemas by default.
3. Native nested objects and typed lists.
4. Excellent validation errors.
5. Safe schema evolution.
6. Fast point lookups and useful filtered reads.
7. Transactional durability.
8. Rust-first core with ergonomic Python bindings.
9. Predictable concurrency model for local apps.
10. Long-term path to TypeScript / Tauri / Electron support.

## Non-Goals for v1
1. Distributed operation.
2. Multi-node replication.
3. Full SQL compatibility.
4. Network server mode.
5. Full text search.
6. Vector search.
7. Browser-native storage engine.
8. High-scale analytics competition with DuckDB.
9. Cross-process high-write concurrency.

## Conceptual Model
The user interacts with:
- **Database**: a file-backed engine instance
- **Schema Registry**: catalog of collection definitions
- **Collection**: typed record container
- **Record**: validated structured document
- **Index**: lookup / uniqueness / range accelerator
- **Transaction**: atomic read/write boundary
- **Migration**: schema evolution unit

## Data Model Overview

### Primitive Types
- `bool`
- `int8`, `int16`, `int32`, `int64`
- `uint8`, `uint16`, `uint32`, `uint64`
- `float32`, `float64`
- `string`
- `bytes`
- `uuid`
- `date`
- `time`
- `timestamp`

### Composite Types
- `optional<T>`
- `list<T>`
- `object<{...}>`
- `enum<...>`

### Deferred / Future Types
- `decimal`
- `map<string, T>`
- `tuple<...>`
- `reference<T>`
- `union<T...>` / tagged union
- `vector<f32, N>`

## Optionality Model
Optionality must distinguish:
1. required and non-null
2. required but nullable
3. omitted => default
4. omitted allowed but distinct from null

Recommended semantics:
- **required**: key must appear on write unless default exists
- **nullable**: value may be null
- **defaulted**: omission permitted; engine writes default or resolves lazily
- **omittable**: omission preserved in logical record if desired

For v1, prefer simpler semantics:
- required / optional-with-default / nullable
- omit-vs-null preserved only where necessary for compatibility

## Record Semantics
Collections are document-like, but with schema contracts. Example:

```rust
collection User {
    id: Uuid @primary
    email: String @unique @validate(email)
    name: String
    age: Option<U16>
    role: Enum["admin", "member", "viewer"]
    profile: {
        display_name: String,
        timezone: String,
        marketing_opt_in: Bool = false
    }
    tags: List<String> = []
    created_at: Timestamp
}
```

## Storage Philosophy
Use an **append-only log of record versions** plus **materialized secondary indexes**.

Why:
- simpler crash recovery story
- natural future history/time-travel support
- straightforward MVCC / snapshot reads
- easy durability model
- good fit for validated structured records

## Core Engine Components

### 1. File Header
Contains:
- magic bytes
- engine version
- format version
- feature flags
- root pointers / segment table location
- checksum metadata

### 2. Schema Catalog
Persistent metadata store for:
- collection IDs and names
- schema versions
- field definitions
- validators
- index definitions
- schema evolution history
- compatibility notes

### 3. Record Log
Append-only segments containing:
- insert events
- replace/update events
- delete tombstones
- schema evolution markers
- transaction commit markers

### 4. Index Store
Separate physical structures for:
- primary key index
- unique indexes
- field indexes
- nested path indexes

### 5. Snapshot Manager
Maintains consistent transactional read views by logical sequence number or transaction ID.

### 6. Compactor
Rewrites old segments, merges live records, rebuilds stale indexes, and prunes obsolete versions as policy allows.

## Record Lifecycle
1. User submits object or typed model.
2. Engine resolves target collection schema.
3. Input coerced only if strictness policy allows.
4. Full validation performed.
5. Defaults applied.
6. Index constraints checked.
7. Record version serialized.
8. Write appended to log inside transaction.
9. Index mutations staged and committed.
10. Commit marker written.
11. Readers see new snapshot after commit.

## Update Model
Public API may accept patch-style updates, but internally the engine should:
1. load current logical record
2. apply patch
3. validate resulting whole record
4. append full new record version

This simplifies validation and compaction.

## Transaction Model
v1 should support:
- atomic single-operation writes
- multi-operation transactions
- many concurrent readers
- one writer at a time per process
- snapshot isolation for readers

Reader/writer strategy:
- readers use immutable snapshot IDs
- writer appends and atomically publishes new commit sequence

## Durability Model
- WAL-like append log is source of truth
- commit record marks visibility
- fsync policy configurable:
  - safe: fsync each commit
  - balanced: group commits
  - relaxed: app-managed durability window

## Validation Model

### Layer 1: Structural Type Validation
Checks:
- required fields
- primitive type correctness
- nested object structure
- list element type correctness
- enum membership

### Layer 2: Field Constraints
Checks:
- min/max numeric bounds
- string length
- regex
- uniqueness
- path-specific shape rules
- non-empty list

### Layer 3: Record-Level Invariants
Checks:
- cross-field relationships
- conditional requirements
- custom validators

Examples:
- `start_at <= end_at`
- if `plan == "free"` then `billing_customer_id` must be null
- if `country == "US"` then `state` required

## Error Model
Errors should be structured and high quality:
- field path
- expected type / rule
- actual value / actual type
- code
- human-readable message
- schema version
- optional hint

Example:
```json
{
  "code": "enum.invalid_member",
  "path": "role",
  "message": "Expected one of ['admin', 'member', 'viewer'], got 'owner'",
  "schema_version": 3
}
```

## Query Philosophy
v1 should be model-first, not SQL-first.

### Desired capabilities
- point lookup by primary key
- equality filters
- nested path equality filters
- simple conjunctions
- ordering
- limit / offset or cursor pagination
- optional range predicates on indexed scalar fields

### Example API
```python
db.users.where(
    User.role == "member",
    User.profile.timezone == "America/New_York"
).order_by(User.created_at.desc()).limit(50)
```

## Indexing
Required v1 indexes:
- primary key
- unique
- scalar equality
- nested path equality

Nice-to-have:
- ordered/range index for timestamps and numbers
- compound index
- prefix index for strings

## Schema Evolution Principles
Schema evolution should be interpreted through compatibility rules.

### Safe
- add optional field
- add field with default
- add enum member
- add new index
- widen integer size

### Requires backfill or transform
- rename field
- move field into nested object
- convert string to enum
- split field into multiple fields

### Breaking
- remove field with existing data
- remove enum member
- narrow numeric width
- change list element type incompatibly

## Migration Execution Model
Migration object:
```rust
Migration {
    from: 3,
    to: 4,
    changes: [
        AddField { path: "age", ty: Optional<U8>, default: Null },
        AddIndex { path: "profile.timezone" }
    ]
}
```

Execution stages:
1. compare old and new schemas
2. classify changes
3. require transform code for unsafe changes
4. apply schema metadata update
5. optionally rewrite or backfill records
6. rebuild affected indexes
7. publish new schema version

## API Design Principles
- typed model registration
- collection handles
- transaction blocks
- explicit errors
- ergonomic defaults
- raw record access possible, but typed use encouraged

## Python Strategy
Python can be the fastest adoption path:
- PyO3 bindings
- Pydantic compatibility
- model registration from Pydantic classes
- typed query builder
- conversion to/from Python dicts
- zero-copy opportunities where practical

## Rust Strategy
Rust remains the implementation truth:
- derive macros for schemas
- strongly typed collection handles
- serde-friendly interop
- explicit transaction and error APIs

## TypeScript Strategy
Later, provide:
- Tauri/Electron bindings
- schema DSL or zod-style schema registration
- typed query helpers

## Security / Integrity
v1 should include:
- file checksums
- optional page/segment checksums
- crash-safe commits
- defensive schema validation
- no unsafe untrusted extension runtime

## Observability
Helpful capabilities:
- schema inspection
- index stats
- segment stats
- compaction stats
- last checkpoint / last commit metadata
- explain-plan for queries

## Performance Goals for v1
1. Fast startup.
2. Fast point lookups.
3. Good performance for small and medium collections.
4. Predictable write latency.
5. Acceptable range scans on indexed fields.
6. Compact file size with compaction.

## Suggested MVP
- single file
- append-only segments
- one writer / many readers
- schema catalog
- strict validation
- nested objects
- typed lists
- unique and primary indexes
- schema evolution classifier
- Rust + Python APIs

## Future Extensions
- history / time-travel queries
- replication / sync
- read-only SQL compatibility layer
- FTS
- browser/WASM storage backend
- server wrapper mode
- change streams / subscriptions

## Positioning Statement
**A lightweight embedded Rust database for typed application data, with strict schemas, nested objects, lists, enums, write-time validation, and safe schema evolution.**
