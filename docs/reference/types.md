# Types matrix

This document is a **truth table** for what Typra supports *today* across:

- catalog field `Type`
- row values (`RowValue` / `ScalarValue`)
- constraints
- indexes + predicates

It is intentionally conservative: if something is partially implemented or shape-limited, it is called out explicitly.

## Types (`schema::Type`) and row values

### Primitive scalars

- **`Bool`**: `RowValue::Bool`, `ScalarValue::Bool`
- **`Int64`**: `RowValue::Int64`, `ScalarValue::Int64`
- **`Uint64`**: `RowValue::Uint64`, `ScalarValue::Uint64`
- **`Float64`**: `RowValue::Float64`, `ScalarValue::Float64`
- **`String`**: `RowValue::String`, `ScalarValue::String`
- **`Bytes`**: `RowValue::Bytes`, `ScalarValue::Bytes`
- **`Uuid`**: `RowValue::Uuid`, `ScalarValue::Uuid`
- **`Timestamp`**: `RowValue::Timestamp`, `ScalarValue::Timestamp`

### Composites

- **`Optional(T)`**
  - Value may be absent at the root (treated as `RowValue::None`).
  - Nested optionals are supported as part of list/object trees.
- **`List(T)`**
  - `RowValue::List(Vec<RowValue>)` (homogeneous by validation rules).
- **`Object(fields)`**
  - Stored as `RowValue::Object(BTreeMap<String, RowValue>)`.
  - Object fields are validated recursively against the declared nested field definitions.
- **`Enum(variants)`**
  - Stored as `RowValue::Enum(String)` and validated against the allowed variant set.

### Schema path shape (important limitation)

- **Top-level field defs** in a collection schema must currently be **single-segment** paths (e.g. `["title"]`).
  - Nested structure should be expressed via `Type::Object(...)` on a top-level field (e.g. `["profile"] : Object([...])`), not via multi-segment top-level field paths.
  - Multi-segment field paths are supported in **queries/projections/indexes**, but defining them as *top-level schema fields* is not fully supported yet.

## Constraints (`schema::Constraint`)

Constraints are enforced **on write** (insert/replace), after type checks:

- **Numeric**: `MinI64`, `MaxI64`, `MinU64`, `MaxU64`, `MinF64`, `MaxF64`
- **Length**: `MinLength`, `MaxLength` (string bytes length or list length)
- **String shape**: `Regex`, `Email`, `Url`
- **Non-empty**: `NonEmpty` (string/bytes/list)

## Indexes

- **Kinds**: `Unique`, `NonUnique`
- **Indexed value type**: index keys are derived from a **scalar value** at the index path.
- **Paths**
  - Index paths may be **nested** (e.g. `["profile","timezone"]`) as long as the stored row value at that path is a scalar.
- **Maintenance**
  - Index maintenance is transactional and persisted via `SegmentType::Index`.
  - Inserts use replace-by-primary-key semantics; index deltas are applied for replace/delete.

## Queries (typed query AST)

### Predicates

- **Equality**: `path == value`
- **Boolean composition**: `AND`, `OR`
- **Ranges**: `<`, `<=`, `>`, `>=` (where supported by the scalar type)

### Operators

- **`limit`**: supported
- **`order_by`**: supported
  - May spill to ephemeral `Temp` segments on file-backed databases to avoid unbounded memory usage.
- **Projection**: supported (subset projections by field paths)

## SQL / DB-API (Python)

- SQL text is intentionally minimal and exists to support a read-only subset of DB-API.
- For application code, prefer the Rust/Python typed query builder APIs.

