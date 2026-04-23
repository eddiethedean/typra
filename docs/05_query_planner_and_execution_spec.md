# Typed Embedded Database – Query Planner and Execution Specification

## Implementation status (as of **0.8.0**)

The **`typra-core`** crate includes a minimal **AST** (`Query`, `Predicate::Eq` / `And`), **heuristic planning** (prefer a matching **unique** index, else **non-unique** index, else **collection scan** with optional **residual** predicates), **`execute_query`** / **`execute_query_iter`**, string **`explain`**, and **`limit`**. **Python** exposes a small builder on **`collection(...)`** (`where`, `and_where`, `limit`, `explain`, `all`, `all(fields=[...])`). This spec still describes the **full** target model; **order_by**, **offset**, **range** filters, and **OR** are **not** implemented yet.

## Goals
The query layer should:
- be typed
- avoid SQL parsing in v1
- support practical application queries
- take advantage of indexes
- produce explainable plans
- remain extensible

## Query Model
The v1 query model is a structured AST rather than SQL text.

### Core Query Operations
- `get(pk)`
- `filter`
- `where`
- `order_by`
- `limit`
- `offset`
- cursor pagination later
- projection later if useful

### Predicate Support in v1
- equality on scalar fields
- equality on nested scalar paths
- conjunction (`AND`)
- optional range ops on indexed numeric/timestamp fields

### Future Predicate Support
- `OR`
- `IN`
- prefix string search
- contains for list fields
- full-text integration
- joins/references

## Query AST
Example conceptual AST:

```rust
Query {
    collection: "User",
    filter: And([
        Eq(path("role"), "member"),
        Eq(path("profile.timezone"), "America/New_York")
    ]),
    order_by: [Desc(path("created_at"))],
    limit: 50
}
```

## Typed Paths
Every field path is resolved against schema metadata.
Examples:
- `role`
- `created_at`
- `profile.timezone`

Planner should reject invalid paths before execution.

## Planning Pipeline
1. parse/construct typed query AST
2. validate against schema
3. normalize predicates
4. inspect available indexes
5. estimate candidate plans
6. choose winning plan
7. execute against snapshot
8. materialize typed records or raw records

## Logical Plan Nodes
- `CollectionScan`
- `IndexLookup`
- `IndexRangeScan`
- `Filter`
- `Sort`
- `Limit`
- `Materialize`
- `Project` (future)

## Physical Plan Examples

### 1. Primary Key Get
```text
IndexLookup(pk=id) -> Materialize
```

### 2. Equality on Indexed Field
```text
IndexLookup(role="member") -> Filter(profile.timezone=...) -> Limit
```

### 3. Equality on Nested Indexed Path
```text
IndexLookup(profile.timezone="UTC") -> Materialize
```

### 4. No Useful Index
```text
CollectionScan -> Filter(role="member") -> Sort(created_at desc) -> Limit
```

## Cost Model
A simple heuristic cost model is sufficient for v1.

Inputs:
- index exists?
- unique vs non-unique?
- estimated cardinality
- requires full scan?
- requires explicit sort?
- record materialization count

Heuristics:
- PK lookup cheapest
- unique lookup next
- selective equality index next
- collection scan expensive
- explicit sort on large set expensive

Later, collect stats:
- index entry count
- distinct values
- histogram-ish range metadata
- average list/object sizes

## Predicate Pushdown
Plan should push predicates into indexed lookups where possible.

Examples:
- `Eq(role, "member")` can use role index
- `Eq(profile.timezone, "UTC")` can use nested path index
- if both indexed, planner may choose more selective one and post-filter the other

## Sort Planning
If order matches an available ordered index, avoid explicit sort.

Otherwise:
- fetch candidate record IDs
- materialize values
- sort in memory
- apply limit

In v1, keep sort support modest:
- one field sort
- scalar indexed fields preferred
- stable ordering via PK tie-breaker

## Execution Model
Execution works against a consistent snapshot.
Steps:
1. open snapshot ID
2. walk physical plan
3. obtain candidate PKs or record pointers
4. fetch latest visible versions
5. apply residual filters
6. sort/limit if needed
7. decode into raw or typed output

## Materialization Strategy
Two possible approaches:
1. decode full records early
2. carry lightweight handles/PKs until necessary

Preferred:
- delay full decode until needed
- especially for indexed lookup + limit paths

## Example API Shapes

### Python
```python
db.users.where(
    User.role == "member",
    User.profile.timezone == "UTC"
).order_by(User.created_at.desc()).limit(20).all()
```

### Rust
```rust
let users = db
    .collection::<User>()
    .where_(User::role().eq(Role::Member))
    .and(User::profile().timezone().eq("UTC"))
    .order_by_desc(User::created_at())
    .limit(20)
    .all()?;
```

## Explain Plan
A valuable tool even in v1.

Example:
```text
Plan:
  IndexLookup index=users_role key="member"
  ResidualFilter profile.timezone == "UTC"
  Sort created_at DESC
  Limit 20
Estimated rows after lookup: 1200
```

## Index Selection Rules
Planner should consider:
- PK index
- unique index
- field index
- nested field index
- possibly compound index later

If multiple candidate indexes apply:
- prefer unique
- else prefer lower estimated cardinality
- else prefer index satisfying sort
- else prefer simpler path

## Nested Path Querying
Because nested objects are first-class, nested path querying is critical.

Requirements:
- schema-resolved path validation
- indexability of scalar nested paths
- direct filtering in API
- good error if path lands on non-scalar where scalar comparison required

Example error:
> Cannot compare `profile` to scalar string; path `profile` resolves to object type.

## Collection Scan Behavior
When scan required:
- iterate visible record IDs
- skip tombstones
- lazily decode only needed fields if possible
- apply filters
- collect rows until limit or end

## Pagination
Offset pagination is acceptable for v1, but cursor pagination should be considered soon after.

### Cursor candidate
- sort field value + PK tie-breaker

## Result Shapes
Queries may return:
- typed models
- dict/raw object
- iterators / lazy streams
- scalar count later

For v1, `.all()`, `.first()`, `.get(pk)` are sufficient.

## Counting
Optional v1 feature:
- `count()` with fast path if using equality index
- otherwise scan

## Transaction Interaction
Reads within a transaction should see that transaction’s snapshot plus its own uncommitted writes if supported.
Simpler v1 alternative:
- read-only snapshots
- write transactions expose only committed state until commit

## Planner Phasing
### MVP
- get by PK
- equality filters
- nested path equality
- limit
- simple sort
- explain

### Next
- range filters
- compound indexes
- projection
- count optimizations
- cursor pagination

## Testing
Planner tests should include:
1. valid path resolution
2. invalid path rejection
3. index chosen when available
4. scan fallback when index absent
5. residual filter correctness
6. ordering correctness
7. explain plan stability

## Non-Goals for v1 Query Layer
- arbitrary joins
- correlated subqueries
- SQL parser
- aggregation framework
- complex boolean expression optimization

## Key Principle
The planner exists to make **typed local application queries** efficient, not to become a full relational optimizer on day one.
