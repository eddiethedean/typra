# Typra User Guide: Models & Collections

This guide explains how application models map to collections, how collection naming should work, and how **subset models / projections** should reduce friction when working with large schemas.

## Current status (important)

Today, Typra’s implemented surface area is still early (open/create database file, derive marker trait). This guide describes **intended** user-facing behavior as it lands over upcoming milestones in [`ROADMAP.md`](/Users/odosmatthews/Documents/coding/typra/ROADMAP.md).

## Collection identity vs name

Typra should treat:

- **Collection ID**: the stable internal identity (does not change)
- **Collection name**: a human-facing handle used in APIs and debugging

This mirrors the idea that you should be able to **rename** a model class without accidentally renaming the underlying stored collection.

## Default collection names

### Rust

Default should be the Rust type name (e.g. `User`), but with an override.

Planned direction is consistent with the design spec’s `DbModel` trait shape:

```text
DbModel::collection_name() -> &'static str
```

### Python

Default should be the class `__name__` (e.g. `User`), with an override (e.g. a `__collection__` attribute or config field).

## Overriding collection names

Typra should support explicit naming to avoid accidental renames:

- **Rust**: `#[db(collection = \"users\")]` (exact attribute spelling TBD)
- **Python**: `__collection__ = \"users\"` (exact mechanism TBD)

## Registering models and schema compatibility

Conceptually, the database will have a schema registry/catalog:

- `db.register(User)` (Python-style)
- `db.register_collection::<User>()` or similar (Rust-style)

Compatibility rules should be explicit:

- If a collection name does not exist yet: create it with that schema.
- If it exists: the schema must be compatible (or you must provide a migration path).
In early versions, the engine should prefer **strict equality** to avoid surprising behavior.

## Subset models / projections

Large collections can become cumbersome to use if every interaction requires a huge model with deeply nested fields. Typra should support **subset models** (projections/views) so you can interact with only the fields you care about.

### What a subset model is

If the underlying collection has many fields, you can define a model with fewer fields:

- only 5 of 20 top-level fields
- only a subset of nested object fields (partial nested projection)

When you query into that subset model, results materialize into the subset shape.

### Semantics (v1 target)

- Subset models are **read projections** (they do not alter storage).
- A subset model must be **compatible** with the collection schema:
  - every declared field path exists in the collection schema
  - types match (or are safely coercible under strictness policy)
- Undeclared fields are simply not materialized.

### Performance expectation

Where the encoding allows, queries should avoid decoding fields that are not requested by the projection (projection-aware materialization).

### API direction

**Rust-first** conceptual options:

- `db.collection::<FullUser>()` vs `db.collection::<UserSummary>()`
- or `db.collection::<FullUser>().project::<UserSummary>()`

Python should allow defining a class with fewer fields than the collection, then querying to return that type.

### Common use cases

- UI list views (e.g. `UserSummary { id, display_name, last_seen_at }`)
- partial nested reads (e.g. `profile.timezone` without loading all of `profile`)
- low-latency endpoints that don’t need the full record

## Naming + subset models together

Subset models should be able to target the **same collection name** as the full model, because they represent a different **materialization**, not a different stored dataset.

To avoid ambiguity:

- the collection identity is anchored by the catalog entry
- subset models must pass compatibility checks against that catalog schema

