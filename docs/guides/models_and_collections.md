# Models & collections

This guide explains how application models map to Typra collections, naming overrides, and **subset models** (read projections).

## Current status (1.0.x)

**Shipped today:**

- **Rust:** `Database::register_model::<T>()`, typed `db.collection::<T>()`, and subset models validated via the same catalog entry (fewer `DbModel` fields allowed).
- **Python:** `typra.models.collection(db, Model)` auto-registers on first use; re-opening validates the model against the existing catalog (primary key, field paths/types, indexes when the model declares the full schema).
- **Projections:** `ModelQuery.select([...])` / `all(fields=[...])` (Python) and `QueryBuilder::all()` on subset types (Rust) materialize only declared fields.
- **Naming:** Rust `#[db(collection = "...")]` / `DbModel::collection_name()`; Python `__typra_collection__` or default pluralized snake_case class name.

**Planned for 1.1:** projection-aware decode (skip unused columns at the record layer); `DbModel` derive support for nested paths and constraints.

See the [Python guide](python.md), [async policy](../reference/async_policy.md), and [`ROADMAP.md`](https://github.com/eddiethedean/typra/blob/main/ROADMAP.md).

## Collection identity vs name

- **Collection ID:** stable internal identity (does not change)
- **Collection name:** human-facing handle in APIs and debugging

Rename a model class without renaming stored data by keeping the same collection name override.

## Default collection names

### Rust

Default is the Rust type name (e.g. `User`). Override with `#[db(collection = "users")]` or `DbModel::collection_name()`.

### Python

Default is pluralized snake_case of the class name (e.g. `User` → `users`). Override with `__typra_collection__ = "users"`.

## Registering models and schema compatibility

| Surface | Registration |
|---------|----------------|
| Rust | `db.register_model::<Book>()` then `db.collection::<Book>()` |
| Python | `typra.models.collection(db, Book)` |

Compatibility rules:

- **Collection missing:** create with the model schema.
- **Collection exists:** model fields must be a **compatible subset** of the catalog (same primary key; each declared path/type must match). Full-schema models must also match index definitions.

Schema **version** changes use `plan_schema_version` / `register_schema_version` (and `typra.models.plan` / `apply`), not silent re-registration.

## Subset models / projections

Define a class or struct with **fewer fields** than the stored collection to reduce materialization cost at the API layer.

### Semantics

- Subset models are **read projections** (they do not alter storage).
- Every declared field path must exist in the catalog with a matching type.
- Undeclared catalog fields are omitted from materialized results.
- Inserts/updates through a subset model still validate the **subset** fields you provide; use the full model when writing complete rows.

### Rust example

See [`crates/typra/examples/subset_models.rs`](https://github.com/eddiethedean/typra/blob/main/crates/typra/examples/subset_models.rs).

### Python example

```python
@dataclass
class Book:
    __typra_primary_key__ = "id"
    id: int
    title: str
    year: int

@dataclass
class BookTitle:
    __typra_primary_key__ = "id"
    __typra_collection__ = "books"  # same collection as Book
    id: int
    title: str

books = typra.models.collection(db, BookTitle)
rows = books.where("id", 1).all()
```

### Performance note

1.0 decodes full rows internally then projects in memory. Avoiding decode for unused fields is a 1.1 optimization.

### Common use cases

- UI list views (`UserSummary`)
- Partial nested reads (declare nested paths in Python/Rust field metadata)
- Low-latency endpoints that do not need full records

## Naming + subset models together

Subset models target the **same collection name** as the full model. Compatibility checks run against the catalog entry anchored by that name.
