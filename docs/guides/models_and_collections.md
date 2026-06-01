# Models & collections

**Store application models directly** — how dataclasses, Pydantic classes, and Rust structs map to ModelVault collections: naming, registration, compatibility, and **subset models** (read projections).

For a first model, see [Pydantic](pydantic.md) or [Quickstart](quickstart.md). For a full app shape, see [Examples](../examples/index.md).

## What ships in 1.0

| Surface | Capability |
|---------|------------|
| **Rust** | `register_model::<T>()`, `collection::<T>()`, subset models as compatible catalog subsets |
| **Python** | `modelvault.models.collection(db, Model)` — auto-register on first use; reopen validates against catalog |
| **Projections** | `ModelQuery.select([...])` / `all(fields=[...])` (Python); subset `QueryBuilder::all()` (Rust) |
| **Naming** | Rust `#[db(collection = "...")]`; Python `__modelvault_collection__` or pluralized snake_case |

!!! note "Coming in 1.1"
    Projection-aware decode (skip unused columns at record layer); `DbModel` derive for nested paths and constraints. See [roadmap](https://github.com/eddiethedean/modelvault/blob/main/ROADMAP.md).

Related: [Python guide](python.md) · [Async policy](../reference/async_policy.md)

## Collection identity vs name

| Concept | Behavior |
|---------|----------|
| **Collection ID** | Stable internal identity — never changes |
| **Collection name** | Human-facing handle in APIs and CLI |

Rename a Python class or Rust struct without touching stored data by keeping the same collection name override.

## Default collection names

=== "Python"

    Pluralized snake_case of the class name:

    | Class | Default collection |
    |-------|-------------------|
    | `User` | `users` |
    | `OrderLine` | `order_lines` |

    Override: `__modelvault_collection__ = "users"`

=== "Rust"

    Default is the Rust type name (e.g. `User`).

    Override: `#[db(collection = "users")]` or `DbModel::collection_name()`

## Registering models

| Language | Pattern |
|----------|---------|
| **Rust** | `db.register_model::<Book>()` then `db.collection::<Book>()` |
| **Python** | `modelvault.models.collection(db, Book)` |

### Compatibility on reopen

- **Collection missing** → create with model schema
- **Collection exists** → model fields must be a **compatible subset** of the catalog (same PK; each path/type must match). Full-schema models must match index definitions too

Schema **version** changes use `plan_schema_version` / `register_schema_version` (Python: `modelvault.models.plan` / `apply`) — not silent re-registration.

## Subset models

Define a type with **fewer fields** than the stored collection to reduce materialization at the API layer.

### Semantics

- **Read projections only** — do not alter storage
- Every declared path must exist in the catalog with matching type
- Undeclared catalog fields are omitted from results
- Writes through a subset model validate only the fields you provide — use the full model for complete rows

### Python example

```python
@dataclass
class Book:
    __modelvault_primary_key__ = "id"
    id: int
    title: str
    year: int


@dataclass
class BookTitle:
    __modelvault_primary_key__ = "id"
    __modelvault_collection__ = "books"  # same collection as Book
    id: int
    title: str


books = modelvault.models.collection(db, BookTitle)
rows = books.where("id", 1).all()
```

### Rust example

See [`subset_models.rs` on GitHub](https://github.com/eddiethedean/modelvault/blob/main/crates/modelvault/examples/subset_models.rs).

### Performance (1.0)

Full rows are decoded internally, then projected in memory. Skipping decode for unused fields is a **1.1** optimization.

### Common use cases

- UI list views (`UserSummary`)
- Partial nested reads
- Low-latency endpoints that do not need full records

## Naming + subsets together

Subset models target the **same collection name** as the full model. Compatibility checks run against the catalog entry for that name.
