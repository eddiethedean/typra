# Typra

[![CI](https://github.com/eddiethedean/typra/actions/workflows/ci.yml/badge.svg)](https://github.com/eddiethedean/typra/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/typra.svg)](https://crates.io/crates/typra)
[![PyPI](https://img.shields.io/pypi/v/typra.svg)](https://pypi.org/project/typra/)

> **SQLite simplicity, with real types.**

Typra is a **typed, embedded database** for application data.  
It combines the ease of SQLite with **strict schemas, validation, and nested data support**—so your data is modeled explicitly end to end.

## Status (v0.13.x)

| Surface | What ships today |
|---------|------------------|
| **Rust** | Persisted **catalog** (create + schema versions; **constraints** + **index definitions**), **schema compatibility checks** + migration planning helpers, **`insert` / `get` / `delete`**, **`RowValue`** + validation, **`open_in_memory`** + snapshots, **secondary indexes** + replay, **queries** (**equality**, **`And`**, **`Or`**, **range**, **`limit`**, **`order_by`**, **`explain`**), **`Database::query_iter`**, **subset row projection**, **compaction** |
| **Python** | **`Database.open`**, **`register_collection`**, **`register_schema_version`** + planning/backfill helpers, **`insert` / `get` / `delete`**, **`with db.transaction():`**, query builder (**`where` / `and_where` / `limit` / `explain` / `all`**), **`typra.dbapi`** (PEP 249, read-only minimal `SELECT`), in-memory + snapshots, **compaction**, **`collection_names()`** |
| **Format** | Catalog **v4** on new writes (constraints from **v3** + **indexes**); record payload **v1 + v2**; **index** segment batches (**0.7.0+**); **transaction markers** (**0.8.0+**); file format minor **6** (lazy upgrades from older minors) |

Typra ships an **experimental, read-only DB-API 2.0 adapter** (minimal `SELECT` subset) in **0.10.0**. Full SQL and SQLAlchemy integration remain **out of scope** for now. See **[CHANGELOG.md](CHANGELOG.md)** and **[ROADMAP.md](ROADMAP.md)**.

## Guarantees and contracts (1.0-ready docs)

- **Compatibility and recovery contract**: [`docs/compatibility_matrix.md`](docs/compatibility_matrix.md)
- **Supported features matrix** (types, constraints, indexes, queries): [`docs/types_matrix.md`](docs/types_matrix.md)
- **Operations and failure modes**: [`docs/guide_operations_and_failure_modes.md`](docs/guide_operations_and_failure_modes.md)
- **Security posture** (threat model + disclosure): [`docs/security_threat_model.md`](docs/security_threat_model.md) and [`SECURITY.md`](SECURITY.md)

| Resource | Link |
|----------|------|
| **User guides** | [Getting started](docs/guide_getting_started.md) · [Concepts](docs/guide_concepts.md) · [Python](docs/guide_python.md) · [Operations & failure modes](docs/guide_operations_and_failure_modes.md) · [Models & collections](docs/guide_models_and_collections.md) · [Storage modes](docs/guide_storage_modes.md) · [Compatibility](docs/compatibility_matrix.md) · [Types matrix](docs/types_matrix.md) · [Rust module layout](docs/03_rust_crate_and_module_layout.md) · [Record encoding v2](docs/07_record_encoding_v2.md) |
| **Migration** | [0.4.x → 0.5.x](docs/migration_0.4_to_0.5.md) · [0.5.x → 0.6.x](docs/migration_0.5_to_0.6.md) · [0.6.x → 0.7.x](docs/migration_0.6_to_0.7.md) · [0.7.x → 0.8.x](docs/migration_0.7_to_0.8.md) |
| **Contributing** | [docs/contributing.md](docs/contributing.md) |

---

## Why Typra?

Modern applications already define their data using Rust structs, Pydantic models, or TypeScript schemas—but many databases accept loosely typed rows anyway.

**Typra** targets **models as schema**, **validation on write**, **nested data as first-class**, and **single-file** deployment.

---

## Features (roadmap)

Many items below are **goals**; see the changelog for what each release actually ships.

- Type-first design  
- Validation on write  
- Nested objects and lists  
- Embedded, zero-config, single file  
- Safe schema evolution  
- Typed queries  

---

## Typra vs SQLite (vision)

| Feature | SQLite | Typra (target) |
|---------|--------|----------------|
| Typing | Weak | Strong |
| Validation | Minimal | Built-in |
| Nested data | JSON | Native |
| API | SQL | Model-first |

---

## Python

The **`typra`** package on PyPI is a native extension. **`fields_json`** is a JSON array of field descriptors—see **[`python/typra/README.md`](python/typra/README.md)** and **[`docs/guide_python.md`](docs/guide_python.md)**.

- **Python:** 3.9+  
- **Wheels:** `cp39-abi3` (one wheel per platform)

```python
# Setup: module, in-memory DB, and `books` collection (PK `title`).
import typra

db = typra.Database.open_in_memory()
_, _ = db.register_collection(
    "books",
    '[{"path": ["title"], "type": "string"}]',
    "title",
)
# Example: insert and read one row; print version.
db.insert("books", {"title": "Hello"})
print(db.get("books", "Hello"))
print(typra.__version__)
```

Output:

```text
{'title': 'Hello'}
0.13.0
```

```bash
pip install "typra>=0.13.0,<0.14"
```

---

## Rust

### Application crate (recommended)

Use the **`typra`** crate — it re-exports the engine and enables **`#[derive(DbModel)]`** by default. See **[`crates/typra/README.md`](crates/typra/README.md)**.

```toml
[dependencies]
typra = "0.13"
```

Without proc-macros (engine only):

```toml
typra = { version = "0.13", default-features = false }
```

### Lower-level crates

Depend on **`typra-core`** and **`typra-derive`** directly when you need a minimal graph or custom macro wiring (same semver as **`typra`**).

### Example

In-memory (repeatable; no leftover file). From the repo: **`cargo run -p typra --example open`**.

```rust
use std::borrow::Cow;
use typra::prelude::*;
use typra::FieldDef;
use typra::Type;
use typra::schema::FieldPath;

fn main() -> Result<(), DbError> {
    // Setup: in-memory database (no file on disk).
    let mut db = Database::open_in_memory()?;
    println!("opened: {}", db.path().display());
    // Example: register a `books` collection with a string primary key `title`.
    let (id, ver) = db.register_collection(
        "books",
        vec![FieldDef {
            path: FieldPath::new([Cow::Borrowed("title")])?,
            ty: Type::String,
            constraints: vec![],
        }],
        "title",
    )?;
    println!("registered collection id={} version={}", id.0, ver.0);
    Ok(())
}
```

Output:

```text
opened: :memory:
registered collection id=1 version=1
```

Field attributes (`#[db(primary)]`, etc.) on **`DbModel`** are **not** implemented yet.

---

## Philosophy

> **Your data should be correct by construction.**

---

## Development

| Path | Role |
|------|------|
| **`crates/`** | Rust crates (**`typra`**, **`typra-core`**, **`typra-derive`**) — see per-crate READMEs |
| **`python/`** | PyPI packaging — see **[`python/README.md`](python/README.md)** |

Full local checks (ruff, ty, cargo fmt/clippy/test, pytest, **documented example output verification**):

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -U pip
make check-full
```

Design specs live under **[`docs/`](docs/)**.

## License

MIT — see **[LICENSE](LICENSE)**.
