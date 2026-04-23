# Typra

[![CI](https://github.com/eddiethedean/typra/actions/workflows/ci.yml/badge.svg)](https://github.com/eddiethedean/typra/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/typra.svg)](https://crates.io/crates/typra)
[![PyPI](https://img.shields.io/pypi/v/typra.svg)](https://pypi.org/project/typra/)

> **SQLite simplicity, with real types.**

Typra is a **typed, embedded database** for application data.  
It combines the ease of SQLite with **strict schemas, validation, and nested data support**—so your data is modeled explicitly end to end.

## Status (v0.7.x)

| Surface | What ships today |
|---------|------------------|
| **Rust** | Persisted **catalog** (create + schema versions; **constraints** + **index definitions**), **`insert` / `get`**, **`RowValue`** + validation, **`open_in_memory`** + snapshots, **secondary indexes** + **`SegmentType::Index`** replay, minimal **queries** (**equality**, **`And`**, **`limit`**, **`explain`**), **`Database::query_iter`**, **subset row projection**, **`#[derive(DbModel)]`** |
| **Python** | **`Database.open`**, **`register_collection`** (optional **`constraints`** / **`indexes_json`**), **`insert` / `get`**, **`db.collection(...).where` / `and_where` / `limit` / `explain` / `all`** and **`all(fields=[...])`**, in-memory + snapshot helpers, **`collection_names()`** |
| **Format** | Catalog **v4** on new writes (constraints from **v3** + **indexes**); record payload **v1 + v2**; **index** segment batches (**0.7.0+**); file format minor **5** (lazy upgrades from older minors) |

**SQL** text and **DB-API** layers remain **out of scope** for now. See **[CHANGELOG.md](CHANGELOG.md)** and **[ROADMAP.md](ROADMAP.md)**.

| Resource | Link |
|----------|------|
| **User guides** | [Getting started](docs/guide_getting_started.md) · [Concepts](docs/guide_concepts.md) · [Python](docs/guide_python.md) · [Models & collections](docs/guide_models_and_collections.md) · [Storage modes](docs/guide_storage_modes.md) · [Rust module layout](docs/03_rust_crate_and_module_layout.md) · [Record encoding v2](docs/07_record_encoding_v2.md) |
| **Migration** | [0.4.x → 0.5.x](docs/migration_0.4_to_0.5.md) · [0.5.x → 0.6.x](docs/migration_0.5_to_0.6.md) |
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
import typra

db = typra.Database.open_in_memory()
_, _ = db.register_collection(
    "books",
    '[{"path": ["title"], "type": "string"}]',
    "title",
)
db.insert("books", {"title": "Hello"})
print(db.get("books", "Hello"))
print(typra.__version__)
```

Output:

```text
{'title': 'Hello'}
0.7.0
```

```bash
pip install "typra>=0.7.0,<0.8"
```

---

## Rust

### Application crate (recommended)

Use the **`typra`** crate — it re-exports the engine and enables **`#[derive(DbModel)]`** by default. See **[`crates/typra/README.md`](crates/typra/README.md)**.

```toml
[dependencies]
typra = "0.7"
```

Without proc-macros (engine only):

```toml
typra = { version = "0.7", default-features = false }
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
    let mut db = Database::open_in_memory()?;
    println!("opened: {}", db.path().display());
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
