# Typra

[![CI](https://github.com/eddiethedean/typra/actions/workflows/ci.yml/badge.svg)](https://github.com/eddiethedean/typra/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/typra.svg)](https://crates.io/crates/typra)
[![PyPI](https://img.shields.io/pypi/v/typra.svg)](https://pypi.org/project/typra/)

> **SQLite simplicity, with real types.**

Typra is a **typed, embedded database** for application data.  
It combines the ease of SQLite with **strict schemas, validation, and nested data support**—so your data is modeled explicitly end to end.

## Status (v0.5.x)

| Surface | What ships today |
|---------|------------------|
| **Rust** | `Database::open`, **`register_collection` / `register_schema_version`** (with **`primary_field`**), **`insert` / `get`**, **`open_in_memory`**, snapshot helpers, **`#[derive(DbModel)]`** |
| **Python** | **`register_collection(..., primary_field)`**, **`insert`**, **`get`**, in-memory / snapshot APIs, **`collection_names()`** |
| **Format** | New databases use file format minor **5** (lazy **4 → 5** on first record write; **3 → 4** on first catalog write as in 0.4.x) |

Rich validation, SQL-style queries, and secondary indexes are **under development**. See **[CHANGELOG.md](CHANGELOG.md)** and **[ROADMAP.md](ROADMAP.md)**.

| Resource | Link |
|----------|------|
| **User guides** | [Getting started](docs/guide_getting_started.md) · [Concepts](docs/guide_concepts.md) · [Python](docs/guide_python.md) · [Models & collections](docs/guide_models_and_collections.md) · [Storage modes](docs/guide_storage_modes.md) |
| **Migration** | [0.4.x → 0.5.x](docs/migration_0.4_to_0.5.md) |
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
0.5.1
```

```bash
pip install "typra>=0.5.0,<0.6"
```

---

## Rust

### Application crate (recommended)

Use the **`typra`** crate — it re-exports the engine and enables **`#[derive(DbModel)]`** by default. See **[`crates/typra/README.md`](crates/typra/README.md)**.

```toml
[dependencies]
typra = "0.5"
```

Without proc-macros (engine only):

```toml
typra = { version = "0.5", default-features = false }
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
