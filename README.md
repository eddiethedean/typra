# Typra

[![CI](https://github.com/eddiethedean/typra/actions/workflows/ci.yml/badge.svg)](https://github.com/eddiethedean/typra/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/typra.svg)](https://crates.io/crates/typra)
[![PyPI](https://img.shields.io/pypi/v/typra.svg)](https://pypi.org/project/typra/)

> **SQLite simplicity, with real types.**

Typra is a **typed, embedded database** for application data.  
It combines the ease of SQLite with **strict schemas, validation, and nested data support**—so your data is always correct by design.

**Status (v0.4.0):** Early semver releases. The Rust crates expose `Database::open`, **`register_collection` / `register_schema_version`** (persisted schema catalog in `SegmentType::Schema` payloads), and a `DbModel` derive. Python exposes `Database.open`, **`register_collection`**, and **`collection_names`**. Record storage and validation are still **under development**. The on-disk format is **v0.4** (minor bump) with lazy upgrade from **v0.3** on first catalog write. See [CHANGELOG.md](CHANGELOG.md).

---

## Why Typra?

Modern applications already define their data using:

- Rust structs
- Pydantic models
- TypeScript schemas

But most databases ignore that structure and accept loosely typed data.

**Typra** is meant to fix that: models as schema, validation on write, nested data as first-class, single-file deployment.

---

## Features (roadmap)

Many items below are **goals**; check the changelog for what each release actually ships.

- Type-first design
- Validation on write
- Nested objects and lists
- Embedded, zero-config, single file
- Safe schema evolution
- Typed queries

---

## Typra vs SQLite (vision)

| Feature           | SQLite | Typra (target) |
|-------------------|--------|----------------|
| Typing            | Weak   | Strong         |
| Validation        | Minimal| Built-in       |
| Nested data       | JSON   | Native         |
| API               | SQL    | Model-first    |

---

## Python

The `typra` package on PyPI exposes the native extension. **0.4.0** includes `Database.open`, `register_collection(name, fields_json)`, and `collection_names()`. **`fields_json`** is a JSON array of field descriptors (see [`python/typra/README.md`](python/typra/README.md)).

- **Python support**: **3.9+**
- **Wheels**: **`cp39-abi3`** (one wheel per platform for CPython 3.9+)

```python
import typra

db = typra.Database.open("app.typra")
db.register_collection("books", '[{"path": ["title"], "type": "string"}]')
print(typra.__version__)
```

```bash
pip install "typra>=0.4.0,<0.5"
```

---

## Rust

### Application crate (recommended)

Use the **`typra`** crate — it re-exports the engine and enables `#[derive(DbModel)]` by default.

```toml
[dependencies]
typra = "0.4"
```

Disable the default `derive` feature if you only need the engine:

```toml
typra = { version = "0.4", default-features = false }
```

### Lower-level crates

For a minimal dependency tree or out-of-tree macros, depend on **`typra-core`** and **`typra-derive`** directly (same versions as the facade).

### Example (0.4.x)

```rust
use std::borrow::Cow;
use typra::prelude::*;
use typra::FieldDef;
use typra::Type;
use typra::schema::FieldPath;

fn main() -> Result<(), DbError> {
    let mut db = Database::open("example.typra")?;
    let _ = db.register_collection(
        "books",
        vec![FieldDef {
            path: FieldPath::new([Cow::Borrowed("title")])?,
            ty: Type::String,
        }],
    )?;
    Ok(())
}
```

Field attributes (`#[db(primary)]`, etc.) on `DbModel` are **not** implemented yet; they remain design targets.

---

## Philosophy

> **Your data should be correct by construction.**

---

## Development

Rust crates live under `crates/` (`typra` facade, `typra-core`, `typra-derive`); PyPI packages under `python/` ([`python/README.md`](python/README.md)). See [docs/contributing.md](docs/contributing.md) for layout, build commands, and publishing.

From the repo root, you can run the full local CI suite:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -U pip
make check-full
```

Design specs live under [docs/](docs/).

## License

MIT — see [LICENSE](LICENSE).
