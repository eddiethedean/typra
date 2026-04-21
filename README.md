# Typra

> **SQLite simplicity, with real types.**

Typra is a **typed, embedded database** for application data.  
It combines the ease of SQLite with **strict schemas, validation, and nested data support**—so your data is always correct by design.

**Status (v0.1.0):** First semver release. The Rust crates expose `Database::open` and a `DbModel` derive; the storage engine and higher-level Python APIs are still **under development**. See [CHANGELOG.md](CHANGELOG.md).

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

## Python (preview)

The `typra` package on PyPI exposes the native extension; **0.1.0** includes `__version__` only—higher-level APIs will land in later releases.

- **Python support**: **3.9+**
- **Wheels**: **`cp39-abi3`** (one wheel per platform for CPython 3.9+)

```python
import typra

print(typra.__version__)
```

```bash
pip install "typra>=0.1.0,<0.2"
```

---

## Rust

### Application crate (recommended)

Use the **`typra`** crate — it re-exports the engine and enables `#[derive(DbModel)]` by default.

```toml
[dependencies]
typra = "0.1"
```

Disable the default `derive` feature if you only need the engine:

```toml
typra = { version = "0.1", default-features = false }
```

### Lower-level crates

For a minimal dependency tree or out-of-tree macros, depend on **`typra-core`** and **`typra-derive`** directly (same versions as the facade).

### Example (compiles on 0.1.x)

```rust
use typra::prelude::*;
use typra::DbModel;

#[derive(DbModel)]
struct Book {
    title: String,
}

fn main() -> Result<(), DbError> {
    let _db = Database::open("example.typra")?;
    let _book = Book {
        title: "Example".into(),
    };
    Ok(())
}
```

Field attributes (`#[db(primary)]`, etc.) and enums are **not** implemented in 0.1.0; they remain design targets.

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
