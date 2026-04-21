# Typra User Guide: Getting Started

Typra is a typed, embedded database with a Rust-first core and optional Python bindings.

## Current status (important)

As of **v0.4.x**, Typra ships a **persisted schema catalog** (append-only schema segments) alongside the earlier on-disk foundations:

- **Rust**: `Database::open`, **`register_collection`** / **`register_schema_version`**, and `#[derive(DbModel)]`.
- **Python**: `typra.Database.open`, **`register_collection(name, fields_json)`**, **`collection_names()`**, and `__version__`.
- **Not yet**: record insert/get, queries, validation-on-write, and indexes—see [`ROADMAP.md`](/Users/odosmatthews/Documents/coding/typra/ROADMAP.md).

## Install (Rust)

In your application `Cargo.toml`:

```toml
[dependencies]
typra = "0.4"
```

## Minimal Rust example

This opens (or creates) a database file and **registers a collection** in the persisted catalog. Record insert/get is not available yet.

```rust
use std::borrow::Cow;

use typra::prelude::*;
use typra::schema::FieldPath;
use typra::DbModel;
use typra::FieldDef;
use typra::Type;

#[derive(DbModel)]
struct Book {
    title: String,
}

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

### Run it (from this repo)

Typra also includes a runnable example program in the workspace:

```bash
cargo run -q -p typra --example open
```

Output (matches `cargo run -p typra --example open`):

```text
opened: example.typra
registered collection id=1 version=1
```

## Install (Python)

```bash
pip install "typra>=0.4.0,<0.5"
```

## Minimal Python example

```python
import typra

db = typra.Database.open("example.typra")
db.register_collection("books", '[{"path": ["title"], "type": "string"}]')
print(typra.__version__)
```

### Run it (from this repo)

Because the extension targets **Python 3.9+**, you need a Python 3.9+ interpreter when building locally.

```bash
PYENV_VERSION=3.12.11 pyenv exec python -m venv .venv_py
. .venv_py/bin/activate
python -m pip install -U pip
python -m pip install -U "maturin>=1.5,<2"
cd python/typra
maturin develop --release
python -c "import typra; print(typra.__version__)"
```

Output:

```text
0.4.0
```

## Development quickstart (repo contributors)

From the repo root:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -U pip
make check-full
```

This runs:
- Rust format/clippy/tests
- Python ruff/ty checks
- Python tests (via `maturin develop --release` + `pytest`)

## Where to go next

- **Concepts**: [`docs/guide_concepts.md`](/Users/odosmatthews/Documents/coding/typra/docs/guide_concepts.md)
- **Models & collections** (naming + subset models): [`docs/guide_models_and_collections.md`](/Users/odosmatthews/Documents/coding/typra/docs/guide_models_and_collections.md)
- **Storage modes** (disk vs in-memory vs hybrid/streaming): [`docs/guide_storage_modes.md`](/Users/odosmatthews/Documents/coding/typra/docs/guide_storage_modes.md)

If you want deeper design specs (not a user guide), start in [`docs/01_full_architecture_spec.md`](/Users/odosmatthews/Documents/coding/typra/docs/01_full_architecture_spec.md).

