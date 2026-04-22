# Typra User Guide: Getting Started

Typra is a typed, embedded database with a Rust-first core and optional Python bindings.

## Current status (important)

As of **v0.5.x**, Typra ships a **persisted schema catalog** plus **record insert/get** (v1 encoding), **in-memory** databases and snapshots, alongside earlier on-disk foundations:

- **Rust**: `Database::open`, **`register_collection(..., primary_field)`** / **`register_schema_version`**, **`insert` / `get`**, **`Database::open_in_memory`**, and `#[derive(DbModel)]`.
- **Python**: `typra.Database.open`, **`open_in_memory`**, **`open_snapshot_bytes`**, **`register_collection(name, fields_json, primary_field)`**, **`insert`**, **`get`**, **`snapshot_bytes`**, **`collection_names()`**, and `__version__`.
- **Not yet**: SQL / rich queries, full validation-on-write, secondary indexes—see [`ROADMAP.md`](../ROADMAP.md).

Contributor-oriented layout (Rust crates and `typra-core` modules): [`03_rust_crate_and_module_layout.md`](03_rust_crate_and_module_layout.md).

## Install (Rust)

In your application `Cargo.toml`:

```toml
[dependencies]
typra = "0.5"
```

## Minimal Rust example

This uses an **in-memory** database (no file; safe to run repeatedly). For an on-disk file, use `Database::open("my.typra")?` instead of `open_in_memory()`.

```rust
use std::borrow::Cow;

use typra::prelude::*;
use typra::schema::FieldPath;
use typra::FieldDef;
use typra::Type;

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

### Run it (from this repo)

The workspace includes the same program as **`crates/typra/examples/open.rs`**:

```bash
cargo run -q -p typra --example open
```

Output:

```text
opened: :memory:
registered collection id=1 version=1
```

## Install (Python)

```bash
pip install "typra>=0.5.0,<0.6"
```

## Minimal Python example

In-memory (repeatable; same idea as the Rust example above):

```python
import typra

db = typra.Database.open_in_memory()
cid, ver = db.register_collection(
    "books",
    '[{"path": ["title"], "type": "string"}]',
    "title",
)
print("registered collection_id=", cid, "schema_version=", ver)
db.insert("books", {"title": "Hello"})
print("get:", db.get("books", "Hello"))
print("typra", typra.__version__)
```

### Run it (from this repo)

Requires **Python 3.9+**. From the repository root, build the extension then run the snippet (bash):

```bash
make python-develop
.venv/bin/python <<'PY'
import typra

db = typra.Database.open_in_memory()
cid, ver = db.register_collection(
    "books",
    '[{"path": ["title"], "type": "string"}]',
    "title",
)
print("registered collection_id=", cid, "schema_version=", ver)
db.insert("books", {"title": "Hello"})
print("get:", db.get("books", "Hello"))
print("typra", typra.__version__)
PY
```

Output (the **`typra`** version line tracks the workspace / PyPI release):

```text
registered collection_id= 1 schema_version= 1
get: {'title': 'Hello'}
typra 0.5.1
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
- **`make verify-doc-examples`**: asserts stdout from `cargo run -p typra --example open` and the Python snippets above matches the documented output blocks on this page and in the READMEs

## Where to go next

- **Concepts**: [`guide_concepts.md`](guide_concepts.md)
- **Python** (`Database`, `fields_json`, errors): [`guide_python.md`](guide_python.md)
- **Models & collections** (naming + subset models): [`guide_models_and_collections.md`](guide_models_and_collections.md)
- **Storage modes** (disk vs in-memory vs hybrid/streaming): [`guide_storage_modes.md`](guide_storage_modes.md)

If you want deeper design specs (not a user guide), start in [`01_full_architecture_spec.md`](01_full_architecture_spec.md).

