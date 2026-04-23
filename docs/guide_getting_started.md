# Typra User Guide: Getting Started

Typra is a typed, embedded database with a Rust-first core and optional Python bindings.

## Current status (important)

As of **v0.8.x+**, Typra ships a **persisted schema catalog** (per-field **constraints** on catalog **v3**; **index definitions** on catalog **v4**), **secondary indexes** and minimal **queries** (equality, `limit`, `explain`), **record insert/get** with **nested row values** (new writes use **record payload v2**; **v1** segments still replay), **engine validation** before append, **transactions** (`with db.transaction()` in Python / `Database::transaction` in Rust), **in-memory** databases and snapshots, alongside earlier on-disk foundations:

- **Rust**: `Database::open`, **`open_with_options`**, **`register_collection(..., primary_field)`** / **`register_schema_version`**, **`insert` / `get`** with **`RowValue`**, **`Database::open_in_memory`**, `Database::transaction`, typed **queries**, **secondary indexes**, and `#[derive(DbModel)]` (see [`ROADMAP.md`](../ROADMAP.md)).
- **Python**: `typra.Database.open`, **`open_in_memory`**, **`open_snapshot_bytes`**, **`register_collection(..., indexes_json=...)`** (optional **`constraints`** in `fields_json`), **`insert`**, **`get`**, **`db.collection(name).where(...).all()`** (plus **`and_where`**, **`limit`**, **`explain`**, subset **`all(fields=[...])`**), **`snapshot_bytes`**, **`collection_names()`**, and `__version__`. A **disk + indexes** walkthrough is in **[`guide_python.md` — Realistic workflow](guide_python.md#realistic-workflow-indexed-queries-on-disk)**.
- **Not yet**: arbitrary **SQL** text, **DB-API / SQLAlchemy**—see [`ROADMAP.md`](../ROADMAP.md).

Contributor-oriented layout (Rust crates and `typra-core` modules): [`03_rust_crate_and_module_layout.md`](03_rust_crate_and_module_layout.md).

## Install (Rust)

In your application `Cargo.toml`:

```toml
[dependencies]
typra = "0.8"
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
pip install "typra>=0.8.0,<0.9"
```

## Minimal Python example

In-memory (repeatable; same idea as the Rust example above):

```python
# Setup: module and in-memory database.
import typra

db = typra.Database.open_in_memory()
cid, ver = db.register_collection(
    "books",
    '[{"path": ["title"], "type": "string"}]',
    "title",
)
# Example: insert one row, read it back, print package version.
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
# Setup: module and in-memory database.
import typra

db = typra.Database.open_in_memory()
cid, ver = db.register_collection(
    "books",
    '[{"path": ["title"], "type": "string"}]',
    "title",
)
# Example: insert one row, read it back, print package version.
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
typra 0.8.0
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
- **`make verify-doc-examples`**: asserts stdout from `cargo run -p typra --example open` and the embedded Python snippets match each documented **text** output block on this page, the root **`README.md`**, **`docs/guide_python.md`** (quick start, query, workflow, fields example), and **`python/typra/README.md`** (quick start, indexed sketch, **`fields_json`** examples). See **`scripts/verify-doc-examples.sh`** for the exact snippets.

## Where to go next

- **Concepts**: [`guide_concepts.md`](guide_concepts.md)
- **Python** (`Database`, `fields_json`, errors): [`guide_python.md`](guide_python.md)
- **Models & collections** (naming + subset models): [`guide_models_and_collections.md`](guide_models_and_collections.md)
- **Storage modes** (disk vs in-memory vs hybrid/streaming): [`guide_storage_modes.md`](guide_storage_modes.md)

If you want deeper design specs (not a user guide), start in [`01_full_architecture_spec.md`](01_full_architecture_spec.md).

