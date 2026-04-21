# Typra User Guide: Getting Started

Typra is a typed, embedded database with a Rust-first core and optional Python bindings.

## Current status (important)

As of **v0.3.x**, Typra is still shipping **foundational pieces**:

- **Rust**: `Database::open(path)` and `#[derive(DbModel)]` exist.
- **Python**: the `typra` module currently exposes `__version__` only.
- Storage, queries, validation, schema evolution, and rich APIs are under active development.

For the evolving plan, see [`ROADMAP.md`](/Users/odosmatthews/Documents/coding/typra/ROADMAP.md).

## Install (Rust)

In your application `Cargo.toml`:

```toml
[dependencies]
typra = "0.3"
```

## Minimal Rust example

This compiles today, but does not yet write records (it only creates/opens a Typra file and validates the header).

```rust
use typra::prelude::*;
use typra::DbModel;

#[derive(DbModel)]
struct Book {
    title: String,
}

fn main() -> Result<(), DbError> {
    let _db = Database::open("example.typra")?;
    Ok(())
}
```

### Run it (from this repo)

Typra also includes a runnable example program in the workspace:

```bash
cargo run -q -p typra --example open
```

Output:

```text
opened: example.typra
```

## Install (Python)

```bash
pip install "typra>=0.3.0,<0.4"
```

## Minimal Python example

```python
import typra

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
0.3.0
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

