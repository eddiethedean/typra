# Python workspace (`python/`)

[![CI](https://github.com/eddiethedean/typra/actions/workflows/ci.yml/badge.svg)](https://github.com/eddiethedean/typra/actions/workflows/ci.yml)

This directory holds **PyPI packaging and Python tooling** for Typra. The embedded engine lives in Rust under **`crates/`**; the **`typra`** wheel is a **native extension** (PyO3) that calls into **`typra-core`**.

Use this doc when you work **in or under `python/`**. End users installing from PyPI should start with the package README: **[`typra/README.md`](typra/README.md)** (also shown on [PyPI](https://pypi.org/project/typra/)).

| Resource | Link |
|----------|------|
| **Full Python guide** | [`docs/guides/python.md`](../docs/guides/python.md) |
| **Getting started** | [`docs/guides/quickstart.md`](../docs/guides/quickstart.md) |
| **Rust module layout** | [`docs/specs/rust_crate_layout.md`](../docs/specs/rust_crate_layout.md) |
| **Changelog** | [`CHANGELOG.md`](../CHANGELOG.md) |
| **Roadmap** | [`ROADMAP.md`](../ROADMAP.md) |
| **Contributing / publish** | [`docs/contributing.md`](../docs/contributing.md) |

## Layout

| Path | Role |
|------|------|
| **`typra/`** | Maturin project: **`pyproject.toml`**, **`Cargo.toml`** (Rust package name **`typra-python`**), **`src/`** (PyO3 module), **`tests/`** (pytest). Produces the **`typra`** distribution on PyPI. |
| **`typra.pyi`** | Inline type stubs for editors / **`ty`**; kept beside the package for discoverability. |

The Rust workspace lists **`python/typra`** as a member so **`cargo check -p typra-python`** and release versioning stay aligned with **`crates/`**.

## What the extension exposes (v1.0.x)

Primary API: **`typra.models`** (dataclass/Pydantic-style schemas). Lower-level **`fields_json`** remains supported.

- **`typra.Database`**: `open`, `open_in_memory`, snapshot import/export, `register_collection`, `register_schema_version`, `plan_schema_version`, `backfill_top_level_field`, `insert`, `get`, `delete`, `transaction`, `collection`, `collection_names`, `compact` / `compact_to`, `rebuild_indexes`
- **`typra.models`**: class-defined schemas, `collection`, migration helpers
- **`typra.dbapi`**: read-only PEP 249 adapter (minimal `SELECT` subset)
- **`Typra*Error`** subclasses for structured error mapping
- **`typra.__version__`**: matches the workspace / crates release (currently **1.0.0**)

Full API reference: **[`docs/reference/python_api.md`](../docs/reference/python_api.md)** and **[`python/typra/README.md`](typra/README.md)**.

## Setup and tests

From the **repository root**, the usual loop is:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -U pip
make check-full
```

That installs dev tools into `.venv`, runs **ruff**, **ty**, **cargo** fmt/clippy/test, **`maturin develop --release`** + **pytest** under `python/typra`, then **`scripts/verify-doc-examples.sh`** (asserts README / guide command output matches the minimal snippets).

Manual equivalent (minimal):

```bash
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
python -m pip install -U pip "maturin>=1.5,<2" "ruff>=0.8" "ty>=0.0.28" pytest
cd python/typra
maturin develop --release
pytest -v
```

Tests live in **`python/typra/tests/`**. CI runs the same checks via **[`.github/workflows/ci.yml`](../.github/workflows/ci.yml)** (Linux, macOS, Windows).

## Publishing

PyPI and crates.io releases are driven from the repo root (tags, **`scripts/publish-all.sh`**). See **[`docs/contributing.md`](../docs/contributing.md)** for tokens, **`cargo publish`** order, and GitHub Actions.

## Adding pure Python later

If you add `.py` helpers next to the extension, follow [maturin mixed / hybrid layouts](https://www.maturin.rs/project_layout.html) and update **`pyproject.toml`** accordingly.
