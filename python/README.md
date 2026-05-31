# Python workspace (`python/`)

[![CI](https://github.com/eddiethedean/typra/actions/workflows/ci.yml/badge.svg)](https://github.com/eddiethedean/typra/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/typra.svg)](https://pypi.org/project/typra/)
[![Docs](https://readthedocs.org/projects/typra/badge/?version=latest)](https://typra.readthedocs.io/en/latest/?badge=latest)

Contributor guide for **PyPI packaging and Python tooling** in the Typra monorepo. The embedded engine lives in Rust under **`crates/`**; the **`typra`** wheel is a **native extension** (PyO3) built on **`typra-core`**.

**End users** installing from PyPI should start with the package README: **[python/typra/README.md](https://github.com/eddiethedean/typra/blob/main/python/typra/README.md)** (also shown on [PyPI](https://pypi.org/project/typra/)).

| Resource | Link |
|----------|------|
| **Repository root** | [README.md](https://github.com/eddiethedean/typra/blob/main/README.md) |
| **Package README (PyPI)** | [python/typra/README.md](https://github.com/eddiethedean/typra/blob/main/python/typra/README.md) |
| **Python guide** | [docs/guides/python.md](https://github.com/eddiethedean/typra/blob/main/docs/guides/python.md) |
| **Quickstart** | [docs/guides/quickstart.md](https://github.com/eddiethedean/typra/blob/main/docs/guides/quickstart.md) |
| **Python API reference** | [docs/reference/python_api.md](https://github.com/eddiethedean/typra/blob/main/docs/reference/python_api.md) |
| **Changelog** | [CHANGELOG.md](https://github.com/eddiethedean/typra/blob/main/CHANGELOG.md) |
| **Roadmap** | [ROADMAP.md](https://github.com/eddiethedean/typra/blob/main/ROADMAP.md) |
| **Contributing / publish** | [docs/contributing.md](https://github.com/eddiethedean/typra/blob/main/docs/contributing.md) |

## Layout

| Path | Role |
|------|------|
| **`typra/`** | Maturin project: **`pyproject.toml`**, **`Cargo.toml`** (Rust package **`typra-python`**), **`src/`** (PyO3), **`tests/`** (pytest). Produces the **`typra`** distribution on PyPI. |
| **`typra.pyi`** | Type stubs for editors / **`ty`**; kept beside the package. |

The Rust workspace lists **`python/typra`** as a member so **`cargo check -p typra-python`** and release versioning stay aligned with **`crates/`**.

## What the extension exposes (v1.0.x)

**Primary API:** **`typra.models`** (dataclass / Pydantic v2). Lower-level **`fields_json`** remains supported.

| Module / type | Surface |
|---------------|---------|
| **`typra.Database`** | `open`, `open_in_memory`, snapshots, `register_collection`, schema migrations, `insert` / `get` / `delete`, `transaction`, `collection`, compaction |
| **`typra.models`** | Class-defined schemas, `collection`, `plan` / `apply`, constraints, indexes |
| **`typra.dbapi`** | Read-only PEP 249 adapter (minimal `SELECT` subset) |
| **`Typra*Error`** | Structured exception mapping (`TypraFormatError`, `TypraSchemaError`, …) |
| **`typra.__version__`** | Matches workspace / crates release (**1.0.0**) |

Install for local development:

```bash
cd python/typra
maturin develop --release
```

## Setup and tests

From the **repository root**:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -U pip
make check-full
```

That runs **ruff**, **ty**, **cargo** fmt/clippy/test, **`maturin develop --release`** + **pytest**, and **`scripts/verify-doc-examples.sh`** (README / guide output verification).

**1.0 readiness gate:**

```bash
make check-1p0-ready
```

Manual loop (minimal):

```bash
python3 -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
python -m pip install -U pip "maturin>=1.5,<2" "ruff>=0.8" "ty>=0.0.28" pytest
cd python/typra
maturin develop --release
pytest -v
```

Tests: **`python/typra/tests/`**. CI: [`.github/workflows/ci.yml`](https://github.com/eddiethedean/typra/blob/main/.github/workflows/ci.yml) (Linux, macOS, Windows).

## Publishing

Releases are tag-driven from the repo root. See [docs/contributing.md](https://github.com/eddiethedean/typra/blob/main/docs/contributing.md) for **`scripts/publish-all.sh`**, crates.io order, and GitHub Actions secrets.

## Adding pure Python later

Follow [maturin mixed / hybrid layouts](https://www.maturin.rs/project_layout.html) and update **`pyproject.toml`** accordingly.

## License

MIT — see [LICENSE](https://github.com/eddiethedean/typra/blob/main/LICENSE).
