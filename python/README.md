# Python workspace (`python/`)

[![CI](https://github.com/eddiethedean/typra/actions/workflows/ci.yml/badge.svg)](https://github.com/eddiethedean/typra/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/typra.svg)](https://pypi.org/project/typra/)
[![Docs](https://readthedocs.org/projects/typra/badge/?version=latest)](https://typra.readthedocs.io/en/latest/)

Contributor guide for **PyPI packaging** in the Typra monorepo. The engine is Rust under **`crates/`**; the **`typra`** wheel is a PyO3 extension on **`typra-core`**.

**End users:** install from PyPI and read **[typra.readthedocs.io](https://typra.readthedocs.io/en/latest/)** — package blurb: [python/typra/README.md](https://github.com/eddiethedean/typra/blob/main/python/typra/README.md) (also on [PyPI](https://pypi.org/project/typra/)).

| | |
|--|--|
| [Python guide](https://typra.readthedocs.io/en/latest/guides/python/) | Application API |
| [Python API reference](https://typra.readthedocs.io/en/latest/reference/python_api/) | Curated surface |
| [Contributing](https://typra.readthedocs.io/en/latest/dev/contributing_guide/) | Dev setup, CI, publish |
| [Repository root](https://github.com/eddiethedean/typra/blob/main/README.md) | Project overview |

## Layout

| Path | Role |
|------|------|
| **`typra/`** | Maturin project → **`typra`** on PyPI (`pyproject.toml`, `Cargo.toml` as **`typra-python`**, `src/`, `tests/`) |
| **`typra.pyi`** | Type stubs for editors / **`ty`** |

Listed in the Rust workspace so `cargo check -p typra-python` stays aligned with **`crates/`** releases.

## Local development

From the **repository root**:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -U pip
make check-full          # ruff, ty, cargo, pytest, doc examples, docs build
make check-1p0-ready     # check-full + async facade tests
```

Manual loop:

```bash
cd python/typra
maturin develop --release
pytest -v
```

Tests: **`python/typra/tests/`** · CI: [`.github/workflows/ci.yml`](https://github.com/eddiethedean/typra/blob/main/.github/workflows/ci.yml)

## Publishing

Tag-driven from repo root — see **[Contributing → Publishing](https://typra.readthedocs.io/en/latest/dev/contributing_guide/#publishing)** and [docs/contributing.md](https://github.com/eddiethedean/typra/blob/main/docs/contributing.md) for secrets and crate order.

## License

MIT — [LICENSE](https://github.com/eddiethedean/typra/blob/main/LICENSE)
