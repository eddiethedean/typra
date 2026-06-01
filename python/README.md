# Python workspace (`python/`)

[![CI](https://github.com/eddiethedean/modelvault/actions/workflows/ci.yml/badge.svg)](https://github.com/eddiethedean/modelvault/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/modelvault.svg)](https://pypi.org/project/modelvault/)
[![Docs](https://readthedocs.org/projects/modelvault/badge/?version=latest)](https://modelvault.readthedocs.io/en/latest/)

Contributor guide for **PyPI packaging** in the ModelVault monorepo. The engine is Rust under **`crates/`**; the **`modelvault`** wheel is a PyO3 extension on **`modelvault-core`**.

**End users:** install from PyPI — *store Pydantic models directly* — and read **[modelvault.readthedocs.io](https://modelvault.readthedocs.io/en/latest/)**. Package blurb: [python/modelvault/README.md](https://github.com/eddiethedean/modelvault/blob/main/python/modelvault/README.md) (also on [PyPI](https://pypi.org/project/modelvault/)).

| | |
|--|--|
| [Why ModelVault](https://modelvault.readthedocs.io/en/latest/guides/why_modelvault/) | Positioning |
| [Pydantic](https://modelvault.readthedocs.io/en/latest/guides/pydantic/) · [FastAPI](https://modelvault.readthedocs.io/en/latest/guides/fastapi/) | Application guides |
| [Python guide](https://modelvault.readthedocs.io/en/latest/guides/python/) | Full application API |
| [Async policy](https://modelvault.readthedocs.io/en/latest/reference/async_policy/) | `AsyncDatabase`, concurrent reads vs exclusive writes |
| [Python API reference](https://modelvault.readthedocs.io/en/latest/reference/python_api/) | Curated surface |
| [Contributing](https://modelvault.readthedocs.io/en/latest/dev/contributing_guide/) | Dev setup, CI, publish |
| [Repository root](https://github.com/eddiethedean/modelvault/blob/main/README.md) | Project overview |

## Layout

| Path | Role |
|------|------|
| **`modelvault/`** | Maturin project → **`modelvault`** on PyPI (`pyproject.toml`, `Cargo.toml` as **`modelvault-python`**, `src/`, `tests/`) |
| **`modelvault.pyi`** | Type stubs for editors / **`ty`** |

Listed in the Rust workspace so `cargo check -p modelvault-python` stays aligned with **`crates/`** releases.

## Local development

From the **repository root**:

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -U pip
make check-full          # ruff, ty, cargo, pytest, doc examples, docs build
make check-2p0-ready     # check-full + async facade tests
```

Manual loop:

```bash
cd python/modelvault
maturin develop --release
pytest -v
```

Tests: **`python/modelvault/tests/`** · CI: [`.github/workflows/ci.yml`](https://github.com/eddiethedean/modelvault/blob/main/.github/workflows/ci.yml)

## Publishing

Tag-driven from repo root — see **[Contributing → Publishing](https://modelvault.readthedocs.io/en/latest/dev/contributing_guide/#publishing)** and [docs/contributing.md](https://github.com/eddiethedean/modelvault/blob/main/docs/contributing.md) for secrets and crate order.

## License

MIT — [LICENSE](https://github.com/eddiethedean/modelvault/blob/main/LICENSE)
