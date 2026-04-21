# Python packages

PyPI distributions and Python-facing code live here, **separate from Rust crates** under `crates/`.

| Path | Role |
|------|------|
| **`typra/`** | The **`typra`** package on PyPI: maturin + PyO3 native extension (`import typra`). |

Pure Python modules (e.g. helpers, type stubs) can be added under `typra/` as the project grows (see [maturin mixed projects](https://www.maturin.rs/project_layout.html)).

## Supported Python

- **CPython 3.9+**
- Published wheels use **PyO3 abi3** (`cp39-abi3`) so there is **one wheel per platform** for CPython 3.9+.

## Tests

From the repo root, use a virtualenv, build/install the extension in editable mode, then run **pytest** (tests live under `typra/tests/`):

```bash
python -m venv .venv
source .venv/bin/activate
pip install maturin pytest
cd python/typra
maturin develop --release
pytest -v
```

Or just run the repo-root target:

```bash
make check-full
```
