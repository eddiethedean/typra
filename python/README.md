# Python packages

PyPI distributions and Python-facing code live here, **separate from Rust crates** under `crates/`.

| Path | Role |
|------|------|
| **`typra/`** | The **`typra`** package on PyPI: maturin + PyO3 native extension (`import typra`). |

As of **0.4.0**, the extension exposes **`typra.Database`**: **`open`**, **`register_collection(name, fields_json)`**, and **`collection_names()`** (see [`typra/README.md`](typra/README.md)). For a full API and `fields_json` reference, see **[`docs/guide_python.md`](../docs/guide_python.md)**. Record APIs are planned for later releases.

Pure Python modules (e.g. helpers, type stubs) can be added under `typra/` as the project grows (see [maturin mixed projects](https://www.maturin.rs/project_layout.html)).

## Supported Python

- **CPython 3.9+**
- Published wheels use **PyO3 abi3** (`cp39-abi3`) so there is **one wheel per platform** for CPython 3.9+.

## Tests

From the repo root, use a virtualenv, build/install the extension in editable mode, then run **pytest** (tests live under `typra/tests/`):

```bash
uv venv .venv --python 3.12
source .venv/bin/activate
python -m ensurepip --upgrade
python -m pip install -U pip
python -m pip install maturin pytest
cd python/typra
maturin develop --release
pytest -v
```

Or just run the repo-root target:

```bash
make check-full
```
