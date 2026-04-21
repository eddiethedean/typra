# Python packages

PyPI distributions and Python-facing code live here, **separate from Rust crates** under `crates/`.

| Path | Role |
|------|------|
| **`typra/`** | The **`typra`** package on PyPI: maturin + PyO3 native extension (`import typra`). |

Pure Python modules (e.g. helpers, type stubs) can be added under `typra/` as the project grows (see [maturin mixed projects](https://www.maturin.rs/project_layout.html)).
