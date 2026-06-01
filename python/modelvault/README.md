# modelvault (Python)

[![CI](https://github.com/eddiethedean/modelvault/actions/workflows/ci.yml/badge.svg)](https://github.com/eddiethedean/modelvault/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/modelvault.svg)](https://pypi.org/project/modelvault/)
[![Docs](https://readthedocs.org/projects/modelvault/badge/?version=latest)](https://modelvault.readthedocs.io/en/latest/)

## Store Pydantic models directly

**Schema-first typed storage for application models.** Official **CPython** bindings for ModelVault (PyO3).

Store dataclasses and **Pydantic v2** models with validation, indexes, migrations, and single-file deployment — no low-level schema JSON required for the recommended path.

**Asyncio:** `AsyncDatabase` runs engine work on a thread pool and supports **concurrent reads** on one handle (`await asyncio.gather(...)` for many `get` / `query` calls). Writes and transactions remain exclusive. See [Async policy](https://modelvault.readthedocs.io/en/latest/reference/async_policy/).

**Read the docs:** **[modelvault.readthedocs.io](https://modelvault.readthedocs.io/en/latest/)**

| | |
|--|--|
| [Why ModelVault](https://modelvault.readthedocs.io/en/latest/guides/why_modelvault/) | Positioning and tradeoffs |
| [Pydantic guide](https://modelvault.readthedocs.io/en/latest/guides/pydantic/) | Model-first schemas |
| [FastAPI guide](https://modelvault.readthedocs.io/en/latest/guides/fastapi/) | Sync + async API services |
| [Async policy](https://modelvault.readthedocs.io/en/latest/reference/async_policy/) | `AsyncDatabase`, concurrent reads |
| [Quickstart](https://modelvault.readthedocs.io/en/latest/guides/quickstart/) | First insert in minutes |
| [Why ModelVault](https://modelvault.readthedocs.io/en/latest/guides/why_modelvault/) | Positioning and design goals |

## Install

**CPython 3.9+** · stable ABI wheels (`cp39-abi3`)

```bash
pip install "modelvault>=0.15.0,<0.16"
```

## Quick start (Pydantic)

```python
from pydantic import BaseModel
import modelvault

class Book(BaseModel):
    __modelvault_primary_key__ = "title"
    title: str
    year: int

db = modelvault.Database.open_in_memory()
books = modelvault.models.collection(db, Book)
books.insert(Book(title="ModelVault", year=2020))
print(books.get("ModelVault"))
print(modelvault.__version__)
```

Output:

```text
title='ModelVault' year=2020
0.15.0
```

Dataclass example and indexed queries: **[Quickstart](https://modelvault.readthedocs.io/en/latest/guides/quickstart/)** · **[Python guide](https://modelvault.readthedocs.io/en/latest/guides/python/)**

## Build from source

Requires Rust, Python 3.9+, and [maturin](https://www.maturin.rs/).

```bash
cd python/modelvault && maturin develop --release && pytest -q
```

Contributor layout: [python/README.md](https://github.com/eddiethedean/modelvault/blob/main/python/README.md) · full pipeline: `make check-full` from repo root

## License

MIT — [LICENSE](https://github.com/eddiethedean/modelvault/blob/main/LICENSE)
