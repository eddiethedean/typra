# typra (Python)

[![CI](https://github.com/eddiethedean/typra/actions/workflows/ci.yml/badge.svg)](https://github.com/eddiethedean/typra/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/typra.svg)](https://pypi.org/project/typra/)
[![Docs](https://readthedocs.org/projects/typra/badge/?version=latest)](https://typra.readthedocs.io/en/latest/)

## Store Pydantic models directly

**SQLite simplicity, with real types.** Official **CPython** bindings for Typra (PyO3).

Store dataclasses and **Pydantic v2** models with validation, indexes, migrations, and single-file deployment — no low-level schema JSON required for the recommended path.

**Read the docs:** **[typra.readthedocs.io](https://typra.readthedocs.io/en/latest/)**

| | |
|--|--|
| [Why Typra](https://typra.readthedocs.io/en/latest/guides/why_typra/) | Positioning and tradeoffs |
| [Pydantic guide](https://typra.readthedocs.io/en/latest/guides/pydantic/) | Model-first schemas |
| [FastAPI guide](https://typra.readthedocs.io/en/latest/guides/fastapi/) | Small API services |
| [Quickstart](https://typra.readthedocs.io/en/latest/guides/quickstart/) | First insert in minutes |
| [Comparisons](https://typra.readthedocs.io/en/latest/comparisons/) | vs SQLite, JSON, TinyDB, DuckDB |

## Install

**CPython 3.9+** · stable ABI wheels (`cp39-abi3`)

```bash
pip install "typra>=1.0.0,<2"
```

## Quick start (Pydantic)

```python
from pydantic import BaseModel
import typra

class Book(BaseModel):
    __typra_primary_key__ = "title"
    title: str
    year: int

db = typra.Database.open_in_memory()
books = typra.models.collection(db, Book)
books.insert(Book(title="Typra", year=2020))
print(books.get("Typra"))
print(typra.__version__)
```

Output:

```text
title='Typra' year=2020
1.0.0
```

Dataclass example and indexed queries: **[Quickstart](https://typra.readthedocs.io/en/latest/guides/quickstart/)** · **[Python guide](https://typra.readthedocs.io/en/latest/guides/python/)**

## Build from source

Requires Rust, Python 3.9+, and [maturin](https://www.maturin.rs/).

```bash
cd python/typra && maturin develop --release && pytest -q
```

Contributor layout: [python/README.md](https://github.com/eddiethedean/typra/blob/main/python/README.md) · full pipeline: `make check-full` from repo root

## License

MIT — [LICENSE](https://github.com/eddiethedean/typra/blob/main/LICENSE)
