# typra (Python)

[![CI](https://github.com/eddiethedean/typra/actions/workflows/ci.yml/badge.svg)](https://github.com/eddiethedean/typra/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/typra.svg)](https://pypi.org/project/typra/)
[![Docs](https://readthedocs.org/projects/typra/badge/?version=latest)](https://typra.readthedocs.io/en/latest/)

> **SQLite simplicity, with real types.**

Official **CPython** bindings for Typra (PyO3). **Recommended API:** **`typra.models`** with dataclasses or Pydantic v2.

**Read the docs:** **[typra.readthedocs.io](https://typra.readthedocs.io/en/latest/)**

| | |
|--|--|
| [Python guide](https://typra.readthedocs.io/en/latest/guides/python/) | Full API, queries, DB-API, `fields_json` |
| [Quickstart](https://typra.readthedocs.io/en/latest/guides/quickstart/) | Install and first steps |
| [Models & collections](https://typra.readthedocs.io/en/latest/guides/models_and_collections/) | Class schemas and projections |
| [Python API reference](https://typra.readthedocs.io/en/latest/reference/python_api/) | Curated member list |
| [Types matrix](https://typra.readthedocs.io/en/latest/reference/types/) | Supported types and constraints |
| [Repository](https://github.com/eddiethedean/typra) · [Rust crate](https://crates.io/crates/typra) | Source and engine |

## Install

**CPython 3.9+** · stable ABI wheels (`cp39-abi3`)

```bash
pip install "typra>=1.0.0,<2"
```

## Quick start

```python
# Setup: class-defined schema + in-memory DB.
from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, Optional

import typra


@dataclass
class Book:
    __typra_primary_key__ = "title"
    __typra_indexes__ = [
        typra.models.index("year"),
        typra.models.unique("title"),
    ]

    title: str
    year: Annotated[int, typra.models.constrained(min_i64=0)]
    rating: Optional[float] = None


db = typra.Database.open_in_memory()
books = typra.models.collection(db, Book)

books.insert(Book(title="Typra", year=2020, rating=4.5))
print(books.get("Typra"))
print(typra.__version__)
```

Output:

```text
Book(title='Typra', year=2020, rating=4.5)
1.0.0
```

Next steps: **[Python guide](https://typra.readthedocs.io/en/latest/guides/python/)** (indexed queries, migrations, errors, DB-API) · **[Operations runbook](https://typra.readthedocs.io/en/latest/ops/operations_and_failure_modes/)**

## Build from source

Requires Rust, Python 3.9+, and [maturin](https://www.maturin.rs/).

```bash
cd python/typra && maturin develop --release && pytest -q
```

Contributor layout: [python/README.md](https://github.com/eddiethedean/typra/blob/main/python/README.md) · full pipeline: `make check-full` from repo root

## License

MIT — [LICENSE](https://github.com/eddiethedean/typra/blob/main/LICENSE)
