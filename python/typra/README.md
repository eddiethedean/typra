# typra (Python)

[![CI](https://github.com/eddiethedean/typra/actions/workflows/ci.yml/badge.svg)](https://github.com/eddiethedean/typra/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/typra.svg)](https://pypi.org/project/typra/)

Python bindings for **Typra**. **0.3.0** exposes `typra.__version__`; higher-level APIs will ship in later releases.

## Install

```bash
pip install "typra>=0.3.0,<0.4"
```

Supports **CPython 3.9+**. Wheels are published as **`cp39-abi3`** (one per platform for CPython 3.9+).

```python
import typra

print(typra.__version__)
```
