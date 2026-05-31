# Examples

Runnable sample applications for Typra. Each example follows **problem → solution → code → result**.

## Prerequisites

From the repository root:

```bash
make python-develop   # builds the typra extension into .venv
```

## Examples

| Directory | Run | Topics |
|-----------|-----|--------|
| [todo_app/](todo_app/) | `.venv/bin/python examples/todo_app/main.py add "task"` | Pydantic, CRUD, indexes, queries |
| [cli_notes/](cli_notes/) | `.venv/bin/python examples/cli_notes/main.py add "note"` | Dataclass, CLI persistence |
| [fastapi_app/](fastapi_app/) | `uvicorn examples.fastapi_app.main:app` | REST API, DI, validation |
| [desktop_app/](desktop_app/) | `.venv/bin/python examples/desktop_app/main.py` | User data dir, offline settings |

Rust facade demo: `cargo run -p typra --example open`

## Documentation

Published catalog: **[Examples on Read the Docs](https://typra.readthedocs.io/en/latest/examples/)**

Guides: [Pydantic](https://typra.readthedocs.io/en/latest/guides/pydantic/) · [FastAPI](https://typra.readthedocs.io/en/latest/guides/fastapi/) · [Quickstart](https://typra.readthedocs.io/en/latest/guides/quickstart/)
