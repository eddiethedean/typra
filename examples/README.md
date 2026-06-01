# Examples

Runnable sample applications for ModelVault. Each example follows **problem → solution → code → result**.

## Prerequisites

From the repository root:

```bash
make python-develop   # builds the modelvault extension into .venv
```

## Examples

| Directory | Run | Topics |
|-----------|-----|--------|
| [todo_app/](todo_app/) | `.venv/bin/python examples/todo_app/main.py add "task"` | Pydantic, CRUD, indexes, queries |
| [cli_notes/](cli_notes/) | `.venv/bin/python examples/cli_notes/main.py add "note"` | Dataclass, CLI persistence |
| [fastapi_app/](fastapi_app/) | `uvicorn examples.fastapi_app.main:app` (sync) or `main_async:app` (async + parallel reads) | REST API, DI, validation |
| [desktop_app/](desktop_app/) | `.venv/bin/python examples/desktop_app/main.py` | User data dir, offline settings |

Rust facade demo: `cargo run -p modelvault --example open`

## Documentation

Published catalog: **[Examples on Read the Docs](https://modelvault.readthedocs.io/en/latest/examples/)**

Guides: [Pydantic](https://modelvault.readthedocs.io/en/latest/guides/pydantic/) · [FastAPI](https://modelvault.readthedocs.io/en/latest/guides/fastapi/) · [Quickstart](https://modelvault.readthedocs.io/en/latest/guides/quickstart/)
