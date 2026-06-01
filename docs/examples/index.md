# Example applications

**Audience:** developers who learn best from runnable code.

These examples show ModelVault in **real application shapes**—not isolated one-liners. Each maps a common persona from the [README](https://github.com/eddiethedean/modelvault/blob/main/README.md) to a small, inspectable project.

## Runnable examples (repository)

From the repo root after `make python-develop`:

| Example | Command | What it demonstrates |
|---------|---------|---------------------|
| **Todo app** | `python examples/todo_app/main.py add "Ship"` | Pydantic models, CRUD, indexed queries on disk |
| **CLI notes** | `python examples/cli_notes/main.py add "note"` | Dataclasses, durable CLI persistence |
| **FastAPI** | `uvicorn examples.fastapi_app.main:app` | REST API, `async def` handlers, `AsyncDatabase`, dependencies |
| **Desktop data dir** | `python examples/desktop_app/main.py` | Per-user `app.modelvault`, offline settings |

Source and per-example READMEs: [examples on GitHub](https://github.com/eddiethedean/modelvault/tree/main/examples)

!!! note "FastAPI dependencies"
    The FastAPI example requires `pip install fastapi uvicorn` in addition to ModelVault.

**CI:** `make examples-smoke` runs todo, CLI, and desktop smoke tests (FastAPI is documented but not in the default smoke target).

## Documented walkthroughs

| Example | Where | What it demonstrates |
|---------|-------|---------------------|
| **Books (minimal)** | [Quickstart](../guides/quickstart.md) | Insert, get, in-memory |
| **Inventory workflow** | [Python guide](../guides/python.md#realistic-workflow-indexed-queries-on-disk) | On-disk file, indexes, conjunctive queries |
| **FastAPI patterns** | [FastAPI guide](../guides/fastapi.md) | Lifespan, `AsyncDatabase`, `async_collection`, `async def` routes |

## Rust

```bash
cargo run -p modelvault --example open
```

Registers a collection via the facade API—see [Quickstart](../guides/quickstart.md).

## Desktop and installers

The [desktop_app](https://github.com/eddiethedean/modelvault/tree/main/examples/desktop_app) example stores settings under the OS application data directory. A GUI shell (e.g. Tauri) can use the same `Database.open` pattern—see [Storage modes](../guides/storage_modes.md).

## Narrative introduction

[ModelVault: the database for application models](https://github.com/eddiethedean/modelvault/blob/main/blog/modelvault-for-application-models.md) — essay-length overview for evaluators and blog readers.

## Next steps

- [Why ModelVault](../guides/why_modelvault.md)
- [Pydantic](../guides/pydantic.md) · [FastAPI](../guides/fastapi.md)
- [Comparisons](../comparisons/index.md)
