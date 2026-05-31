# Example applications

Example-driven docs follow **problem → solution → code → result**. Use these to see Typra in realistic application shapes.

## Runnable examples (repository)

From the repo root after `make python-develop`:

| Example | Command | Demonstrates |
|---------|---------|----------------|
| **Todo app** | `python examples/todo_app/main.py add "Ship"` | Pydantic, CRUD, indexed `open` query |
| **CLI notes** | `python examples/cli_notes/main.py add "note"` | Dataclass, durable CLI storage |
| **FastAPI** | `uvicorn examples.fastapi_app.main:app` | REST, dependencies, shared Pydantic models |
| **Desktop data dir** | `python examples/desktop_app/main.py` | Per-user `app.typra`, offline settings |

Source and READMEs: [github.com/eddiethedean/typra/tree/main/examples](https://github.com/eddiethedean/typra/tree/main/examples)

**CI:** `make examples-smoke` runs todo, CLI, and desktop (desktop uses `TYPRA_EXAMPLE_DATA_DIR` under `examples/desktop_app/.smoke-data` so your real app-data folder is untouched).

## In documentation

| Example | Where | Demonstrates |
|---------|-------|----------------|
| **Books (minimal)** | [Quickstart](../guides/quickstart.md) | Insert, get, in-memory |
| **Inventory workflow** | [Python guide](../guides/python.md#realistic-workflow-indexed-queries-on-disk) | On-disk file, indexes, conjunctive queries |
| **FastAPI patterns** | [FastAPI guide](../guides/fastapi.md) | Architecture without a full app checkout |

## Rust

```bash
cargo run -p typra --example open
```

Registers a collection via the facade API — see [Quickstart](../guides/quickstart.md).

## Desktop

The [desktop_app](https://github.com/eddiethedean/typra/tree/main/examples/desktop_app) example stores settings under the OS app-data directory. A full GUI shell (e.g. Tauri) can use the same `Database.open` pattern — see [Storage modes](../guides/storage_modes.md).

## Launch essay

[Typra: the database for application models](https://github.com/eddiethedean/typra/blob/main/blog/typra-for-application-models.md) — narrative intro for evaluators.

## Next steps

- [Why Typra](../guides/why_typra.md)
- [Pydantic](../guides/pydantic.md) · [FastAPI](../guides/fastapi.md)
- [Comparisons](../comparisons/index.md)
