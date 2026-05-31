# Todo app example

**Problem:** You need a small local task list with durable storage and indexed “open tasks” queries.

**Solution:** A Pydantic `Task` model and `typra.models.collection` — one `tasks.typra` file beside this script.

## Run

From the repository root (after `make python-develop`):

```bash
.venv/bin/python examples/todo_app/main.py add "Write docs"
.venv/bin/python examples/todo_app/main.py add "Ship release"
.venv/bin/python examples/todo_app/main.py list
.venv/bin/python examples/todo_app/main.py open
.venv/bin/python examples/todo_app/main.py done 1
.venv/bin/python examples/todo_app/main.py delete 2
```

**Result:** tasks persist in `examples/todo_app/tasks.typra` across runs.

## What it demonstrates

- Pydantic model as schema (`__typra_primary_key__`, indexes)
- Insert, get, update, delete
- Indexed query: `where(Task.done, False)`

Docs: [Pydantic guide](https://typra.readthedocs.io/en/latest/guides/pydantic/) · [Examples](https://typra.readthedocs.io/en/latest/examples/)
