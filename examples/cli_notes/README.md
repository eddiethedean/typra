# CLI notes example

**Problem:** A CLI tool needs structured, durable local storage without running a database server.

**Solution:** Dataclass models in a single `notes.typra` file next to the script.

## Run

```bash
.venv/bin/python examples/cli_notes/main.py add "deploy checklist"
.venv/bin/python examples/cli_notes/main.py list
```

## What it demonstrates

- Dataclass schema (no Pydantic required)
- Append-style CLI workflow
- On-disk single-file persistence

Docs: [Why Typra](https://typra.readthedocs.io/en/latest/guides/why_typra/) · [Typra vs JSON](https://typra.readthedocs.io/en/latest/comparisons/json/)
