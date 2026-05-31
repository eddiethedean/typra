# Typra vs TinyDB

[TinyDB](https://github.com/msiemens/tinydb) is a lightweight Python document store — great for scripts, less ideal when you need **strict typing and production evolution**.

## Problem

TinyDB feels familiar: insert JSON-like documents, query with a simple API. For small tools that is enough.

Gaps for application backends:

- **Typing** — documents are untyped dicts
- **Validation** — integrity is application code
- **Indexes** — not comparable to declared secondary indexes on typed paths
- **Production readiness** — durability, recovery, and format contracts are not the focus
- **Schema evolution** — no first-class catalog versioning story

## Solution

Typra offers similar **Python ergonomics** with engine-level contracts:

```python
from dataclasses import dataclass
import typra

@dataclass
class Task:
    __typra_primary_key__ = "id"
    __typra_indexes__ = [typra.models.index("done")]
    id: int
    title: str
    done: bool

db = typra.Database.open("tasks.typra")
tasks = typra.models.collection(db, Task)
tasks.insert(Task(id=1, title="Ship docs", done=False))
open_tasks = tasks.where(Task.done, False).all()
```

**Result:** document-oriented developer experience with schemas, validation, and indexes enforced by the engine.

## Comparison

| Topic | TinyDB | Typra |
|-------|--------|-------|
| Data model | JSON documents | Typed collections |
| Validation | App | Engine |
| Indexes | Limited | First-class |
| Rust / Python | Python | Both |
| Format contract | Simple JSON file | Versioned binary segments + recovery |

## When TinyDB still wins

- One-off scripts with **minimal dependencies**
- You only need a few hundred documents and never evolve schema

## When Typra wins

- You are building a **product** (CLI, desktop, API) not a script
- You need **validation, indexes, and migrations**
- You may add a **Rust** component later — same engine

## Related

- [Why Typra](../guides/why_typra.md)
- [Pydantic guide](../guides/pydantic.md)
