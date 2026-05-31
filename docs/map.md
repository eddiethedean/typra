# Choose your path

Not sure which doc to open? Start here — organized by **outcomes**, not engine internals.

## I'm evaluating Typra

| Question | Read |
|----------|------|
| Why does Typra exist? | [Why Typra](guides/why_typra.md) |
| How is it different from SQLite / JSON / TinyDB? | [Comparisons](comparisons/index.md) |
| Can I try it in 5 minutes? | [Quickstart](guides/quickstart.md) |
| What does it guarantee? | [Compatibility](reference/compatibility.md) · [Types](reference/types.md) · [Security](reference/security.md) |
| What's planned next? | [Roadmap on GitHub](https://github.com/eddiethedean/typra/blob/main/ROADMAP.md) |

## I'm building an app

| I use… | Start with |
|--------|------------|
| **Pydantic** | [Pydantic guide](guides/pydantic.md) → [Models & collections](guides/models_and_collections.md) |
| **FastAPI** | [FastAPI guide](guides/fastapi.md) → [Python guide](guides/python.md) |
| **Python (general)** | [Python guide](guides/python.md) → [Quickstart](guides/quickstart.md) |
| **Rust** | [Quickstart](guides/quickstart.md) → [Rust API](reference/rust_api.md) |

Then:

- [Core concepts](guides/concepts.md) — mental model
- [Storage modes](guides/storage_modes.md) — file vs memory
- [Examples](examples/index.md) — application patterns
- [Operations runbook](ops/operations_and_failure_modes.md) — backups, recovery, locking

## I'm operating in production

| Task | Doc |
|------|-----|
| Backup / restore | [Operations runbook](ops/operations_and_failure_modes.md) |
| Inspect or verify a file | [CLI reference](reference/cli.md) |
| Corruption or crash recovery | [Compatibility → recovery](reference/compatibility.md) |
| Enable tracing | [Debugging](ops/debugging.md) |

## I'm contributing or extending Typra

| Task | Doc |
|------|-----|
| Dev setup & CI | [Contributing](dev/contributing_guide.md) |
| Doc audience & editorial rules | [Documentation map](dev/documentation_map.md) |
| File format & engine layout | [Specifications](specs/index.md) |
| 1.0 release checklist | [Readiness](reference/readiness.md) |

## Terminology

Quick definitions: [Glossary](glossary.md).
