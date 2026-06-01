# Choose your path

Use this page when you know your **goal** but not which document to open. The site is organized by outcomes—not by Rust module names or segment types.

## I am evaluating ModelVault

You need a clear answer to *what problem this solves* and *what it does not try to be*.

| Question | Document |
|----------|----------|
| Why does ModelVault exist? | [Why ModelVault](guides/why_modelvault.md) |
| How is it different from SQLite, JSON, TinyDB, DuckDB? | [Comparisons](comparisons/index.md) |
| Can I try it in five minutes? | [Quickstart](guides/quickstart.md) |
| What does the project guarantee? | [Compatibility](reference/compatibility.md) · [Types](reference/types.md) · [Security](reference/security.md) |
| What is planned next? | [Roadmap on GitHub](https://github.com/eddiethedean/modelvault/blob/main/ROADMAP.md) |
| Narrative overview | [Launch essay](https://github.com/eddiethedean/modelvault/blob/main/blog/modelvault-for-application-models.md) |

Start on the [home page](index.md) for the same positioning summary as the GitHub README.

## I am building an application

You have chosen ModelVault (or are close) and need the right tutorial.

| I use… | Start here |
|--------|------------|
| **Pydantic** | [Pydantic guide](guides/pydantic.md) → [Models & collections](guides/models_and_collections.md) |
| **FastAPI** | [FastAPI guide](guides/fastapi.md) (`AsyncDatabase`, `async def`) → [Python guide](guides/python.md) |
| **Python (general)** | [Quickstart](guides/quickstart.md) → [Python guide](guides/python.md) |
| **Rust** | [Quickstart](guides/quickstart.md) → [Rust API](reference/rust_api.md) |

Then deepen your understanding:

- [Core concepts](guides/concepts.md) — database, collection, schema, validation
- [Storage modes](guides/storage_modes.md) — file vs memory, locking
- [Examples](examples/index.md) — todo, CLI, FastAPI, desktop patterns
- [Operations runbook](ops/operations_and_failure_modes.md) — backup, recovery

## I am operating in production

You run ModelVault in shipped software and need runbooks, not tutorials.

| Task | Document |
|------|----------|
| Backup and restore | [Operations runbook](ops/operations_and_failure_modes.md) |
| Inspect or verify a file | [CLI reference](reference/cli.md) |
| Crash or corruption recovery | [Compatibility](reference/compatibility.md) |
| Tracing and diagnostics | [Debugging](ops/debugging.md) |

## I am contributing or extending the engine

You work on the repository or need file-format contracts.

| Task | Document |
|------|----------|
| Dev setup and CI | [Contributing](dev/contributing_guide.md) |
| Documentation conventions | [Documentation map](dev/documentation_map.md) |
| On-disk layout and specs | [Specifications](specs/index.md) |
| Release checklist | [Readiness](reference/readiness.md) |

## Terminology

Short definitions: [Glossary](glossary.md).
