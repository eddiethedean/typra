# Documentation map

How the ModelVault docs are organized by **audience** and **goal**. Use this when editing or reviewing pages for consistent positioning ([master plan](https://github.com/eddiethedean/modelvault/blob/main/docs/MODELVAULT_DOCUMENTATION_POSITIONING_MASTER_PLAN.md)).

## Audience levels

| Level | Who | What they need |
|-------|-----|----------------|
| **Beginner** | Evaluating or first insert | Why, comparisons, quickstart, Pydantic, examples |
| **Intermediate** | Shipping an app | Python guide, FastAPI, storage modes, operations, reference APIs |
| **Advanced** | Engine / format / ops at scale | Specifications, compatibility matrix, security, CLI, debugging |

Every major guide should open with **outcomes** (problem → solution), not implementation trivia.

## By goal

### Evaluate ModelVault (beginner)

| Page | Level |
|------|-------|
| [Home](../index.md) | Beginner |
| [Why ModelVault](../guides/why_modelvault.md) | Beginner |
| [Comparisons](../comparisons/index.md) | Beginner |
| [Quickstart](../guides/quickstart.md) | Beginner |
| [Choose your path](../map.md) | Beginner |

### Build an application (beginner → intermediate)

| Page | Level |
|------|-------|
| [Pydantic](../guides/pydantic.md) | Beginner |
| [FastAPI](../guides/fastapi.md) | Intermediate |
| [Python guide](../guides/python.md) | Intermediate |
| [Models & collections](../guides/models_and_collections.md) | Intermediate |
| [Core concepts](../guides/concepts.md) | Beginner |
| [Storage modes](../guides/storage_modes.md) | Intermediate |
| [Examples](../examples/index.md) | Beginner |

### Operate in production (intermediate)

| Page | Level |
|------|-------|
| [Operations runbook](../ops/operations_and_failure_modes.md) | Intermediate |
| [Debugging](../ops/debugging.md) | Advanced |
| [CLI](../reference/cli.md) | Intermediate |
| [Compatibility](../reference/compatibility.md) | Intermediate |
| [Security](../reference/security.md) | Intermediate |
| [Types matrix](../reference/types.md) | Intermediate |

### API reference (intermediate)

| Page | Level |
|------|-------|
| [Python API](../reference/python_api.md) | Intermediate |
| [Rust API](../reference/rust_api.md) | Intermediate |
| [Async policy](../reference/async_policy.md) (concurrent reads, exclusive writes) | Advanced |

### Engine & format (advanced)

| Page | Level |
|------|-------|
| [Specifications overview](../specs/index.md) | Advanced |
| [On-disk format](../specs/on_disk_file_format.md) | Advanced |
| [Catalog / index / record encodings](../specs/index.md#normative-on-disk-specification-10x) | Advanced |
| [Query planner](../specs/query_planner.md) | Advanced |
| [Architecture](../specs/full_architecture.md) | Advanced |

## Runnable examples (repo)

| Example | Level | Path |
|---------|-------|------|
| Todo app | Beginner | `examples/todo_app/` |
| CLI notes | Beginner | `examples/cli_notes/` |
| FastAPI | Intermediate | `examples/fastapi_app/` |
| Desktop data dir | Intermediate | `examples/desktop_app/` |

CI: `make examples-smoke` (todo + CLI + desktop with isolated `MODELVAULT_EXAMPLE_DATA_DIR`).

## Launch material

| Asset | Path |
|-------|------|
| Copy bank | [launch_messaging.md](launch_messaging.md) |
| Blog post | [blog/modelvault-for-application-models.md](https://github.com/eddiethedean/modelvault/blob/main/blog/modelvault-for-application-models.md) |

## Editorial checklist (new or updated pages)

1. **Headline** — outcome or question, not “SegmentType catalog”
2. **Problem** — who is stuck and why
3. **Solution** — what ModelVault provides
4. **Code + result** — runnable or verified output where possible
5. **Links** — Why ModelVault, comparisons, or quickstart for newcomers
6. **Level** — place advanced detail behind intermediate summaries

## Maintainers

- Verified stdout: `make verify-doc-examples`
- Example CLIs: `make examples-smoke`
- Full gate: `make check-full` (includes examples-smoke)
