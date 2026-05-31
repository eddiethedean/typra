# Launch messaging (copy bank)

Reusable copy aligned with the [positioning plan](https://github.com/eddiethedean/typra/blob/main/docs/TYPRA_DOCUMENTATION_POSITIONING_MASTER_PLAN.md). **Audience:** maintainers, blog posts, talks, PyPI/social updates.

## One-liners

- **SQLite simplicity, with real types.**
- **The database for application models.**
- **Store Pydantic models directly — validation, indexes, migrations, single-file deployment.**

## Tweet / social (short)

> Typra 1.0: typed embedded DB for app data. Store Pydantic/dataclass models in one file — validation on write, no DB server. Rust + Python. https://typra.readthedocs.io

## Elevator pitch (30 seconds)

Typra is an embedded database for **application models**, not ad-hoc SQL or JSON files. You define a Pydantic or dataclass schema; Typra validates on write, supports indexes and migrations, and ships as a **single `.typra` file**. Same engine in Rust and Python. Ideal for FastAPI side projects, desktop apps, and CLI tools that have outgrown JSON.

## vs SQLite (one sentence)

SQLite is the default embedded SQL engine; Typra is for teams whose source of truth is already **typed models** and who want validation and evolution without maintaining parallel SQL migrations.

## vs JSON (one sentence)

JSON files are fine until you need **indexes, queries, and integrity** — Typra keeps single-file deployment with engine-enforced schemas.

## Call to action

- Docs: https://typra.readthedocs.io/en/latest/guides/quickstart/
- PyPI: `pip install "typra>=1.0.0,<2"`
- Rust: `typra = "1.0"` on crates.io
- Examples: https://github.com/eddiethedean/typra/tree/main/examples

## Published blog draft

Full launch post (adapt for Dev.to, company blog, etc.):

- [blog/typra-for-application-models.md](https://github.com/eddiethedean/typra/blob/main/blog/typra-for-application-models.md)

## Talk outline (15 minutes)

1. Problem: app models vs SQL vs JSON (3 min)
2. Demo: Pydantic → `typra.models.collection` → query (5 min)
3. Single-file ops: backup, CLI inspect (3 min)
4. Comparisons and roadmap: SQL track post-1.0 (4 min)

## Do not lead with

- Record payload v3, segment types, catalog wire format
- “Schema catalog” without explaining user-visible outcomes
