# Comparisons

**Audience:** technical leads and developers choosing local persistence.

ModelVault is **the database for application models**—not a universal replacement for every datastore. Use this section to explain the product to your team and to pick the right tool for each job.

## The question ModelVault answers

> **Where do I put typed application data locally without running a database server?**

If your answer today is “SQLite + ORM,” “a JSON file,” or “TinyDB,” read the tables below. If your primary need is **warehouse-scale analytics**, start with [ModelVault vs DuckDB](duckdb.md).

## Why ModelVault? (README summary)

| Alternative | Limitation | ModelVault |
|-------------|------------|------------|
| **Relational DBs** | App models mapped into tables/rows; schema friction | Model-first schemas; validation on write; nested objects native |
| **JSON files** | No indexes, weak queries, manual integrity | Typed storage, indexes, queries, durability |
| **TinyDB** | Document store without production-grade validation or evolution | Strict schemas, migrations, indexes, crash-safe file format |
| **DuckDB** | Built for analytics (OLAP), not app CRUD | Embedded OLTP for application models (complementary, not competing) |

[:octicons-arrow-right-24: Why ModelVault](../guides/why_modelvault.md)

## Capability matrix

| Capability | SQLite | JSON files | TinyDB | DuckDB | **ModelVault** |
|------------|--------|------------|--------|--------|----------------|
| **Primary abstraction** | SQL tables | Files / documents | JSON documents | SQL tables (analytics) | **Typed collections / models** |
| **Maps to app models** | Via ORM / manual | Manual | Manual | Via SQL | **Native (`modelvault.models`)** |
| **Validation on write** | App / `CHECK` | None | App | App / constraints | **Engine (types + constraints)** |
| **Nested objects** | JSON column / serialize | Yes (untyped) | Yes | Structs / nested | **First-class typed paths** |
| **Secondary indexes** | SQL indexes | None | Limited | Yes | **Declared on model** |
| **Schema evolution** | SQL migrations | Manual versioning | Weak | SQL migrations | **Catalog + plan/apply** |
| **Single-file deploy** | Yes | Yes | Yes | Yes | **Yes** |
| **Server required** | No | No | No | No | **No** |
| **Best for** | General embedded SQL | Prototypes, config | Tiny scripts | **OLAP / analytics** | **Application OLTP models** |

## Deep dives

| Comparison | Read when… |
|------------|------------|
| [ModelVault vs SQLite](sqlite.md) | You know SQLite and want model-first ergonomics without ORM ceremony |
| [ModelVault vs JSON files](json.md) | You outgrew `data.json` and need indexes and integrity |
| [ModelVault vs TinyDB](tinydb.md) | You want document-like simplicity with production structure |
| [ModelVault vs DuckDB](duckdb.md) | You are splitting app state from analytics workloads |

## Hybrid architecture (recommended pattern)

ModelVault and DuckDB are **complementary**:

| Layer | Role |
|-------|------|
| **ModelVault** | Authoritative **application state**—users, settings, domain records, CLI persistence |
| **DuckDB** | **Analytics**, exports, ad-hoc SQL on snapshots or ETL inputs |

Many teams already use **SQLite + DuckDB**; **ModelVault + DuckDB** follows the same split with a model-native OLTP side.

## What ModelVault does not replace

- **Network multi-user databases** — use PostgreSQL or MySQL
- **Full SQL reporting** across arbitrary relational schemas — use SQLite or a server database
- **Large-scale OLAP** — use DuckDB or a warehouse

## Next steps

- [Quickstart](../guides/quickstart.md) — first model in five minutes
- [Pydantic guide](../guides/pydantic.md) — Python model-first path
- [Core concepts](../guides/concepts.md) — mental model
