# Comparisons

Typra is positioned as **the database for application models** — not a replacement for every datastore. Use this section to pick the right tool and explain Typra to your team.

## At a glance

| Capability | SQLite | JSON files | TinyDB | DuckDB | **Typra** |
|------------|--------|------------|--------|--------|-----------|
| **Primary abstraction** | SQL tables | Files / documents | JSON documents | SQL tables (analytics) | **Typed collections / models** |
| **Maps to app models** | Via ORM / manual | Manual | Manual | Via SQL | **Native (`typra.models`)** |
| **Validation on write** | App / CHECK | None | App | App / constraints | **Engine (types + constraints)** |
| **Nested objects** | JSON column / serialize | Yes (untyped) | Yes | Structs / nested | **First-class typed paths** |
| **Secondary indexes** | SQL indexes | None | Limited | Yes | **Declared on model** |
| **Schema evolution** | SQL migrations | Manual versioning | Weak | SQL migrations | **Catalog + plan/apply** |
| **Single-file deploy** | Yes | Yes | Yes | Yes | **Yes** |
| **Server required** | No | No | No | No | **No** |
| **Best for** | General embedded SQL | Prototypes, config | Tiny scripts | **OLAP / analytics** | **Application OLTP models** |

## Deep dives

| Comparison | Read when… |
|------------|------------|
| [Typra vs SQLite](sqlite.md) | You already know SQLite and want model-first ergonomics |
| [Typra vs JSON files](json.md) | You outgrew `data.json` |
| [Typra vs TinyDB](tinydb.md) | You want TinyDB-like simplicity with production structure |
| [Typra vs DuckDB](duckdb.md) | You are choosing between app storage and analytics |

## Hybrid patterns

Typra and DuckDB are **complementary**:

- **Typra** — authoritative app state (users, settings, domain records)
- **DuckDB** — analytics, exports, ad-hoc SQL on snapshots

Many teams use SQLite + DuckDB today; Typra + DuckDB follows the same split with a model-native OLTP side.

## Next steps

- [Why Typra](../guides/why_typra.md)
- [Quickstart](../guides/quickstart.md)
- [Pydantic guide](../guides/pydantic.md)
