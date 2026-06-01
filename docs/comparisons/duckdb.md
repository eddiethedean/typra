# ModelVault vs DuckDB

[DuckDB](https://duckdb.org/) is an embedded **analytical** SQL engine (OLAP). ModelVault is an embedded **application model** store (OLTP). They solve different problems and often **belong in the same project**.

## Problem

Teams sometimes reach for one embedded database for everything:

- **App state** — users, settings, small domain tables
- **Analytics** — aggregations over millions of rows, columnar scans, Parquet ingest

DuckDB excels at the second. Using it as the only store for fine-grained app CRUD with Pydantic models is workable but not its sweet spot — you still model SQL, not your classes.

## Solution

Use ModelVault for **authoritative application data** and DuckDB when you need **analytics**:

```text
┌─────────────────┐     export / ETL      ┌──────────────┐
│  app.modelvault      │ ────────────────────► │  reports.duckdb │
│  (ModelVault OLTP)   │                       │  (DuckDB OLAP)  │
└─────────────────┘                       └──────────────┘
```

**Result:** model-native writes in the app; columnar SQL for dashboards and batch jobs.

## OLTP vs OLAP

| Dimension | ModelVault | DuckDB |
|-----------|-------|--------|
| Workload | Point reads/writes, app CRUD | Scans, aggregations, analytics |
| Schema | Model / collection catalog | SQL tables |
| Primary user | App developer with Pydantic/Rust | Data analyst / pipeline |
| Nested app objects | Native typed paths | SQL + nested types (varies) |

## When DuckDB wins

- Large **analytical** queries over historical data
- **Parquet/CSV** ingestion and columnar performance
- **SQL-first** exploration by analysts

## When ModelVault wins

- **Application models** with validation on every write
- **Single-file** app deployment with indexes on domain fields
- **Schema evolution** tied to your Python/Rust types

## Hybrid usage

1. Persist domain records in ModelVault (`users`, `orders`, `settings`).
2. Periodically export snapshots (JSON, CSV, or custom) into DuckDB for reporting.
3. Keep ModelVault as the **system of record**; DuckDB as **read-optimized replica**.

## Related

- [Why ModelVault](../guides/why_modelvault.md)
- [Comparisons overview](index.md)
