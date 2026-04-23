# Migrating from 0.6.x to 0.7.0

## Summary

0.7.0 adds **secondary indexes** (unique and non-unique), persisted **`SegmentType::Index`** segments, a minimal **query** planner (**equality**, **`And`**, **`limit`**, heuristic **`explain`**), **`Database::query_iter`** (Rust), subset **row projection** by field paths, and matching **Python** APIs (`indexes_json`, `db.collection(...).where` / `all(fields=[...])`, …).

## Rust

- **Additive**: new APIs for index-backed queries and iterators; existing **`insert`** / **`get`** flows unchanged if you do not declare indexes.
- **`register_collection`** / **`register_schema_version`** accept index definitions; new catalog writes may use **catalog v4** when indexes are present (see [`02_on_disk_file_format.md`](02_on_disk_file_format.md)).

## Python

- **`register_collection`** gains optional **`indexes_json`** (JSON array of index defs). Omit it for the same behavior as 0.6.x.
- Optional **`db.collection(name).where(...).and_where(...).limit(...).explain()`** and **`all()`** / **`all(fields=[...])`** for queries and subset dicts.

## On-disk compatibility

- Databases created in **0.6.x** open in **0.7.0**; replay includes any **index** segments written after upgrade.
- Until you register collections **with** indexes and insert rows, you only see **schema** and **record** replay as in 0.6.x.

## See also

- [`guide_python.md`](guide_python.md) (queries, indexes, subset projection)
- [`CHANGELOG.md`](../CHANGELOG.md)
