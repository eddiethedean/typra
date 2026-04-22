# Migrating from 0.5.x to 0.6.0

## Summary

0.6.0 adds **validation**, **nested row values**, **record payload v2**, and **catalog payload v3** (per-field **constraints**). Inserts are validated before persistence; new record segments use **version 2** encoding while **version 1** segments still replay.

## Rust

- **`Database::insert`** and in-memory **`latest`** rows use **`BTreeMap<String, RowValue>`** instead of only `ScalarValue`.
- **`get`** still takes a primitive **`&ScalarValue`** primary key but returns **`Option<BTreeMap<String, RowValue>>`**.
- New error variant **`DbError::Validation(ValidationError)`** for type/constraint failures (with nested **path**).
- **`FieldDef`** includes **`constraints: Vec<Constraint>`** (default empty). New catalog writes use **catalog v3**; older files with v1/v2 catalogs still open.

### Mechanical migration

Replace scalar-only maps with **`RowValue`** variants (`RowValue::String`, `RowValue::Int64`, …). Use **`RowValue::from_scalar`** if you still have a `ScalarValue`.

## Python

- Row **`dict`** values may be nested **`dict`** / **`list`** when the schema uses **`object`**, **`list`**, **`optional`**, or **`enum`**.
- Optional **`fields_json`** key **`"constraints"`** on each field: array of objects such as `{"min_i64": 0}`, `{"email": true}`, `{"regex": "^[A-Z]+$"}` (see [`python/typra/README.md`](../python/typra/README.md)).
- Validation failures surface as **`ValueError`** with the same messages as the Rust **`Display`** for **`ValidationError`**.

## On-disk compatibility

- Existing databases with only **record v1** segments continue to open; replay accepts **v1** and **v2**.
- New **schema** segments are written as **catalog v3**; readers still decode **v1/v2/v3**.

## See also

- [`07_record_encoding_v2.md`](07_record_encoding_v2.md)
- [`CHANGELOG.md`](../CHANGELOG.md)
