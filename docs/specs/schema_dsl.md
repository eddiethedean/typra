# Schema DSL spec

## Field paths (1.0 contract)

ModelVault’s internal schema model uses `FieldPath` segments to address nested fields.

In 1.0, collection schemas may define fields using **multi-segment paths** (e.g. `["profile","timezone"]`) as first-class nested leaf fields, subject to invariants:

- paths must be non-empty and have no empty segments
- no duplicate field paths
- no parent/child conflicts (e.g. defining both `["a"]` and `["a","b"]` is rejected)
- the primary key remains a single-segment top-level scalar field

## On-disk catalog wire format

How schemas are persisted in `SegmentType::Schema` segments is specified in **[Catalog encoding](catalog_encoding.md)** (binary payloads, catalog versions v1–v4, constraints, indexes).

## Text DSL (future)

A human-readable schema DSL (`.tdb` files) is described in the historical design doc [Schema DSL spec on GitHub](https://github.com/eddiethedean/modelvault/blob/main/docs/04_schema_dsl_spec.md). That grammar is **not** what the engine writes to disk today; registration uses Rust `FieldDef` / Python model inference, encoded via the catalog codec.

## See also

- [Types matrix](../reference/types.md)
- [Record encoding v3](record_encoding_v3.md)
- [Schema migrations](../guides/python.md) (Python guide)
