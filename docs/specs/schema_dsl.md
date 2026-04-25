# Schema DSL spec

Canonical spec: [`docs/04_schema_dsl_spec.md`](../04_schema_dsl_spec.md).

## Field paths (1.0 contract)

Typra’s internal schema model uses `FieldPath` segments to address nested fields.

In 1.0, collection schemas may define fields using **multi-segment paths** (e.g. `["profile","timezone"]`) as first-class nested leaf fields, subject to invariants:

- paths must be non-empty and have no empty segments
- no duplicate field paths
- no parent/child conflicts (e.g. defining both `["a"]` and `["a","b"]` is rejected)
- the primary key remains a single-segment top-level scalar field

