# Record encoding v3 (1.0.0)

**Status:** required when the collection schema includes any **multi-segment** `FieldPath`. Otherwise prefer [v2](record_encoding_v2.md).

Record payloads live in `SegmentType::Record` segments. Payloads begin with a little-endian `u16` **payload version** (**3**).

## Motivation

[v1](record_encoding_v1.md) and [v2](record_encoding_v2.md) store non-PK values in a fixed **schema order** and assume top-level field definitions. That cannot represent nested leaf paths such as `["profile","timezone"]` without flattening.

v3 keeps the public row shape as a nested object tree (`RowValue::Object`) but persists each value with its full **`FieldPath`**, so replay rebuilds nested rows deterministically.

## Wire layout (version 3)

All integers are **little-endian**.

| Field | Type | Notes |
|-------|------|------|
| `payload_version` | `u16` | `3` |
| `collection_id` | `u32` | Must exist in catalog |
| `schema_version` | `u32` | Must equal catalog’s current version for that collection |
| `op` | `u8` | `1` = insert, `2` = replace, `3` = delete |
| `pk` | tagged primitive | Primary key: single-segment top-level scalar ([v1 tags](record_encoding_v1.md#tagged-scalar-primitives)) |
| `field_count` | `u32` | Number of **non-PK** schema fields (0 for delete) |
| `entries` | repeated | For each non-PK field: `(field_path, value)` |

### `field_path` encoding (record v3)

Distinct from [catalog FieldPath](catalog_encoding.md#fieldpath-catalog) (which uses `u32` segment count):

| Field | Type | Notes |
|-------|------|-------|
| `segment_count` | `u8` | Must be ≥ 1 and ≤ 255 |
| `segments` | repeated | Each: `u16` byte length + UTF-8 bytes (non-empty) |

### `value` encoding

Each value uses [RowValue encoding](record_encoding_v2.md#rowvalue-encoding) for the field’s schema `Type`.

## Decoding and replay rules

- `field_count` must match the number of non-PK fields in the schema (0 for `op = 3`).
- Each decoded `field_path` must match exactly one schema `FieldDef.path` (excluding PK).
- Duplicate `field_path` entries are rejected.
- The decoded row is reconstructed as a nested object tree:
  - `["a"]` → top-level key `"a"`.
  - `["a","b","c"]` → nested objects ending at `row["a"]["b"]["c"]`.
- **Last-write-wins** by `(collection_id, pk)` at the segment replay layer.

## When to emit v3

Emit v3 when the collection schema includes any multi-segment `FieldDef.path`; otherwise emit v2.

## Compatibility

Replay accepts **v1, v2, and v3** in the same file.

## See also

- [On-disk file format](on_disk_file_format.md)
- [Catalog encoding](catalog_encoding.md)
