# Record encoding v3 (1.0.0)

Record payloads live in `SegmentType::Record` segments. Payloads begin with a little-endian `u16` **payload version**:

- v1: primitives-only, non-PK values in schema order (0.5.x)
- v2: composite `RowValue` encoding, non-PK values in schema order (0.6.0+)
- **v3 (this document)**: supports **multi-segment schema field definitions** by encoding each non-PK value with its full **`FieldPath`**.

## Motivation

v1/v2 assume that collection schemas define non-PK fields as **single-segment, top-level** paths and store non-PK values in a fixed schema order. That cannot represent schemas whose field definitions are nested leaf paths (e.g. `["profile","timezone"]`) without losing structure.

v3 keeps the public row shape as a nested object tree (`RowValue::Object`) but persists values keyed by full `FieldPath`, so replay can rebuild nested rows deterministically.

## Wire layout (version 3)

All integers are **little-endian**.

| Field | Type | Notes |
|-------|------|------|
| `payload_version` | `u16` | `3` |
| `collection_id` | `u32` | Must exist in catalog |
| `schema_version` | `u32` | Must equal catalog’s current version for that collection |
| `op` | `u8` | `1` = insert, `2` = replace, `3` = delete |
| `pk` | tagged primitive | Primary key remains a **single-segment top-level scalar** |
| `field_count` | `u32` | Number of **non-PK** schema fields |
| entries | repeated | For each non-PK schema field: `(field_path, value)` |

### `field_path` encoding

Each entry encodes the `FieldPath` explicitly:

| Field | Type | Notes |
|-------|------|------|
| `segment_count` | `u8` | Must be `>= 1` |
| segments | repeated | Each: `u16` byte length + UTF-8 bytes |

Constraints:

- `segment_count` must be non-zero.
- segment length must be non-zero.
- segments must be valid UTF-8.

### `value` encoding

`value` is encoded as `RowValue` according to the field’s schema `Type`, using the v2 type-driven codec semantics (see [`docs/07_record_encoding_v2.md`](../07_record_encoding_v2.md)).

## Decoding and replay rules

- The decoder validates `field_count` matches the number of non-PK fields in the schema.
- Each decoded `field_path` must match exactly one schema `FieldDef.path` (excluding PK).
- Duplicate `field_path` entries are rejected.
- The decoded row is reconstructed as a nested object tree:
  - `["a"]` inserts into top-level key `"a"`.
  - `["a","b","c"]` creates/merges objects so the leaf is stored under `row["a"]["b"]["c"]`.
- Last-write-wins by `(collection_id, pk)` still applies at the segment replay layer.

## When to emit v3

An engine may emit v3 when the collection’s schema includes any multi-segment `FieldDef.path`.

## Compatibility

Replay accepts **v1/v2/v3** payload versions in the same file. New writes should prefer:

- v2 when all schema field definitions are single-segment top-level paths
- v3 when the schema includes multi-segment `FieldPath`s

