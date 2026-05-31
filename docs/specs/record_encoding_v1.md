# Record encoding v1

**Status:** read compatibility only. New writes emit [v2](record_encoding_v2.md) or [v3](record_encoding_v3.md).

Record payloads live in `SegmentType::Record` segment bodies (after the [segment header](on_disk_file_format.md#segment-framing)). All integers are **little-endian**.

## Wire layout

| Field | Type | Notes |
|-------|------|-------|
| `payload_version` | `u16` | **1** |
| `collection_id` | `u32` | Must exist in catalog |
| `schema_version` | `u32` | Must equal catalog’s current version for that collection |
| `op` | `u8` | `1` = insert; `2` = replace; `3` = delete |
| `pk` | [tagged scalar](#tagged-scalar-primitives) | Must match primary key field type (primitives only) |
| `field_count` | `u32` | Number of **non-PK** fields |
| `values` | repeated | `field_count` tagged scalars in **schema order** (excluding PK) |

For `op = 3` (delete), `field_count` must be **0** on decode.

## Tagged scalar primitives

Each scalar is a `u8` type tag followed by payload bytes:

| Tag | Type | Payload |
|-----|------|---------|
| 0 | `bool` | `u8` (0 or 1) |
| 1 | `int64` | `i64` |
| 2 | `uint64` | `u64` |
| 3 | `float64` | `f64` IEEE bits |
| 4 | `string` | `u32` len + UTF-8 bytes |
| 5 | `bytes` | `u32` len + raw bytes |
| 6 | `uuid` | 16 bytes |
| 7 | `timestamp` | `i64` Unix microseconds |

Implementation: `crates/typra-core/src/record/scalar.rs`.

## Replay rules

- Segments are applied in **on-disk order**.
- **Last write wins** per `(collection_id, pk)` at the record replay layer.
- Wrong `schema_version`, unknown `op`, truncated payload, or type mismatch → format/schema error (no silent accept).

## See also

- [Record encoding v2](record_encoding_v2.md) — composite `RowValue` values
- [Record encoding v3](record_encoding_v3.md) — multi-segment `FieldPath`s
- [Catalog encoding](catalog_encoding.md) — primary key declaration
