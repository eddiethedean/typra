# Record encoding v2

**Status:** default for new inserts when all schema field definitions are **single-segment top-level** paths. Use [v3](record_encoding_v3.md) when the schema includes multi-segment `FieldPath`s.

Record payloads live in `SegmentType::Record` segment bodies. All integers are **little-endian**.

## Wire layout

| Field | Type | Notes |
|-------|------|-------|
| `payload_version` | `u16` | **2** |
| `collection_id` | `u32` | |
| `schema_version` | `u32` | Must match catalog |
| `op` | `u8` | `1` = insert, `2` = replace, `3` = delete |
| `pk` | tagged primitive | Same tags as [v1](record_encoding_v1.md#tagged-scalar-primitives) |
| `field_count` | `u32` | Non-PK fields in **schema order** (excluding PK) |
| `values` | repeated | Each value: [RowValue encoding](#rowvalue-encoding) per field `Type` |

For `op = 3`, `field_count` must be **0**.

## Primary key

The primary key remains a **primitive** type (`bool` through `timestamp`), encoded as a tagged scalar (v1 tags 0–7).

## Optional semantics

- **Non-optional** fields: must be present; `RowValue::None` is rejected for non-optional scalars.
- **`Optional<T>`**: field may be omitted from the row map, or encoded as absent:
  - `u8` **0** = absent (no following bytes)
  - `u8` **1** = present, then `RowValue` for `T`

There is no SQL `NULL` vs omitted distinction in v2.

## RowValue encoding

Type-driven recursive encoding (`encode_row_value` / `decode_row_value`):

### Primitives

Same [tagged scalar](record_encoding_v1.md#tagged-scalar-primitives) layout as v1 (tags 0–7).

### `Optional(inner)`

| Field | Type |
|-------|------|
| `presence` | `u8` (0 = absent, 1 = present) |
| `value` | present only: RowValue for `inner` |

### `List(inner)`

| Field | Type |
|-------|------|
| `count` | `u32` |
| `elements` | `count` × RowValue for `inner` |

### `Object(nested fields)`

For each nested `FieldDef` in **declaration order**:

| Field | Type |
|-------|------|
| `value` | RowValue for that field’s `Type` |

Nested object keys are the **first path segment** of each nested `FieldDef` (top-level keys inside the object map).

### `Enum(variants)`

Encoded as a **string** tagged scalar (tag 4 + UTF-8 bytes). Content must be one of `variants`.

Implementation: `crates/modelvault-core/src/record/row_value.rs`.

## Compatibility

Replay accepts **v1, v2, and v3** in one file. The decoder branches on the leading `u16` payload version.

## See also

- [Record encoding v1](record_encoding_v1.md)
- [Record encoding v3](record_encoding_v3.md)
- [Types matrix](../reference/types.md)
