# Record encoding v2 (0.6.0)

Record payloads in `SegmentType::Record` use a **version prefix** (`u16` LE). Version **1** is primitive-only (0.5.x). Version **2** supports the full [`Type`](./typed_embedded_db_spec.md) tree aligned with the schema catalog.

## Primary key

The primary key field must remain a **primitive** [`Type`](./typed_embedded_db_spec.md) (`bool` through `timestamp`). It is encoded as in v1: a leading type tag (`u8`) and primitive payload (see [06_record_encoding_v1.md](06_record_encoding_v1.md)).

## Optional semantics (0.6.0)

- **Non-optional** fields: the key must be present in the row map; `null` / `None` is rejected for non-optional scalars.
- **`Optional<T>`**: the field may be **omitted** or present; when present, Python `None` / Rust `RowValue::None` denotes **absent** (same as omit). No distinction between SQL NULL and omitted for v1.

## Wire layout (version 2)

| Field | Type | Notes |
|-------|------|-------|
| `RECORD_PAYLOAD_VERSION` | `u16` | `2` |
| `collection_id` | `u32` | |
| `schema_version` | `u32` | Must match catalog |
| `op` | `u8` | `1` = insert |
| PK | tagged primitive | Same tags 0–7 as v1 |
| `field_count` | `u32` | Number of **non-PK** fields, **schema order** (excluding PK) |
| values | repeated | Each value encoded per field’s [`Type`](./typed_embedded_db_spec.md) (see below) |

### Value encoding (type-driven)

Primitives reuse v1 tags `0`–`7` (see [`crates/typra-core/src/record/scalar.rs`](../crates/typra-core/src/record/scalar.rs)).

- **`Optional(inner)`**: `u8` presence — `0` = absent; `1` = present, then `encode_row_value(inner)`.
- **`List(inner)`**: `u32` element count, then each element encoded as `inner`.
- **`Object(fields)`**: for each nested `FieldDef` in **declaration order**, encode that field’s value (required/optional follows nested type).
- **`Enum(variants)`**: UTF-8 string (tag `4` + length + bytes) whose content must be one of `variants`.

## Compatibility

Replay accepts **v1 and v2** segments in one file. New inserts emit **v2** (even for all-primitive rows).

## See also

- [06_record_encoding_v1.md](06_record_encoding_v1.md)
- [ROADMAP.md](../ROADMAP.md) — 0.6.0
