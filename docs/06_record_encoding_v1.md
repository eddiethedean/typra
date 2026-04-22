# Record encoding v1 (0.5.x)

This document specifies **record payloads** in `SegmentType::Record` segments and how they interact with the **schema catalog** and **file format minor 5**.

## Primary key (catalog)

- Each collection has **at most one** primary key, declared at **create** time as the **top-level** field name (single path segment).
- Catalog payload **v2** adds `primary_field` after the field list on `CreateCollection`. Catalog **v1** segments (0.4.x) omit it; those collections load with **`primary_field = none`** and **cannot accept inserts** until re-registered in a future migration (or new collection).
- **0.5.0** `register_collection` requires a non-empty `primary_field` that matches exactly one **top-level** `FieldDef` path segment.

## File format minor

- **`FORMAT_MINOR = 5`** for databases that write records. New databases created under 0.5 use **0.5** headers.
- Existing **0.4** files are upgraded **lazily** to **0.5** on first record write (same pattern as 0.3→0.4 for catalog).

## Record segment payload (v1)

All integers are **little-endian**. Payload is the segment body (after the segment header).

| Field | Type | Notes |
|-------|------|--------|
| `RECORD_PAYLOAD_VERSION` | `u16` | `1` |
| `collection_id` | `u32` | Must exist in catalog |
| `schema_version` | `u32` | Must equal catalog’s current version for that collection |
| `op` | `u8` | `1` = insert; `2`/`3` reserved for replace/delete (not emitted in 0.5.0) |
| PK | tagged value | Type matches the primary key field’s `Type` (primitives only in v1) |
| `field_count` | `u32` | Number of **non-PK** fields, in **schema order** (excluding the PK column) |
| values | repeated | Each value tagged per field type, same order as non-PK fields in schema |

**Latest row**: when replaying record segments in **on-disk order**, the last insert for a given `(collection_id, pk)` wins.

**Decode errors**: wrong `schema_version`, unknown `op`, truncated payload, or type mismatch → `FormatError` / `SchemaError` (no silent acceptance).

## Stability

Pre-1.0: record payload version and catalog v2 may evolve; minor bumps should preserve read paths for older segments where feasible.

## See also

- [02_on_disk_file_format.md](02_on_disk_file_format.md) — segment framing
- [07_record_encoding_v2.md](07_record_encoding_v2.md) — composite row values (0.6.0+)
- [ROADMAP.md](../ROADMAP.md) — milestone 0.5.0 (delivered); follow-ups in 0.6+
