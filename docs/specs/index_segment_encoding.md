# Index segment encoding

**Audience:** advanced

`SegmentType::Index` segment payloads carry **secondary index deltas** (insert/delete of index keys). Replay applies them to rebuild in-memory `IndexState`.

Implementation: `crates/modelvault-core/src/index.rs`.

## Index payload layout

| Field | Type | Notes |
|-------|------|-------|
| `payload_version` | `u16` | **1** or **2** (new writes use **2**) |
| `entry_count` | `u32` | |
| `entries` | repeated | See below |

### Index entry

| Field | Type | Notes |
|-------|------|-------|
| `collection_id` | `u32` | |
| `kind` | `u8` | `1` = unique, `2` = non-unique |
| `op` | `u8` | v2 only: `1` = insert, `2` = delete; v1 implies insert |
| `index_name` | string | `u32` len + UTF-8 (non-empty) |
| `index_key` | bytes | `u32` len + canonical key bytes |
| `pk_key` | bytes | `u32` len + canonical primary-key bytes |

**v1:** no `op` field; all entries are inserts.

**v2:** explicit insert/delete for replace-by-PK and delete operations.

### Canonical key bytes

Primary keys and index keys use scalar canonical encoding for lookups (see `ScalarValue::canonical_key_bytes` in `record/scalar.rs`).

## Replay

- Index segments are applied **after** schema replay and **before** record replay (or loaded from a checkpoint index blob).
- **Unique** indexes: at most one `pk_key` per `index_key`; conflicts raise errors on insert.
- **Non-unique** indexes: `index_key` maps to a set of `pk_key` values.

## See also

- [Catalog encoding](catalog_encoding.md) — index definitions in catalog v4
- [On-disk file format](on_disk_file_format.md)
- [Types matrix](../reference/types.md) — index path rules
