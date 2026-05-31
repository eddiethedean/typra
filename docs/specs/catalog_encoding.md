# Catalog encoding (Schema segments)

**Audience:** advanced

`SegmentType::Schema` segment payloads carry the **schema catalog**: collection creation and schema version bumps. Payloads are length-prefixed catalog blobs inside checkpoint snapshots and schema segments.

Implementation: `crates/typra-core/src/catalog/codec.rs`.

## Catalog payload wrapper

Every catalog blob begins with:

| Field | Type | Notes |
|-------|------|-------|
| `catalog_payload_version` | `u16` | See versions below |
| `entry_kind` | `u16` | `1` = create collection, `2` = new schema version |

### Catalog payload versions

| Version | Features |
|---------|----------|
| **1** | Create / new version; fields without constraints or indexes |
| **2** | + `primary_field` on create |
| **3** | + per-field `constraints` |
| **4** | + `indexes` on create and new version (**current write version**) |

Readers accept v1–v4. New writes use **v4**.

## Entry: CreateCollection (`entry_kind = 1`)

| Field | Type | Notes |
|-------|------|-------|
| `collection_id` | `u32` | |
| `name` | string | `u32` byte len + UTF-8 (1..1023 bytes) |
| `schema_version` | `u32` | Initial version (typically **1**) |
| `fields` | field list | See [Field list](#field-list) |
| `indexes` | index list | v4 only; see [Index list (v4)](#index-list-v4) |
| `primary_field` | optional string | v2+: `u32` len (0 = none) + UTF-8 name |

Legacy v1 catalog segments omit `primary_field`; those collections cannot accept inserts until re-registered.

## Entry: NewSchemaVersion (`entry_kind = 2`)

| Field | Type | Notes |
|-------|------|-------|
| `collection_id` | `u32` | |
| `schema_version` | `u32` | New monotonic version |
| `fields` | field list | Full field set for this version |
| `indexes` | index list | v4 only; see [Index list (v4)](#index-list-v4) |

## Field list

| Field | Type |
|-------|------|
| `count` | `u32` |
| `fields` | repeated `count` times |

Each field:

| Field | Type | Notes |
|-------|------|-------|
| `path` | [FieldPath](#fieldpath-catalog) | |
| `type` | [Type tree](#type-tree) | |
| `constraints` | constraint list | v3+ only |

### FieldPath (catalog)

| Field | Type |
|-------|------|
| `segment_count` | `u32` |
| `segments` | repeated: `u32` len + UTF-8 (non-empty segments) |

Catalog paths must be non-empty, unique, and free of parent/child conflicts (see [Schema DSL](schema_dsl.md)).

### Type tree

Leading `u8` tag, then tag-specific data:

| Tag | Type |
|-----|------|
| 0 | `bool` |
| 1 | `int64` |
| 2 | `uint64` |
| 3 | `float64` |
| 4 | `string` |
| 5 | `bytes` |
| 6 | `uuid` |
| 7 | `timestamp` |
| 8 | `optional` | nested type (depth limit 32) |
| 9 | `list` | nested element type |
| 10 | `object` | `u32` field count, then nested fields (path + type each) |
| 11 | `enum` | `u32` variant count, then variant strings |

### Constraint list (v3+)

| Field | Type |
|-------|------|
| `count` | `u32` |
| `constraints` | repeated |

Each constraint: `u8` tag + payload:

| Tag | Constraint | Payload |
|-----|------------|---------|
| 1 | `MinI64` | `i64` |
| 2 | `MaxI64` | `i64` |
| 3 | `MinU64` | `u64` |
| 4 | `MaxU64` | `u64` |
| 5 | `MinF64` | `u64` (f64 bits) |
| 6 | `MaxF64` | `u64` (f64 bits) |
| 7 | `MinLength` | `u64` |
| 8 | `MaxLength` | `u64` |
| 9 | `Regex` | `u32` len + UTF-8 pattern |
| 10 | `Email` | (none) |
| 11 | `Url` | (none) |
| 12 | `NonEmpty` | (none) |

## Index list (v4)

| Field | Type |
|-------|------|
| `count` | `u32` |
| `indexes` | repeated |

Each index:

| Field | Type | Notes |
|-------|------|-------|
| `kind` | `u8` | `1` = unique, `2` = non-unique |
| `path` | FieldPath | Indexed scalar path |
| `name` | string | `u32` len + UTF-8 (non-empty) |

## Replay

Schema segments are replayed in file order to build the in-memory `Catalog`. Later schema version entries supersede earlier field sets for the same `collection_id`.

## See also

- [Schema DSL](schema_dsl.md) — path invariants and product model
- [On-disk file format](on_disk_file_format.md)
- [Index segment encoding](index_segment_encoding.md)
