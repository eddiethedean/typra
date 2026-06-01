# On-disk file format (implemented)

**Audience:** advanced (engine contributors, alternate implementations, format tooling)

This document is the **published wire specification** for ModelVault **1.0.x** `.modelvault` files. It describes what the engine writes and replays today.

| Document | Role |
|----------|------|
| **This page** | File layout, segment framing, manifest/superblock/txn/checkpoint |
| [Compatibility](../reference/compatibility.md) | **1.x backwards-compat pledge**, read/write policy, recovery |
| [Format evolution (1.x)](format_evolution.md) | What 1.y may change without a major bump |
| [Catalog encoding](catalog_encoding.md) | `SegmentType::Schema` payloads |
| [Index segment encoding](index_segment_encoding.md) | `SegmentType::Index` payloads |
| [Record encoding v1–v3](record_encoding_v1.md) | `SegmentType::Record` payloads |

**Source of truth in code:** `crates/modelvault-core/src/file_format.rs`, `superblock.rs`, `segments/header.rs`, `manifest.rs`, `txn.rs`, `checkpoint.rs`, `publish.rs`.

**Historical design notes** (not normative): [On-disk format (long form) on GitHub](https://github.com/eddiethedean/modelvault/blob/main/docs/02_on_disk_file_format.md).

## Design goals

- **Single file**, append-friendly segment log
- **Crash-safe** metadata via dual superblocks and transaction framing (format minor **6**)
- **Checksums** on segment headers and payloads (CRC32C)
- **Schema-aware** record bodies (see record encoding specs)

## Fixed file layout

All offsets are **byte offsets** from the start of the file. Integers are **little-endian** unless noted.

```text
Offset (hex)   Size        Contents
────────────   ────        ────────
0x0000         32 B        File header (`TDB0`)
0x0020         4096 B      Superblock A (`TSB0`)
0x1020         4096 B      Superblock B (`TSB0`)
0x2020         …           Append-only segment log (headers + payloads)
```

Constants:

| Symbol | Value |
|--------|-------|
| `FILE_HEADER_SIZE` | 32 |
| `SUPERBLOCK_SIZE` | 4096 |
| `SEGMENT_HEADER_LEN` | 32 |
| `segment_start` | `FILE_HEADER_SIZE + 2 × SUPERBLOCK_SIZE` = **8224** (0x2020) |

New databases are created with **format minor 6** (transaction-framed writes). See [Compatibility](../reference/compatibility.md) for minor ≤ 5 behavior.

## File header (32 bytes)

Magic **`TDB0`** (`0x54 0x44 0x42 0x30`).

| Offset | Size | Field |
|--------|------|-------|
| 0 | 4 | `magic` = `TDB0` |
| 4 | 2 | `format_major` (`u16`, currently **0**) |
| 6 | 2 | `format_minor` (`u16`, **6** for new DBs) |
| 8 | 4 | `header_size` (`u32`, **32**) |
| 12 | 8 | `flags` (`u64`, **0**) |
| 20 | 12 | reserved (zeros) |

Unknown **format majors** are refused. Supported minors are defined in [Compatibility](../reference/compatibility.md).

## Superblock (4096 bytes each)

Magic **`TSB0`**. Two copies at offsets **32** and **4128**. On publish, the writer alternates to the **non-active** copy and bumps `generation`.

### Superblock v1 (current write version)

| Offset | Size | Field |
|--------|------|-------|
| 0 | 4 | `magic` = `TSB0` |
| 4 | 2 | `version` (`u16`, **1**) |
| 6 | 2 | reserved |
| 8 | 8 | `generation` (`u64`, monotonic) |
| 16 | 8 | `manifest_offset` (`u64`, byte offset of latest manifest segment) |
| 24 | 4 | `manifest_len` (`u32`, manifest **payload** length only) |
| 28 | 1 | `checksum_kind` (**1** = CRC32C) |
| 29 | 7 | reserved |
| 36 | 8 | `checkpoint_offset` (`u64`, 0 if none) |
| 44 | 4 | `checkpoint_len` (`u32`, checkpoint **payload** length only) |
| 48 | 4 | CRC32C over bytes **0..48** |

**Open rule:** decode both superblocks; if both valid, choose the higher `generation`; if one fails checksum, use the other.

Superblock **v0** (legacy read): no checkpoint fields; CRC over bytes **0..32**.

## Segment framing

The log from `segment_start` onward is a sequence of:

```text
[ SegmentHeader (32 B) ][ payload (payload_len B) ]
```

### Segment header (32 bytes)

Magic **`TSG0`**. Written by `SegmentWriter::append`; `payload_len` and `payload_crc32c` are computed from the payload body.

| Offset | Size | Field |
|--------|------|-------|
| 0 | 4 | `magic` = `TSG0` |
| 4 | 2 | `version` (`u16`, **0**) |
| 6 | 2 | `segment_type` (`u16`, see table below) |
| 8 | 4 | `header_len` (`u32`, **32**) |
| 12 | 8 | `payload_len` (`u64`) |
| 20 | 4 | `payload_crc32c` (`u32`, CRC32C of payload bytes) |
| 24 | 1 | `checksum_kind` (**1** = CRC32C) |
| 25 | 3 | reserved |
| 28 | 4 | CRC32C over bytes **0..28** |

**Payload verification:** on replay scan, payload CRC must match **except** for `Checkpoint` and `Temp` segment types (checkpoint validation happens when the checkpoint body is decoded).

### Segment types

| `segment_type` | Name | Durable | Payload spec |
|----------------|------|---------|----------------|
| 1 | `Schema` | yes | [Catalog encoding](catalog_encoding.md) |
| 2 | `Record` | yes | [Record v1](record_encoding_v1.md) / [v2](record_encoding_v2.md) / [v3](record_encoding_v3.md) |
| 3 | `Manifest` | yes | [Manifest v0](#manifest-v0) (below) |
| 4 | `Checkpoint` | yes | [Checkpoint v0](#checkpoint-v0) (below) |
| 5 | `Index` | yes | [Index segment encoding](index_segment_encoding.md) |
| 6 | `TxnBegin` | yes | [Transaction marker](#transaction-markers-txnpayloadv0) |
| 7 | `TxnCommit` | yes | [Transaction marker](#transaction-markers-txnpayloadv0) |
| 8 | `TxnAbort` | yes | [Transaction marker](#transaction-markers-txnpayloadv0) |
| 9 | `Temp` | **no** | Ephemeral spill; **ignored by replay** |

Unknown segment types are refused unless a future compatibility doc explicitly allows them.

## Manifest v0

Payload inside a `SegmentType::Manifest` segment (18 bytes):

| Field | Type | Notes |
|-------|------|-------|
| `version` | `u16` | **0** |
| `last_segment_offset` | `u64` | Start offset of this manifest segment |
| `last_segment_len` | `u64` | `SEGMENT_HEADER_LEN + payload_len` for this manifest |

Publishing appends a new manifest segment and updates the active superblock to point at it. The manifest is a **tail pointer** for the committed segment chain.

## Transaction markers (`TxnPayloadV0`)

Format minor **6+** wraps durable writes:

```text
TxnBegin(txn_id) → [ Schema | Record | Index … ] → TxnCommit(txn_id)
```

All three marker segment types use the same **24-byte** payload:

| Offset | Size | Field |
|--------|------|-------|
| 0 | 2 | `payload_version` (`u16`, **0**) |
| 2 | 8 | `txn_id` (`u64`) |
| 10 | 8 | `reserved` (zeros) |
| 18 | 4 | CRC32C over bytes **0..18** |
| 22 | 2 | padding (zeros) |

**Visibility:** only segments strictly between a matching `TxnBegin` and `TxnCommit` with the same `txn_id` are replayed. An uncommitted tail after `TxnBegin` is dropped in `AutoTruncate` recovery (see [Compatibility](../reference/compatibility.md)).

`TxnAbort` discards staged segments since the matching begin without committing them.

## Checkpoint v0

Optional acceleration snapshot in a `SegmentType::Checkpoint` segment.

| Field | Type | Notes |
|-------|------|-------|
| `version` | `u16` | **0** |
| `replay_from_offset` | `u64` | Replay log segments from this offset after applying snapshot |
| `catalog_count` | `u32` | Repeated: `u32 len` + catalog payload bytes |
| `record_count` | `u32` | Repeated: `u32 len` + record payload bytes (v1/v2/v3 bodies) |
| `index_blob_len` | `u32` | Length of following index payload |
| `index_blob` | bytes | [Index segment encoding](index_segment_encoding.md) body |

If checkpoint bytes are corrupt, open in `AutoTruncate` mode falls back to full replay rather than returning wrong data.

## Open and replay order

1. Read file header; validate magic and supported minor.
2. Select active superblock; read manifest (and optional checkpoint).
3. **Recovery scan** from `segment_start` (or checkpoint `replay_from_offset`):
   - Truncate torn tail / incomplete txn per `RecoveryMode`.
4. **Replay** committed segments in file order:
   - Apply `Schema` → build catalog
   - Apply `Index` → rebuild `IndexState`
   - Apply `Record` → last-write-wins per `(collection_id, pk)`
5. Serve queries from materialized in-memory state.

Record replay uses **last event wins** for each primary key. Delete operations (`op = 3`) remove the row.

## Checksums

CRC32C (`checksum_kind = 1`) is used for:

- Segment header (bytes 0..28)
- Segment payloads (stored in header as `payload_crc32c`)
- Superblock v1 (bytes 0..48)
- Transaction marker payload (bytes 0..18)

## What this spec does not cover

- Compaction output layout (same segment types; policy in engine)
- `Temp` spill record format (non-durable)
- Future compression or encryption flags

See [Compatibility](../reference/compatibility.md) for API and file-format stability promises.
