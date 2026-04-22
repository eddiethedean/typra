# Typed Embedded Database – On-Disk File Format Specification

## Goals
The on-disk format should be:
- crash-safe
- versioned
- self-describing enough for tooling
- compact
- append-friendly
- recoverable
- forward-evolvable

This spec assumes a **single database file** containing metadata, append-only record segments, index regions, and checkpoint data.

## High-Level Layout

```text
+-----------------------------+
| File Header                 |
+-----------------------------+
| Superblock A                |
+-----------------------------+
| Superblock B                |
+-----------------------------+
| Schema Catalog Segments     |
+-----------------------------+
| Record Log Segments         |
+-----------------------------+
| Index Segments              |
+-----------------------------+
| Checkpoint / Manifest Area  |
+-----------------------------+
| Free / Reserved Space       |
+-----------------------------+
```

Use alternating superblocks so metadata updates are crash-safe.

## File Header
Fixed-size header, e.g. 4 KB.

### Fields
- magic bytes: `TDB0`
- file format major/minor
- engine version compatibility info
- page/segment size
- UUID for database file
- creation timestamp
- checksum algorithm identifier
- flags
- offsets to active superblock candidates

Example:
```text
struct FileHeader {
    magic: [u8; 4],
    format_major: u16,
    format_minor: u16,
    header_size: u32,
    segment_size: u32,
    db_uuid: [u8; 16],
    created_at_unix_micros: u64,
    checksum_kind: u8,
    flags: u64,
    superblock_a_offset: u64,
    superblock_b_offset: u64,
    reserved: [u8; ...],
    checksum: u64
}
```

## Superblock
A small metadata structure pointing to the current logical database roots.

Two copies:
- Superblock A
- Superblock B

Writer alternates between them on checkpoint / metadata update.

### Fields
- monotonic generation number
- last committed transaction ID
- last durable log sequence number
- active schema catalog root
- active manifest root
- optional index manifest root
- free-space map root or free segment list root
- checksum

On open:
1. read both superblocks
2. verify checksums
3. choose highest valid generation

## Segment Model
The file is organized around **segments**, not fixed B-tree pages.

Each segment may store:
- schema metadata entries
- record events
- index entries
- checkpoints
- free space maps

This better fits append-only storage.

### Segment Header
Fields:
- segment type
- segment ID
- schema/catalog epoch
- start LSN
- end LSN
- payload length
- compression kind
- checksum

Segment types:
- `SCHEMA`
- `RECORD`
- `INDEX`
- `CHECKPOINT`
- `MANIFEST`
- `FREE`
- `COMPACTED`

## Log Sequence Numbers
Every committed mutation gets a monotonically increasing **LSN** or transaction sequence.

Used for:
- snapshot visibility
- recovery
- compaction watermarks
- index rebuild coordination

## Schema Catalog Storage
Catalog entries should be append-only records too.

### Catalog Entry Types
- create collection
- replace schema version
- add index
- drop index
- migration marker
- validator registration metadata

A schema version is immutable once written. New versions supersede old ones.

### Example Schema Record
```json
{
  "kind": "schema_version",
  "collection_id": 7,
  "version": 3,
  "fields": [...],
  "indexes": [...],
  "compatibility": {...}
}
```

## Record Log Encoding

**Implemented layout (v1)** for `SegmentType::Record` payloads is specified in **[06_record_encoding_v1.md](06_record_encoding_v1.md)** (Typra **0.5.x**).

Each logical record mutation is an event:
- `insert`
- `replace`
- `delete`

### Record Event Header
- collection ID
- schema version ID
- transaction ID
- record primary key hash / encoded PK
- event kind
- payload length
- checksum

### Payload
Prefer a compact binary encoding:
- length-prefixed field values
- object field table
- schema-aware binary layout
- optional dictionary/shared string encoding later

Possible encodings:
1. custom schema-aware binary format
2. MessagePack-like encoding
3. CBOR-like encoding
4. Cap’n Proto / FlatBuffers style structure

Recommended:
- start with a **custom schema-aware binary format**
- use schema catalog metadata for interpretation
- avoid storing repeated field names per record

## Record Encoding Strategy
Given schema:

```text
User {
  id: uuid,
  email: string,
  age: optional<u16>,
  tags: list<string>,
  profile: object{ display_name: string, timezone: string }
}
```

Encoded record payload may contain:
- null bitmap for nullable fields
- presence bitmap for optional/defaulted fields
- fixed-width scalar area
- variable-width offset table
- variable blob region for strings, lists, nested objects

Nested object encoding can recurse using the same rules.

## Delete/Tombstone Format
Deletes write a tombstone event:
- collection ID
- PK
- transaction ID
- delete marker
- optional reason or metadata reserved field

Readers resolve latest visible version. Tombstone wins over older inserts/replaces.

## Transactions On Disk
A transaction appends:
1. mutation events
2. transaction commit record

Commit record contains:
- transaction ID
- commit timestamp
- count of mutation records
- start/end offsets or LSN bounds
- checksum

Only transactions with valid commit records are visible after recovery.

## Recovery Rules
On open:
1. choose active superblock
2. scan manifests and recent record segments
3. locate last durable commit
4. ignore partial trailing writes without valid commit record
5. reconcile indexes if index manifest lags
6. open database at latest consistent snapshot

## Index Persistence
Indexes may be persisted in dedicated segments.
For v1, consider persisting them and allowing rebuild if damaged or stale.

Index entry includes:
- collection ID
- index ID
- encoded key
- referenced PK / record pointer
- visibility / LSN metadata if needed

### Index Types
- primary key: exact lookup
- unique key: exact lookup + conflict detection
- scalar field index
- nested path index

Recommended internal structure:
- ordered key blocks or B+tree-like mini-structures per index segment
- append delta updates, periodically compacted into new index snapshots

## Checkpoints
A checkpoint is a durable snapshot of roots:
- current superblock generation
- schema catalog root
- record compaction watermark
- index roots
- last committed transaction ID

Checkpointing reduces recovery scan time.

## Compaction
Compaction rewrites live state into fresh segments.

### Compaction outputs
- new record segments containing only latest live versions at target watermark
- rebuilt index segments
- new manifest / checkpoint
- old segments marked reclaimable

Compaction should be atomic by publishing a new manifest/superblock after all outputs are durable.

## Manifest Area
Manifest tracks:
- active segment list
- inactive/reclaimable segment list
- root segment IDs per subsystem
- latest safe compaction point

Manifest is itself append-only and periodically checkpointed.

## Free Space Management
Simplest v1 approach:
- append until threshold reached
- compaction creates replacement file or large rewritten region
- optionally swap atomically

Alternative:
- maintain free segment list and reuse dead segments

Recommended v1:
- support **copy-compaction to fresh file then atomic replace**
- simpler correctness story than in-place hole reuse

## Checksums
Use checksums on:
- file header
- superblocks
- each segment header + payload
- each transaction commit record

Recommended checksum:
- CRC32C for speed
- optional stronger hash in tooling / verification mode

## Compression
Optional per-segment compression:
- none
- zstd

Compression is easier at segment granularity.

## Format Versioning
Rules:
- major version bump for incompatible changes
- minor version bump for additive/compatible changes
- feature flags for optional capabilities

Tooling should expose:
- file format version
- engine compatibility range
- upgrade requirement hints

## Suggested Binary Encodings by Subsystem
- metadata: compact tagged binary records
- schema: versioned metadata structs
- records: schema-aware binary layout
- indexes: key block format

## Minimal Viable Format Summary
1. Header with magic/version.
2. Dual superblocks.
3. Append-only record and schema segments.
4. Commit markers for transactions.
5. Persisted indexes with rebuild option.
6. Checkpoint/manifest for recovery speed.
7. Copy-compaction path for simplification.

## Future Enhancements
- encryption at rest
- bloom filters on segments
- record-level compression
- page cache hints
- partial segment repair tooling
- shadow paging for selected metadata
