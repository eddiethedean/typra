# On-disk file format

Canonical spec: [`docs/02_on_disk_file_format.md`](../02_on_disk_file_format.md).

## Record payload versions (implemented)

Typra replays record segments using a versioned payload prefix (`u16` LE):

- v1: primitives-only (0.5.x) — read compatibility
- v2: composite `RowValue` encoding (0.6.0+) — default for single-segment schema field defs
- v3: encodes non-PK values keyed by full `FieldPath` (1.0.0+) — required for multi-segment schema field defs

See:

- [`docs/specs/record_encoding_v1.md`](record_encoding_v1.md)
- [`docs/specs/record_encoding_v2.md`](record_encoding_v2.md)
- [`docs/specs/record_encoding_v3.md`](record_encoding_v3.md)

