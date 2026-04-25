# Record encoding v1

Canonical spec: [`docs/06_record_encoding_v1.md`](../06_record_encoding_v1.md).

## Status

Record payload **v1** is retained for **read compatibility**. New writes should not emit v1 (see v2/v3).

## Versions

- v1: primitive-only, top-level schema fields (0.5.x)
- v2: composite values (`RowValue`) in schema order (0.6.0+)
- v3: **multi-segment schema field paths** encoded by full `FieldPath` (1.0.0+)

See:

- [`docs/specs/record_encoding_v2.md`](record_encoding_v2.md)
- [`docs/specs/record_encoding_v3.md`](record_encoding_v3.md)

