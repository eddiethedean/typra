# Record encoding v2

Canonical spec: [`docs/07_record_encoding_v2.md`](../07_record_encoding_v2.md).

## Relationship to v3

v2 encodes non-PK values in **schema order** and assumes collection schema field definitions are single-segment top-level paths. For multi-segment schema field definitions, see:

- [`docs/specs/record_encoding_v3.md`](record_encoding_v3.md)

