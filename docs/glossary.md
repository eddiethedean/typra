# Glossary

## Collection

A named container of records (similar to a table), with a schema and a primary key.

## Schema

The contract for a collection: field paths, types, and constraints. Enforced on write.

## Field path

A list of string segments that identifies a field, e.g. `["profile", "timezone"]`.

## Primary key

The field designated as the collection’s identity. Used for `get` and replace-by-key semantics.

## Index

Secondary lookup structure maintained and persisted by Typra. Supports equality lookups and query acceleration for supported predicate shapes.

## Segment

An append-only, checksummed record in the database file’s log.

## Checkpoint

A published state that allows open/replay to start from a known-good point rather than replaying the entire history.

## Recovery mode

Open behavior when integrity checks fail:

- `AutoTruncate`: best-effort salvage by truncating torn tails.
- `Strict`: fail-fast if recovery would require truncation.

