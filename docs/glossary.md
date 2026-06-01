# Glossary

Quick definitions for ModelVault terms. For deeper detail, see [Core concepts](guides/concepts.md) and [Specifications](specs/index.md).

## Collection

A named container of records (similar to a table), with a schema, primary key, and optional secondary indexes.

## Schema

The contract for a collection: field paths, types, constraints, and indexes. Enforced on every write.

## Schema version

Monotonic version number for a collection’s catalog entry. Changes when fields, constraints, or indexes evolve.

## Field path

A list of string segments identifying a field — e.g. `["profile", "timezone"]`. Multi-segment paths are first-class in 1.0.

## Primary key

The field that identifies a record within a collection. Used for `get` and replace-by-key insert semantics. Must be a single-segment top-level scalar in 1.0.

## Index

Secondary lookup structure maintained and persisted by ModelVault. Supports equality lookups and query acceleration for supported predicate shapes. Kinds: **unique** and **non-unique**.

## Row / record

One validated document in a collection, keyed by primary key. Stored as append-only **record** segments; latest wins on read.

## Segment

An append-only, checksummed unit in the database file’s log (schema, record, index, transaction marker, checkpoint, …).

## Checkpoint

Published state marker allowing open/replay to start from a known-good point instead of replaying entire history.

## Superblock

Alternating A/B metadata regions for crash-safe publication of manifest and checkpoint pointers.

## Recovery mode

Behavior when integrity checks fail on open:

| Mode | Behavior |
|------|----------|
| **`AutoTruncate`** | Best-effort salvage — truncate torn tails to last committed prefix |
| **`Strict`** | Fail-fast if recovery would require truncation |

Read-only opens default to **`Strict`**.

## Lazy upgrade

Opening an older format minor and upgrading metadata or minor version as part of an operation that requires newer invariants — without a full-file rewrite.

## Subset model

A Rust struct or Python class declaring fewer fields than the full collection schema. A **read projection** — omits undeclared fields from materialized results.

## Replace-by-key

Insert semantics: writing a row with an existing primary key replaces the prior version (latest row wins on read).
