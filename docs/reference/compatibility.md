# Compatibility matrix

This document describes **read/write compatibility** for Typra database files and **stability expectations** for public APIs.

Typra is still **pre-1.0**: minor versions (`0.x`) may include breaking changes, but we aim to keep file-format evolution explicit and well-tested.

## File-format compatibility

Typra database files have a **format major** and **format minor** (see [On-disk file format spec](../specs/on_disk_file_format.md)).

- **Format major (`FORMAT_MAJOR`)**: breaking changes. Typra will refuse to open unknown majors.
- **Format minor (`FORMAT_MINOR`)**: compatible evolution within a major. Minors may gate new segment types, replay semantics, or publication metadata.

### Compatibility terms

- **Read**: the file can be opened and queried (subject to `OpenOptions.recovery` / `RecoveryMode`).
- **Write**: the file can be opened *and* the engine will append new durable segments to it.
- **Lazy upgrade**: the engine may update metadata or the file’s format minor as part of an operation that requires newer invariants.
  - This is not a whole-file rewrite; it may include publishing newer metadata and/or appending newer segment types.

### Supported format minors

| Minor | Read | Write | Notes |
|------|------|-------|------|
| **6** | ✅ | ✅ | Current for new databases. Transaction framing (`TxnBegin/Commit/Abort`) and strict replay rules. |
| **≤ 5** | ✅ | ⚠️ | Read supported. New writes may lazily upgrade file header/minor when required by newer semantics. |

### Upgrade and write behavior (policy)

- **Existing files** are opened without rewriting whenever possible.
- Typra prefers **preserving the file’s current minor** until an operation requires newer invariants.
- When a **lazy upgrade** happens, Typra will make the post-upgrade behavior explicit in release notes.

#### Practical rules by minor

- **Minor 6**
  - **Writes** are fully supported.
  - **Recovery** honors transaction framing. Tail corruption/incomplete txn tails are handled according to `RecoveryMode`.
- **Minor ≤ 5**
  - **Reads** are supported.
  - **Writes are best-effort**: some write paths require **minor 6** invariants (notably around atomic multi-write durability).
    - In those cases Typra may **lazily upgrade** the file to minor 6 before/while appending new durable state.

### Recovery modes (contract)

Recovery behavior is controlled by `OpenOptions.recovery` / `RecoveryMode`.

- **`AutoTruncate`** (default)
  - Open succeeds if a valid committed prefix can be identified.
  - Torn tails / incomplete transaction tails may be **truncated** back to the last known-good committed state.
  - If a checkpoint is corrupt, the engine should fall back to replaying from an earlier safe point (e.g. full replay) rather than producing silently-wrong results.
- **`Strict`**
  - Open refuses if integrity checks fail for required metadata, or if recovery would require truncation.
  - Intended for environments that prefer fail-fast over best-effort salvage.

### Forward compatibility (contract)

- Unknown **format majors** are refused.
- Unknown **format minors** within a known major are refused unless explicitly handled by the compatibility logic for that release line.
- Unknown **segment types** are refused by default, unless explicitly declared ignorable/ephemeral by the format spec for that major/minor.

Typra prefers **explicit compatibility** over “best guess” parsing.

## Segment types and stability

Typra’s on-disk log is append-only segments with checksums.

- **Stable/persistent segments** (part of the durable logical state): `Schema`, `Record`, `Index`, `Manifest`, `TxnBegin`, `TxnCommit`, `TxnAbort`, `Checkpoint`.
- **Ephemeral segments**: `Temp` is **scratch spill storage** for bounded-memory operators.
  - It is **ignored by replay** and must not affect durable state after reopen.
  - It may be truncated/cleaned opportunistically (including at the end of an operator).

Checkpoint payloads are validated **when used**; corrupt checkpoint bytes should not prevent opening in `AutoTruncate` mode.

## Crate / package API stability

### Rust crates

- **`typra`** (facade): preferred dependency for applications.
  - **Stability goal**: strongest compatibility guarantees in the Rust ecosystem for Typra.
  - **Policy**: breaking changes should be rare and clearly called out, even pre-1.0.
- **`typra-core`** (engine): lower-level APIs and internal types.
  - **Stability goal**: stable enough for power users, but expect more churn than `typra` before 1.0.
  - **Policy**: internal refactors are acceptable as long as `typra` remains stable and behavior is preserved.
- **`typra-derive`**: proc macro for `#[derive(DbModel)]`.
  - **Stability goal**: mostly additive improvements (new attributes and validations) with minimal breakage.

### Python package (`typra` on PyPI)

- The Python surface mirrors the engine where feasible.
- DB-API (`typra.dbapi`) is a read-only subset of PEP 249 for a minimal `SELECT` grammar (see [Python guide](../guides/python.md)).

## DB-API + SQL subset guarantees (current)

- Supported: `SELECT \<cols|*\> FROM \<collection\>` with optional `WHERE` (=`?` params; `AND`/`OR` and ranges), optional `ORDER BY`, optional `LIMIT`.
- Not supported: DDL/DML SQL, joins, group-by SQL, SQLAlchemy dialect.
- Cursor behavior: `fetchone`/`fetchmany`/`fetchall` retrieve results incrementally (no forced full materialization on `execute`).

