# Typra Compatibility Matrix

This document describes **read/write compatibility** for Typra database files and **stability expectations** for public APIs.

Typra is still **pre-1.0**: minor versions (`0.x`) may include breaking changes, but we aim to keep file-format evolution explicit and well-tested.

## File-format compatibility

Typra database files have a **format major** and **format minor** (see [`docs/02_on_disk_file_format.md`](02_on_disk_file_format.md)).

- **Format major (`FORMAT_MAJOR`)**: breaking changes. Typra will refuse to open unknown majors.
- **Format minor (`FORMAT_MINOR`)**: compatible evolution within a major. Minors may gate new segment types, replay semantics, or publication metadata.

### Supported format minors

| Minor | Read | Write | Notes |
|------|------|-------|------|
| **6** | ✅ | ✅ | Current for new databases. Transaction framing (`TxnBegin/Commit/Abort`) and strict replay rules. |
| **≤ 5** | ✅ | ⚠️ | Read supported. New writes may lazily upgrade file header/minor when required by newer semantics (see migration docs). |

### Upgrade behavior (high level)

- **Existing files** are opened without rewriting whenever possible.\n- Some operations may **lazily upgrade** metadata (e.g. header/minor) when newer invariants are required.\n- Recovery behavior is controlled by `OpenOptions.recovery` / `RecoveryMode` (`AutoTruncate` vs `Strict`).

## Segment types and stability

Typra’s on-disk log is append-only segments with checksums.

- **Stable/persistent segments** (part of the durable logical state): `Schema`, `Record`, `Index`, `Manifest`, `TxnBegin`, `TxnCommit`, `TxnAbort`, `Checkpoint`.\n- **Ephemeral segments**: `Temp` is **scratch spill storage** for bounded-memory operators. It is **ignored by replay** and should not affect reopen; it may be truncated/cleaned opportunistically.

Checkpoint payloads are validated **when used**; corrupt checkpoint bytes should not prevent opening in `AutoTruncate` mode.

## Crate / package API stability

### Rust crates

- **`typra`** (facade): preferred dependency for applications.\n  - Stability: best-effort source compatibility within a minor series.\n- **`typra-core`** (engine): lower-level APIs and internal types.\n  - Stability: more churn pre-1.0; expect internal refactors.\n- **`typra-derive`**: proc macro for `#[derive(DbModel)]`.\n  - Stability: additive improvements; avoid breaking changes where possible.

### Python package (`typra` on PyPI)

- The Python surface mirrors the engine where feasible.\n- DB-API (`typra.dbapi`) is **experimental** and implements a **read-only subset** of PEP 249 for a minimal `SELECT` grammar (see [`docs/guide_python.md`](guide_python.md)).

## DB-API + SQL subset guarantees (current)

- Supported: `SELECT <cols|*> FROM <collection>` with optional `WHERE` (=`?` params; `AND`/`OR` and ranges), optional `ORDER BY`, optional `LIMIT`.\n- Not supported: DDL/DML SQL, joins, group-by SQL, SQLAlchemy dialect.\n- Cursor behavior: `fetchone`/`fetchmany`/`fetchall` retrieve results incrementally (no forced full materialization on `execute`).

