# Compatibility

**Audience:** intermediate (operators and library consumers)

Read/write compatibility for `.modelvault` files and stability expectations for public APIs — what you can rely on when you **ship a single `.modelvault` file`** with your app.

New to ModelVault? Start with [Why ModelVault](../guides/why_modelvault.md) and [Quickstart](../guides/quickstart.md). ModelVault is **1.x**: breaking changes require a major version bump. File-format evolution is explicit and tested.

### Versioning (package vs product)

| Name | Meaning |
|------|---------|
| **Package / crate version** | SemVer on [crates.io](https://crates.io/crates/modelvault) and [PyPI](https://pypi.org/project/modelvault/) (e.g. **`0.14.0`** today). |
| **Product milestone** | Docs and marketing refer to the **1.0** feature set (stable engine, `modelvault.models`, format-compat pledge below). |
| **Pre-rebrand files** | Same `TDB0` on-disk layout as today; the legacy file suffix is still supported — see the [rebrand plan on GitHub](https://github.com/eddiethedean/modelvault/blob/main/docs/MODELVAULT_REBRAND_PLAN.md). |

## 1.x on-disk backwards compatibility pledge

**Any ModelVault 1.y.z release must remain able to read `.modelvault` files produced by earlier 1.x releases** (and pre-1.0 minors that 1.0 already reads), without requiring users to migrate or rewrite files.

| Guarantee | Meaning |
|-----------|---------|
| **Read** | Open + replay returns the same logical data the file contained when it was last committed (per `RecoveryMode`). |
| **Append** | Supported write paths may append new segments; they must not corrupt or drop existing committed rows. |
| **Lazy upgrade** | The engine may bump `format_minor` in the file header (e.g. to **6** for transaction framing) when a write requires newer invariants; the log prefix remains replayable. |
| **Payload versions** | Catalog **v1–v4**, record **v1–v3**, and index **v1–v2** stay decodable for the life of **1.x**. |

**Requires ModelVault 2.0 (new `FORMAT_MAJOR`):** removing replay for any of the above, changing segment-type semantics, or incompatible layout changes to headers/superblocks/segments that existing 1.x files rely on.

Contributor rules and release checklist: [Format evolution (1.x)](../specs/format_evolution.md). CI: `make test-format-compat` and golden fixtures under `crates/modelvault-core/tests/fixtures/format/`.

## File-format compatibility

ModelVault database files have a **format major** and **format minor** (see [On-disk file format spec](../specs/on_disk_file_format.md)).

- **Format major (`FORMAT_MAJOR`)**: breaking changes. ModelVault will refuse to open unknown majors. **1.x uses major 0**; a future incompatible format uses major **1** with ModelVault **2.0**.
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
- ModelVault prefers **preserving the file’s current minor** until an operation requires newer invariants.
- When a **lazy upgrade** happens, ModelVault will make the post-upgrade behavior explicit in release notes.

#### Practical rules by minor

- **Minor 6**
  - **Writes** are fully supported.
  - **Recovery** honors transaction framing. Tail corruption/incomplete txn tails are handled according to `RecoveryMode`.
- **Minor ≤ 5**
  - **Reads** are supported.
  - **Writes are best-effort**: some write paths require **minor 6** invariants (notably around atomic multi-write durability).
    - In those cases ModelVault may **lazily upgrade** the file to minor 6 before/while appending new durable state.

### Recovery modes (contract)

Recovery behavior is controlled by `OpenOptions.recovery` / `RecoveryMode`.

- **`AutoTruncate`** (default)
  - Open succeeds if a valid committed prefix can be identified.
  - Torn tails / incomplete transaction tails may be **truncated** back to the last known-good committed state.
  - If a checkpoint is corrupt, the engine should fall back to replaying from an earlier safe point (e.g. full replay) rather than producing silently-wrong results.
- **`Strict`**
  - Open refuses if integrity checks fail for required metadata, or if recovery would require truncation.
  - Intended for environments that prefer fail-fast over best-effort salvage.

Note: `Database::open_read_only(...)` uses **`Strict`** recovery by default (read-only opens never
truncate the underlying file).

### Forward compatibility (contract)

- Unknown **format majors** are refused.
- Unknown **format minors** within a known major are refused unless explicitly handled by the compatibility logic for that release line.
- Unknown **segment types** are refused by default, unless explicitly declared ignorable/ephemeral by the format spec for that major/minor.

ModelVault prefers **explicit compatibility** over “best guess” parsing.

### What 1.1, 1.2, … may add (without breaking old files)

- New **optional** segment types (must be documented; default is refuse unknown types).
- New **record/catalog/index payload versions** (old versions still replay).
- New **format minor** values (older minors **3–6** remain readable; matrix updated in this doc).
- Engine features (SQL, planner, compaction) that only **append** new segments or add decode paths.

See [Format evolution (1.x)](../specs/format_evolution.md).

## Segment types and stability

ModelVault’s on-disk log is append-only segments with checksums.

- **Stable/persistent segments** (part of the durable logical state): `Schema`, `Record`, `Index`, `Manifest`, `TxnBegin`, `TxnCommit`, `TxnAbort`, `Checkpoint`.
- **Ephemeral segments**: `Temp` is **scratch spill storage** for bounded-memory operators.
  - It is **ignored by replay** and must not affect durable state after reopen.
  - It may be truncated/cleaned opportunistically (including at the end of an operator).

Checkpoint payloads are validated **when used**; corrupt checkpoint bytes should not prevent opening in `AutoTruncate` mode.

## Crate / package API stability

### Rust crates

- **`modelvault`** (facade): preferred dependency for applications.
  - **Stability goal**: strongest compatibility guarantees in the Rust ecosystem for ModelVault.
  - **Policy**: breaking changes require a major version bump and are called out explicitly.
- **`modelvault-core`** (engine): lower-level APIs and internal types.
  - **Stability goal (1.x)**: stable for direct use by power users. Breaking changes require a
    major version bump (2.0), same as `modelvault`.
  - **Policy**: internal refactors are acceptable, but **public exports** and documented behavior
    remain compatible within 1.x.
- **`modelvault-derive`**: proc macro for `#[derive(DbModel)]`.
  - **Stability goal**: mostly additive improvements (new attributes and validations) with minimal breakage.

### Python package (`modelvault` on PyPI)

- The Python surface mirrors the engine where feasible.
- DB-API (`modelvault.dbapi`) is a read-only subset of PEP 249 for a minimal `SELECT` grammar (see [Python guide](../guides/python.md)).

## DB-API + SQL subset guarantees (current)

- Supported: `SELECT \<cols|*\> FROM \<collection\>` with optional `WHERE` (=`?` params; `AND`/`OR` and ranges), optional `ORDER BY`, optional `LIMIT`.
- Not supported: DDL/DML SQL, joins, group-by SQL, SQLAlchemy dialect.
- Cursor behavior: `fetchone`/`fetchmany`/`fetchall` retrieve results incrementally (no forced full materialization on `execute`).

