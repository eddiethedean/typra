# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.15.0] - 2026-06-01

### Fixed

- **Python transactions**: `txn_exit` always runs on context-manager exit; failed commits roll back staging before releasing the read/write lock (fixes stuck exclusive mode after commit errors).
- **`AsyncModelQuery.select`**: field projection matches sync `ModelQuery.select` (no longer calls a missing `AsyncQueryBuilder.select`).
- **`plan_insert_row`**: record encode and PK scalar conversion propagate `DbError` instead of panicking via `expect`.
- **`query_iter` / `ORDER BY` spill**: stale index keys return `IndexRowMissing` instead of being skipped silently.
- **Recovery (`AutoTruncate`)**: orphan `TxnCommit` and txn id mismatch segments are truncated at open so salvageable prefix data remains readable.
- **`commit_transaction` without an active transaction** returns `TransactionError::NoActiveTransaction` (was a silent no-op).
- **`Database::transaction`**: rolls back staging when commit fails after a successful closure body.
- **`get` primary-key type mismatch** surfaces as `SchemaError::PrimaryKeyTypeMismatch` (not a format error).
- **Process-wide writer registry**: at most one writable `Database` handle per on-disk path per process.
- **DB-API cursor**: one `query_iter` pass caches rows so repeated `fetchmany` is not O(n²).
- **Python `Database.open`**: optional `recovery=` (`strict` / `auto_truncate`); **`dbapi.connect`**: `strict_read_only` to disable writable fallback.
- **`ModelCollection.get`**: accepts a model instance (like `delete`); **`ModelQuery.order_by`** delegates to the collection query builder.

### Added

- **Python `AsyncDatabase`** (experimental): asyncio surface (`await AsyncDatabase.open(...)`, `AsyncTransaction`, `AsyncCollection` / `AsyncQuery`, `modelvault.models.async_collection` / `async_plan` / `async_apply`) on a thread pool via Tokio `spawn_blocking`.
- **Concurrent reads on one handle** (Python `Database` / `AsyncDatabase`, Rust `AsyncDatabase` with `async` feature): `get`, `query`, and other read paths use a shared lock; writes and open transactions remain exclusive.
- **FastAPI example**: [`examples/fastapi_app/main.py`](examples/fastapi_app/main.py) uses `AsyncDatabase` and `async def` handlers.

### Changed

- **Python bindings**: replaced process-wide `Mutex` on the engine with `RwLock` + transaction-depth tracking (reads upgrade to exclusive lock while a transaction is open).
- **Docs / READMEs**: concurrency and async policy documented across guides and READMEs.
- **CI / dev**: `pytest-asyncio` installed for `make check-full` and GitHub Actions.

### Notes

- **Upgrading from 0.14.x:** on-disk format unchanged; pin `modelvault>=0.15.0,<0.16` (Python) or `modelvault = "0.15"` (Rust).

## [0.14.0] - 2026-06-01

Ships the stable **application-model database** feature set (see **[1.0.0]** below) under the **ModelVault** package name on crates.io and PyPI.

### Changed

- **Package rename:** published as **ModelVault** — crates `modelvault`, `modelvault-core`, `modelvault-derive`, `modelvault-cli`; Python package `modelvault`; CLI binary `modelvault`.
- **File extension:** default `.modelvault` (on-disk format and `TDB0` magic unchanged; legacy 1.x database files remain readable).
- **Python model hooks:** `__modelvault_primary_key__`, `__modelvault_indexes__`, etc. (replace legacy double-underscore model attribute hooks from the prior release line).
- **Exceptions:** `ModelVaultFormatError`, `ModelVaultSchemaError`, `ModelVaultValidationError`, `ModelVaultQueryError`, `ModelVaultTransactionError`.

### Notes

- **Upgrading from the prior 1.x package name:** change imports and dependency names; existing database files open without conversion. See [Compatibility](docs/reference/compatibility.md#versioning-package-vs-product).

## [1.0.0] - 2026-05-31

First **stable 1.x** product release (published on crates.io/PyPI under the previous package name at `1.0.x`, rebranded to **ModelVault** `0.14.x`): semver + on-disk compatibility policy, production-oriented operations, and **`modelvault.models`** as the primary Python API.

### Added

- **Multi-segment schema field paths**: collection schemas may define nested leaf fields via multi-segment `FieldPath`s (e.g. `["profile","timezone"]`) end-to-end (write, replay, indexes, query, projections).
- **Record payload v3**: new record encoding that persists values keyed by full `FieldPath`, enabling multi-segment schema field defs while retaining v1/v2 read compatibility.
- **Python `modelvault.models`**: class-defined schemas (dataclass + Pydantic v2), constraints, indexes, migrations (`plan` / `apply`), and typed collections as the **recommended** application API.
- **Operational CLI (`modelvault`)**: `inspect`, `verify`, `dump-catalog`, `checkpoint`, `compact`, `backup`, and migration helpers — see [`docs/reference/cli.md`](docs/reference/cli.md).
- **Cross-process safety**: single-writer file locking with crisp errors when a second writer opens the same file.
- **Backup/restore**: checkpoint + supported backup workflow (`modelvault backup`, snapshot bytes APIs).
- **Observability**: optional **`tracing`** feature on `modelvault-core` / `modelvault-python`; structured error kinds documented in [`docs/ops/debugging.md`](docs/ops/debugging.md).
- **Docs & contracts**: compatibility matrix ([`docs/reference/compatibility.md`](docs/reference/compatibility.md)), types matrix, security posture ([`SECURITY.md`](SECURITY.md), [`docs/reference/security.md`](docs/reference/security.md)), async policy ([`docs/reference/async_policy.md`](docs/reference/async_policy.md)), and 1.0 readiness checklist ([`docs/reference/readiness.md`](docs/reference/readiness.md)).
- **CI / quality gates**: `make check-2p0-ready` (includes `check-full`, doc-example verification, and async-facade tests); minimum **`modelvault-core`** line-coverage gate in CI.

### Changed

- **Python parity**: `fields_json`, inserts, and the typed query builder accept and resolve multi-segment schema paths; new parity tests cover nested paths and index-backed queries.
- **Stable API policy**: `modelvault`, `modelvault-core`, and `modelvault-derive` **1.0.x** are safe to depend on directly; breaking changes require **2.0**.

### Notes

- **Upgrading from 0.13.x**: additive for typical usage; new multi-segment paths and record v3 writes apply when you register schemas with nested field paths. Existing files remain readable.
- **Post-1.0 work** (SQL, SQLAlchemy) is tracked in [`ROADMAP.md`](ROADMAP.md), not part of this release.

## [0.13.0] - 2026-04-24

### Added

- **Hardening**: a `cargo-fuzz` harness under `fuzz/` with initial fuzz targets for decode/scan surfaces.
- **Property tests**: initial `proptest` invariants covering snapshot roundtrip behavior.
- **Bounded-memory operators (v0)**:
  - Spillable aggregation foundations (`COUNT` + `SUM(Int64)` over a single `Int64` group-by), with forced-spill tests.
  - Minimal spill-capable hash join foundation (match-count on `Int64` key), with forced-spill tests.
- **Compatibility matrix**: documented file format + API stability policy in `docs/reference/compatibility.md`.

## [0.12.0] - 2026-04-24

### Added

- **Bounded-memory scaffolding (v0)**: an internal streaming `query_iter` operator boundary, plus ephemeral `Temp` spill segments and a small spill helper to support future external algorithms.
- **External sort (v0)**: `order_by` can spill to temporary segments for large inputs (on-disk databases) instead of forcing a full in-memory sort.
- **DB-API cursor incremental fetch (v0)**: `modelvault.dbapi` now refills results incrementally instead of materializing the full result set on `execute`.

## [0.11.0] - 2026-04-24

### Added

- **Pager / buffer pool boundary (v1)**: on-disk reads go through a fixed-size page cache (initially 16 KiB pages) to reduce random I/O and prepare for streaming/bounded-memory work.
- **Checkpoints (v1)**: a new `Checkpoint` segment payload persists a logical state snapshot (catalog + latest rows + index state) and is published via superblock pointers.
- **Checkpointed open**: opening a database can now load the latest checkpoint and replay only the log tail after the checkpoint’s `replay_from_offset`.
- **Operational hook**: `Database::checkpoint()` to write and publish a new checkpoint.

### Changed

- **Recovery behavior**: corrupted checkpoint payloads are rejected in `RecoveryMode::Strict`; `AutoTruncate` falls back to full replay.
- **Docs**: updated READMEs and guides to reflect 0.11.0 and checkpointed open.

## [0.10.0] - 2026-04-24

### Added

- **Python DB-API 2.0 (PEP 249)**: experimental, read-only adapter exposed as **`modelvault.dbapi`** (`connect`, `Connection`, `Cursor`, `execute` + `fetch*`, and `cursor.description`).
- **Minimal SQL `SELECT` subset** (read-only): `SELECT <cols|*> FROM <collection>` with optional `WHERE` (`=` / `AND` / `OR` / range predicates using `?` positional params), `ORDER BY <field> [ASC|DESC]`, and `LIMIT n`.
- **Query errors**: new `DbError::Query(QueryError)` for SQL parsing / query adapter failures.
- **Tests**: Rust unit tests for the SQL adapter and Python integration tests for the DB-API module.

### Changed

- **Docs**: updated guides and READMEs to document DB-API + supported SQL subset and to keep versioned examples consistent with 0.10.0.

## [0.9.0] - 2026-04-24

### Added

- **Schema evolution safety**: compatibility classifier for schema diffs enforced on schema-version registration, with explicit **migration required** vs **breaking** errors.
- **Schema evolution (helpers)**: schema evolution planning plus helpers for **backfill** and **index rebuild**.
- **Record ops**: delete support and index delta maintenance for replace/delete semantics.
- **Query upgrades**: `OR`, range predicates (`<`, `<=`, `>`, `>=`), and `order_by` (in-memory sort).
- **Compaction prototype**: rewrite a database into a compacted image (`compact_to`, `compact_in_place`).
- **Python**: bindings for schema version registration/planning, backfill/index rebuild helpers, delete, and compaction; updated type stubs and tests.

## [0.8.0] - 2026-04-22

### Added

- **Transactions (Rust)**: `SegmentType::TxnBegin` / `TxnCommit` / `TxnAbort` with versioned payloads; **`Database::transaction`**, **`begin_transaction`**, **`commit_transaction`**, **`rollback_transaction`**; multi-insert / schema+data batches are one durable commit; **read-your-writes** inside a transaction via shadow catalog/index/latest maps.
- **Format minor 6**: new databases use **6**; lazy upgrade from **5** on first transactional write; **replay v6** applies segments chronologically with txn framing; **legacy replay** unchanged for minors **≤ 5**.
- **Recovery**: **`Store::truncate`**; **`OpenOptions`** / **`RecoveryMode`** (`AutoTruncate` default, **`Strict`**); tail scan tolerates torn last segment; uncommitted txn tails truncate to last **`TxnBegin`** (auto) or error (strict).
- **Python**: **`with db.transaction():`** context manager mapping to Rust commit/rollback.

### Changed

- **Durability**: autocommit **`insert`** and **`register_*`** emit a single **`TxnBegin` … `TxnCommit`** group with **one** manifest rotation + **`sync`** per logical operation (fixes index/record split across crashes on new writes after upgrade to minor **6**).

### Notes

- **0.7.x → 0.8.0**: existing files remain readable; new writes may upgrade header to minor **6**.

## [0.7.0] - 2026-04-22

### Added

- **Secondary indexes (Rust)**: catalog `IndexDef`, insert-time index maintenance, persisted index segments, unique violations, minimal **query AST** (`get` / equality / `limit`), heuristic **`explain`**, **`Database::query_iter`** (pull-based row iterator), **`row_subset_by_field_defs`** for nested path projections.
- **Python**: optional **`indexes_json`** on **`register_collection`**, **`collection(...).where` / `and_where` / `limit` / `explain` / `all`**, subset rows via **`all(fields=[...])`** (paths must match `fields_json`).
- **Benchmarks**: Criterion bench **`crates/modelvault-core/benches/query.rs`** (`make bench`); compares **`get(pk)`**, indexed equality, and scan.
- **Docs**: Python guide sections for queries, indexes, subset projection, and **DB-API / SQLAlchemy scope** (design-only for 0.7).

### Notes

- **0.6.x → 0.7.0** is **additive** for typical `insert` / `get` usage. Publishing **`modelvault-core`** to crates.io before **`modelvault-derive`** / **`modelvault`** / **`modelvault-python`** is required (see [`scripts/publish-crates.sh`](scripts/publish-crates.sh)).

## [0.6.0] - 2026-04-21

### Added

- **Validation engine**: recursive type checks for primitives, `Optional`, `List`, `Object`, and `Enum`; field **constraints** (`min_i64` / `max_i64`, `min_u64` / `max_u64`, `min_f64` / `max_f64`, `min_length` / `max_length`, `regex`, `email`, `url`, `nonempty`) on [`FieldDef`](crates/modelvault-core/src/schema.rs); structured [`DbError::Validation`](crates/modelvault-core/src/error.rs) with nested paths.
- **Row values**: [`RowValue`](crates/modelvault-core/src/record/row_value.rs) for in-memory rows and nested structures; [`Database::insert`](crates/modelvault-core/src/db/mod.rs) / [`get`](crates/modelvault-core/src/db/mod.rs) use `BTreeMap<String, RowValue>` (primary key remains a primitive [`ScalarValue`](crates/modelvault-core/src/record/scalar.rs) for `get` lookups).
- **Record payload v2**: [`encode_record_payload_v2`](crates/modelvault-core/src/record/payload_v2.rs) and unified [`decode_record_payload`](crates/modelvault-core/src/record/payload_v2.rs) (replays **v1** and **v2** segments); see [`docs/07_record_encoding_v2.md`](docs/07_record_encoding_v2.md).
- **Catalog v3**: [`CATALOG_PAYLOAD_VERSION_V3`](crates/modelvault-core/src/catalog/codec.rs) persists per-field `constraints`; decoders still read catalog **v1** and **v2**.
- **Python**: optional `"constraints"` array on each field in `fields_json`; composite values in `insert` / `get`; [`DbError::Validation`](python/modelvault/src/errors.rs) mapped to `ValueError`.

### Changed

- **Breaking (Rust)**: `Database::insert` / `get` row type is `RowValue`, not `ScalarValue` only.
- **Breaking (Python)**: same semantic change for rows (dicts/lists nest as in schema).

See the release notes above for details.

## [0.5.1] - 2026-04-22

### Changed

- **modelvault-core (internal)**: Split `Database` implementation into `db/` submodules (`open`, `replay`, `write`, `helpers`); public `Database` API unchanged.
- Removed unused `StorageEngine` placeholder; `validation` and `config` are documentation-only stubs pending broader validation/config work ([ROADMAP](ROADMAP.md) 0.6+).
- [`Store`](crates/modelvault-core/src/storage.rs): documented deferring a read-only store trait until a second consumer exists.

## [0.5.0] - 2026-04-21

### Added

- **Record encoding v1**: `SegmentType::Record` payloads with typed primary key + body fields; see [`docs/06_record_encoding_v1.md`](docs/06_record_encoding_v1.md).
- **Catalog**: wire v2 with optional `primary_field` on create; [`Catalog::lookup_name`](crates/modelvault-core/src/catalog/state.rs) for name → id.
- **Database (Rust)**: generic [`Database<S: Store>`](crates/modelvault-core/src/db/mod.rs) with default `Database` = on-disk [`FileStore`](crates/modelvault-core/src/storage.rs); [`Database::open_in_memory`](crates/modelvault-core/src/db/mod.rs), [`from_snapshot_bytes`](crates/modelvault-core/src/db/mod.rs), [`snapshot_bytes`](crates/modelvault-core/src/db/mod.rs); [`insert`](crates/modelvault-core/src/db/mod.rs) / [`get`](crates/modelvault-core/src/db/mod.rs); [`register_collection(..., primary_field)`](crates/modelvault-core/src/db/mod.rs).
- **Format**: new databases use file format minor **5**; first record write lazily bumps **4 → 5**; schema-only writes bump **3 → 4** as in 0.4.0.
- **Python**: `register_collection(..., primary_field)`, `insert`, `get`, `open_in_memory`, `open_snapshot_bytes`, `snapshot_bytes`.

### Changed

- **Breaking**: `register_collection` now requires a **primary field** name (top-level field in the schema).

## [0.4.0] - 2026-04-21

### Added

- **Schema catalog (Rust)**: binary encoding for catalog records in `SegmentType::Schema` segment payloads (`CreateCollection`, `NewSchemaVersion`), in-memory [`Catalog`](crates/modelvault-core/src/catalog/state.rs) with replay on `Database::open`, and public APIs [`Database::register_collection`](crates/modelvault-core/src/db/mod.rs) / [`Database::register_schema_version`](crates/modelvault-core/src/db/mod.rs).
- **On-disk format**: file format minor **4**; new databases write **0.4** headers; **0.3** files are upgraded lazily to **0.4** on the first catalog write.
- **Python**: [`Database`](python/modelvault/src/lib.rs) with `open`, `register_collection(fields_json)`, and `collection_names()`; JSON parsing for field definitions in [`fields_json.rs`](python/modelvault/src/fields_json.rs).
- **Errors**: extended [`SchemaError`](crates/modelvault-core/src/error.rs) and [`FormatError::InvalidCatalogPayload`](crates/modelvault-core/src/error.rs).

### Changed

- **New database files** use format **0.4** (was 0.3) while retaining the same superblock + segment layout.

## [0.3.0] - 2026-04-21

### Added

- **On-disk format scaffolding**: reserve dual superblocks (A/B) after the file header, plus checksummed append-only segments with a minimal segment header and an internal segment scan utility.
- **Manifest publication**: append a tiny MANIFEST segment and publish its pointer by alternating superblocks (generation+1), with safe scan fallback when the manifest pointer is invalid.
- **Compatibility**: safe `0.2` → `0.3` upgrade path for header-only `0.2` files.

## [0.2.0] - 2026-04-21

### Added

- **File format**: Create/validate a fixed database file header (`TDB0`, v0.2) on `Database::open`, with explicit format errors for bad magic, unsupported versions, and truncation.
- **Storage boundary**: Introduce a `Store` trait and `FileStore` implementation to abstract I/O and make the engine testable without entangling raw `std::fs::File` usage throughout the codebase.
- **Schema scaffolding**: Add initial schema metadata types (`CollectionSchema`, `FieldPath`, `Type`, etc.) as a foundation for upcoming validation and evolution work.
- **Docs**: Add user guides under `docs/` (getting started, concepts, models/collections, storage modes) and expand the release roadmap.
- **CI / coverage**: Add a coverage job producing Rust + Python reports as artifacts (coverage is reported, not enforced as a hard gate).

### Changed

- **CI**: Run Rust + Python jobs on Linux, macOS, and Windows; fix Python venv handling across platforms.

## [0.1.0] - 2026-04-21

### Added

- **`modelvault-core`**: `Database::open` creates/opens a database file; `DbError` with `Display` / `Error` and I/O mapping; `prelude` module; `DbModel` marker trait.
- **`modelvault-derive`**: `#[derive(DbModel)]` implements `DbModel` for structs (including generics).
- **`modelvault-python`**: PyO3 module `modelvault` with `__version__` aligned to the workspace release.
- Integration tests for derive and file open behavior.

### Notes

- Storage, queries, validation, and rich Python APIs are **not** implemented yet; 0.1.0 establishes semver, crates.io/PyPI layout, and a minimal Rust API surface.

[0.1.0]: https://github.com/eddiethedean/modelvault/releases/tag/v0.1.0
[0.2.0]: https://github.com/eddiethedean/modelvault/releases/tag/v0.2.0
[0.3.0]: https://github.com/eddiethedean/modelvault/releases/tag/v0.3.0
[0.4.0]: https://github.com/eddiethedean/modelvault/releases/tag/v0.4.0
[0.5.0]: https://github.com/eddiethedean/modelvault/releases/tag/v0.5.0
[0.5.1]: https://github.com/eddiethedean/modelvault/releases/tag/v0.5.1
[0.6.0]: https://github.com/eddiethedean/modelvault/releases/tag/v0.6.0
[0.7.0]: https://github.com/eddiethedean/modelvault/releases/tag/v0.7.0
[0.8.0]: https://github.com/eddiethedean/modelvault/releases/tag/v0.8.0
[0.9.0]: https://github.com/eddiethedean/modelvault/releases/tag/v0.9.0
[0.10.0]: https://github.com/eddiethedean/modelvault/releases/tag/v0.10.0
[0.11.0]: https://github.com/eddiethedean/modelvault/releases/tag/v0.11.0
[0.12.0]: https://github.com/eddiethedean/modelvault/releases/tag/v0.12.0
[0.13.0]: https://github.com/eddiethedean/modelvault/releases/tag/v0.13.0
[0.15.0]: https://github.com/eddiethedean/modelvault/releases/tag/v0.15.0
[0.14.0]: https://github.com/eddiethedean/modelvault/releases/tag/v0.14.0
[1.0.0]: https://github.com/eddiethedean/modelvault/releases/tag/v1.0.0
