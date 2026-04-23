# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.7.0] - 2026-04-22

### Added

- **Secondary indexes (Rust)**: catalog `IndexDef`, insert-time index maintenance, persisted index segments, unique violations, minimal **query AST** (`get` / equality / `limit`), heuristic **`explain`**, **`Database::query_iter`** (pull-based row iterator), **`row_subset_by_field_defs`** for nested path projections.
- **Python**: optional **`indexes_json`** on **`register_collection`**, **`collection(...).where` / `and_where` / `limit` / `explain` / `all`**, subset rows via **`all(fields=[...])`** (paths must match `fields_json`).
- **Benchmarks**: Criterion bench **`crates/typra-core/benches/query.rs`** (`make bench`); compares **`get(pk)`**, indexed equality, and scan.
- **Docs**: Python guide sections for queries, indexes, subset projection, and **DB-API / SQLAlchemy scope** (design-only for 0.7).

### Notes

- **0.6.x → 0.7.0** is **additive** for typical `insert` / `get` usage; see [`docs/migration_0.6_to_0.7.md`](docs/migration_0.6_to_0.7.md). Publishing **`typra-core`** to crates.io before **`typra-derive`** / **`typra`** / **`typra-python`** is required (see [`scripts/publish-crates.sh`](scripts/publish-crates.sh)).

## [0.6.0] - 2026-04-21

### Added

- **Validation engine**: recursive type checks for primitives, `Optional`, `List`, `Object`, and `Enum`; field **constraints** (`min_i64` / `max_i64`, `min_u64` / `max_u64`, `min_f64` / `max_f64`, `min_length` / `max_length`, `regex`, `email`, `url`, `nonempty`) on [`FieldDef`](crates/typra-core/src/schema.rs); structured [`DbError::Validation`](crates/typra-core/src/error.rs) with nested paths.
- **Row values**: [`RowValue`](crates/typra-core/src/record/row_value.rs) for in-memory rows and nested structures; [`Database::insert`](crates/typra-core/src/db/mod.rs) / [`get`](crates/typra-core/src/db/mod.rs) use `BTreeMap<String, RowValue>` (primary key remains a primitive [`ScalarValue`](crates/typra-core/src/record/scalar.rs) for `get` lookups).
- **Record payload v2**: [`encode_record_payload_v2`](crates/typra-core/src/record/payload_v2.rs) and unified [`decode_record_payload`](crates/typra-core/src/record/payload_v2.rs) (replays **v1** and **v2** segments); see [`docs/07_record_encoding_v2.md`](docs/07_record_encoding_v2.md).
- **Catalog v3**: [`CATALOG_PAYLOAD_VERSION_V3`](crates/typra-core/src/catalog/codec.rs) persists per-field `constraints`; decoders still read catalog **v1** and **v2**.
- **Python**: optional `"constraints"` array on each field in `fields_json`; composite values in `insert` / `get`; [`DbError::Validation`](python/typra/src/errors.rs) mapped to `ValueError`.

### Changed

- **Breaking (Rust)**: `Database::insert` / `get` row type is `RowValue`, not `ScalarValue` only.
- **Breaking (Python)**: same semantic change for rows (dicts/lists nest as in schema).

See [`docs/migration_0.5_to_0.6.md`](docs/migration_0.5_to_0.6.md).

## [0.5.1] - 2026-04-22

### Changed

- **typra-core (internal)**: Split `Database` implementation into `db/` submodules (`open`, `replay`, `write`, `helpers`); public `Database` API unchanged.
- Removed unused `StorageEngine` placeholder; `validation` and `config` are documentation-only stubs pending broader validation/config work ([ROADMAP](ROADMAP.md) 0.6+).
- [`Store`](crates/typra-core/src/storage.rs): documented deferring a read-only store trait until a second consumer exists.

## [0.5.0] - 2026-04-21

### Added

- **Record encoding v1**: `SegmentType::Record` payloads with typed primary key + body fields; see [`docs/06_record_encoding_v1.md`](docs/06_record_encoding_v1.md).
- **Catalog**: wire v2 with optional `primary_field` on create; [`Catalog::lookup_name`](crates/typra-core/src/catalog/state.rs) for name → id.
- **Database (Rust)**: generic [`Database<S: Store>`](crates/typra-core/src/db/mod.rs) with default `Database` = on-disk [`FileStore`](crates/typra-core/src/storage.rs); [`Database::open_in_memory`](crates/typra-core/src/db/mod.rs), [`from_snapshot_bytes`](crates/typra-core/src/db/mod.rs), [`snapshot_bytes`](crates/typra-core/src/db/mod.rs); [`insert`](crates/typra-core/src/db/mod.rs) / [`get`](crates/typra-core/src/db/mod.rs); [`register_collection(..., primary_field)`](crates/typra-core/src/db/mod.rs).
- **Format**: new databases use file format minor **5**; first record write lazily bumps **4 → 5**; schema-only writes bump **3 → 4** as in 0.4.0.
- **Python**: `register_collection(..., primary_field)`, `insert`, `get`, `open_in_memory`, `open_snapshot_bytes`, `snapshot_bytes`.

### Changed

- **Breaking**: `register_collection` now requires a **primary field** name (top-level field in the schema). See [`docs/migration_0.4_to_0.5.md`](docs/migration_0.4_to_0.5.md).

## [0.4.0] - 2026-04-21

### Added

- **Schema catalog (Rust)**: binary encoding for catalog records in `SegmentType::Schema` segment payloads (`CreateCollection`, `NewSchemaVersion`), in-memory [`Catalog`](crates/typra-core/src/catalog/state.rs) with replay on `Database::open`, and public APIs [`Database::register_collection`](crates/typra-core/src/db/mod.rs) / [`Database::register_schema_version`](crates/typra-core/src/db/mod.rs).
- **On-disk format**: file format minor **4**; new databases write **0.4** headers; **0.3** files are upgraded lazily to **0.4** on the first catalog write.
- **Python**: [`Database`](python/typra/src/lib.rs) with `open`, `register_collection(fields_json)`, and `collection_names()`; JSON parsing for field definitions in [`fields_json.rs`](python/typra/src/fields_json.rs).
- **Errors**: extended [`SchemaError`](crates/typra-core/src/error.rs) and [`FormatError::InvalidCatalogPayload`](crates/typra-core/src/error.rs).

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

- **`typra-core`**: `Database::open` creates/opens a database file; `DbError` with `Display` / `Error` and I/O mapping; `prelude` module; `DbModel` marker trait.
- **`typra-derive`**: `#[derive(DbModel)]` implements `DbModel` for structs (including generics).
- **`typra-python`**: PyO3 module `typra` with `__version__` aligned to the workspace release.
- Integration tests for derive and file open behavior.

### Notes

- Storage, queries, validation, and rich Python APIs are **not** implemented yet; 0.1.0 establishes semver, crates.io/PyPI layout, and a minimal Rust API surface.

[0.1.0]: https://github.com/eddiethedean/typra/releases/tag/v0.1.0
[0.2.0]: https://github.com/eddiethedean/typra/releases/tag/v0.2.0
[0.3.0]: https://github.com/eddiethedean/typra/releases/tag/v0.3.0
[0.4.0]: https://github.com/eddiethedean/typra/releases/tag/v0.4.0
[0.5.0]: https://github.com/eddiethedean/typra/releases/tag/v0.5.0
[0.5.1]: https://github.com/eddiethedean/typra/releases/tag/v0.5.1
[0.6.0]: https://github.com/eddiethedean/typra/releases/tag/v0.6.0
[0.7.0]: https://github.com/eddiethedean/typra/releases/tag/v0.7.0
