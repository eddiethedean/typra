# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.5.0] - 2026-04-21

### Added

- **Record encoding v1**: `SegmentType::Record` payloads with typed primary key + body fields; see [`docs/06_record_encoding_v1.md`](docs/06_record_encoding_v1.md).
- **Catalog**: wire v2 with optional `primary_field` on create; [`Catalog::lookup_name`](crates/typra-core/src/catalog/state.rs) for name → id.
- **Database (Rust)**: generic [`Database<S: Store>`](crates/typra-core/src/db.rs) with default `Database` = on-disk [`FileStore`](crates/typra-core/src/storage.rs); [`Database::open_in_memory`](crates/typra-core/src/db.rs), [`from_snapshot_bytes`](crates/typra-core/src/db.rs), [`snapshot_bytes`](crates/typra-core/src/db.rs); [`insert`](crates/typra-core/src/db.rs) / [`get`](crates/typra-core/src/db.rs); [`register_collection(..., primary_field)`](crates/typra-core/src/db.rs).
- **Format**: new databases use file format minor **5**; first record write lazily bumps **4 → 5**; schema-only writes bump **3 → 4** as in 0.4.0.
- **Python**: `register_collection(..., primary_field)`, `insert`, `get`, `open_in_memory`, `open_snapshot_bytes`, `snapshot_bytes`.

### Changed

- **Breaking**: `register_collection` now requires a **primary field** name (top-level field in the schema). See [`docs/migration_0.4_to_0.5.md`](docs/migration_0.4_to_0.5.md).

## [0.4.0] - 2026-04-21

### Added

- **Schema catalog (Rust)**: binary encoding for catalog records in `SegmentType::Schema` segment payloads (`CreateCollection`, `NewSchemaVersion`), in-memory [`Catalog`](crates/typra-core/src/catalog/state.rs) with replay on `Database::open`, and public APIs [`Database::register_collection`](crates/typra-core/src/db.rs) / [`Database::register_schema_version`](crates/typra-core/src/db.rs).
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
