# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

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
