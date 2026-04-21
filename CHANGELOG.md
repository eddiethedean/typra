# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **`typra`**: Application-facing facade crate re-exporting `typra-core` and (by default) the `DbModel` derive via feature `derive`. Use `typra = "0.1"` in application `Cargo.toml`; depend on `typra-core` / `typra-derive` directly only when you need a slimmer dependency graph.
- **Tests:** Expanded Rust integration tests (`DbError` display/source, `Database` path edge cases, generic `DbModel` derive). **pytest** suite under `python/typra/tests/` for the extension module. **CI** workflow (`.github/workflows/ci.yml`) runs `cargo test --workspace` and Python tests via `maturin develop` + `pytest`.

## [0.1.0] - 2026-04-21

### Added

- **`typra-core`**: `Database::open` creates/opens a database file; `DbError` with `Display` / `Error` and I/O mapping; `prelude` module; `DbModel` marker trait.
- **`typra-derive`**: `#[derive(DbModel)]` implements `DbModel` for structs (including generics).
- **`typra-python`**: PyO3 module `typra` with `__version__` aligned to the workspace release.
- Integration tests for derive and file open behavior.

### Notes

- Storage, queries, validation, and rich Python APIs are **not** implemented yet; 0.1.0 establishes semver, crates.io/PyPI layout, and a minimal Rust API surface.

[0.1.0]: https://github.com/eddiethedean/typra/releases/tag/v0.1.0
