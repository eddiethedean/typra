# ModelVault Rebrand & Rename Plan

This document records the rebrand from **Typra** to **ModelVault** — *the database for application models*.

## Completed (0.14.0)

- **Rust crates:** `modelvault`, `modelvault-core`, `modelvault-derive`, `modelvault-cli` (binary `modelvault`)
- **Python package:** `modelvault` on PyPI (PyO3 extension via Maturin)
- **File extension:** `.modelvault` (default for new databases; on-disk format unchanged — `TDB0` magic)
- **Model attributes:** `__modelvault_primary_key__`, `__modelvault_indexes__`, etc.
- **Exceptions:** `ModelVaultFormatError`, `ModelVaultSchemaError`, …
- **Docs / CI:** Read the Docs site, Makefile targets (`check-2p0-ready`), env vars prefixed `MODELVAULT_`

## Backward compatibility

- **On-disk:** ModelVault 0.14.x reads `.typra` and `.modelvault` files written by Typra 1.x (same `TDB0` format). Golden fixture: `crates/modelvault-core/tests/fixtures/format/legacy_1_0_minor6.typra`
- **Migrating code:** replace `import typra` → `import modelvault`, rename `__typra_*__` → `__modelvault_*__`, update dependency names in `Cargo.toml` / `pyproject.toml`

## Goals (original)

- Rename packages and repositories
- Update branding and messaging
- Align docs with the model-first vision
- Improve discoverability and adoption
