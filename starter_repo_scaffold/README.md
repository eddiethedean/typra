# Typra starter scaffold

A starter Rust workspace scaffold for the Typra typed embedded database project.

## Crates
- `typra-core`: engine core and public API
- `typra-derive`: proc-macro derive helpers
- `typra-python`: PyO3 bindings (`import typra`)

## Versioning

Workspace crates and the PyPI distribution are aligned at **0.0.0** (name reservation / pre-release). Bump `[workspace.package] version` in the root `Cargo.toml` and ship a new `typra` wheel when you cut releases.

## Publishing (name reservation)

### crates.io (Rust)

1. Log in: `cargo login` with an API token from [crates.io account settings](https://crates.io/settings/tokens).
2. Optionally set `repository = "..."` under `[workspace.package]` in `Cargo.toml` (recommended).
3. From this directory, dry-run then publish each crate (no dependency order required today):

```bash
cargo publish -p typra-core --dry-run
cargo publish -p typra-core

cargo publish -p typra-derive --dry-run
cargo publish -p typra-derive

cargo publish -p typra-python --dry-run
cargo publish -p typra-python
```

Commit a clean tree before real publishes; omit `--allow-dirty` if you use `cargo publish` defaults.

### PyPI (Python)

The PyPI package name is **`typra`** (see `crates/typra-python/pyproject.toml`). The Rust crate that builds the extension is **`typra-python`**.

1. Install [maturin](https://www.maturin.rs/) and configure PyPI credentials (API token or trusted publishing).
2. Build to confirm:

```bash
cd crates/typra-python
maturin build --release
```

3. Publish:

```bash
cd crates/typra-python
maturin publish
```

Version is taken from `Cargo.toml` (`[workspace.package]` / crate package) via `dynamic = ["version"]` in `pyproject.toml`.

## Next steps
1. Implement schema metadata types in `typra-core`.
2. Add append-only segment writer/reader.
3. Add validation engine.
4. Add simple collection registration and insert/get APIs.
5. Wire Python module around core operations.
