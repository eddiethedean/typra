# Contributing

## Layout

This repository is a standard Cargo workspace:

```text
typra/
├── Cargo.toml          # workspace manifest
├── Cargo.lock
├── LICENSE
├── README.md
├── crates/
│   ├── typra-core/     # engine core and public API
│   ├── typra-derive/   # proc-macro helpers
│   └── typra-python/   # PyO3 bindings (`import typra`)
└── docs/               # design specifications
```

From the repository root:

```bash
cargo check
cargo test
```

## Versioning

Workspace crates and the PyPI distribution are aligned at **0.0.0** (pre-release / name reservation). Bump `[workspace.package] version` in the root `Cargo.toml` when you cut releases.

## Publishing

### crates.io (Rust)

1. Log in: `cargo login` with an API token from [crates.io account settings](https://crates.io/settings/tokens).
2. Optionally set `repository = "..."` under `[workspace.package]` in the root `Cargo.toml` (recommended).
3. Dry-run then publish each crate (no dependency order required today):

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

The PyPI package name is **`typra`** (`crates/typra-python/pyproject.toml`). The Rust crate that builds the extension is **`typra-python`**.

1. Install [maturin](https://www.maturin.rs/) and configure PyPI credentials (API token or trusted publishing).
2. Build:

```bash
cd crates/typra-python
maturin build --release
```

3. Publish:

```bash
cd crates/typra-python
maturin publish
```

Version is taken from `Cargo.toml` via `dynamic = ["version"]` in `pyproject.toml`.

## Next implementation steps

1. Schema metadata types in `typra-core`.
2. Append-only segment writer/reader.
3. Validation engine.
4. Collection registration and insert/get APIs.
5. Wire Python module around core operations.
