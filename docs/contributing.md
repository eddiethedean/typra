# Contributing

## Layout

Rust libraries live under **`crates/`**. Python distributions (PyPI) live under **`python/`**, even though the extension is implemented with Rust (PyO3).

```text
typra/
├── Cargo.toml          # workspace manifest
├── Cargo.lock
├── LICENSE
├── README.md
├── crates/             # Rust crates (crates.io)
│   ├── typra/          # user-facing facade (`cargo add typra`)
│   ├── typra-core/     # engine core and public API
│   └── typra-derive/   # proc-macro helpers
├── python/             # Python packages (PyPI)
│   └── typra/          # `typra` wheel: maturin + PyO3 (`import typra`)
└── docs/               # design specifications
```

From the repository root:

```bash
cargo check
cargo test
```

**Python tests** (require a venv and a built extension):

```bash
python -m venv .venv && source .venv/bin/activate
pip install maturin pytest
cd python/typra && maturin develop --release && pytest -v
```

CI runs the same Rust and Python checks via [`.github/workflows/ci.yml`](../.github/workflows/ci.yml).

## Versioning

Workspace crates and the PyPI distribution share **`[workspace.package] version`** in the root `Cargo.toml` (currently **0.1.0**). Bump that version when you cut releases, then tag **`vX.Y.Z`** to match.

## Publishing

Automated sequence (from repo root, with credentials in the environment):

```bash
./scripts/publish-all.sh
```

`publish-all.sh` treats **“already exists”** from `cargo publish` as success (for re-pushed tags) and passes **`--skip-existing`** to maturin so duplicate wheels on PyPI do not fail the job.

**Environment variables** (the agent/CI shell must actually export these; they are not always inherited from your login shell):

| Purpose | Variables |
|--------|-----------|
| crates.io | **`CARGO_REGISTRY_TOKEN`** (API token). Alias: `CRATES_IO_TOKEN` is copied to `CARGO_REGISTRY_TOKEN` by the script. |
| PyPI | **`MATURIN_PYPI_TOKEN`** (preferred). Alternatives read by the script: **`PYPI_TOKEN`**, or **`TWINE_USERNAME=__token__`** with **`TWINE_PASSWORD`** (PyPI API token value). |

In **Cursor**, add these under workspace or user settings so the terminal and agent inherit them, or run `./scripts/publish-all.sh` from a local terminal where you have already `export`’d them.

### GitHub Actions

On push of a tag matching `v*.*.*` (e.g. `v0.1.0`), [`.github/workflows/publish.yml`](../.github/workflows/publish.yml) runs `./scripts/publish-all.sh`. Configure repository **Secrets**:

| Secret | Purpose |
|--------|---------|
| `CARGO_REGISTRY_TOKEN` | crates.io API token |
| `PYPI_API_TOKEN` | PyPI API token (mapped to `MATURIN_PYPI_TOKEN` in the workflow) |

The tag **must** match `[workspace.package] version` in the root `Cargo.toml` (e.g. tag `v0.1.0` and `version = "0.1.0"`).

### crates.io (Rust)

Rust crates under `crates/` include **`typra`** (application facade), **`typra-core`**, and **`typra-derive`**. Publish **`typra-core`** and **`typra-derive`** before **`typra`**, since the facade depends on them.

1. Log in: `cargo login` with an API token from [crates.io account settings](https://crates.io/settings/tokens).
2. Optionally set `repository = "..."` under `[workspace.package]` in the root `Cargo.toml` (recommended).
3. Dry-run then publish:

```bash
cargo publish -p typra-core --dry-run
cargo publish -p typra-core

cargo publish -p typra-derive --dry-run
cargo publish -p typra-derive

cargo publish -p typra --dry-run
cargo publish -p typra
```

The **`typra-python`** Rust package (PyO3) is still a Cargo workspace member for versioning and `cargo check`, but it is **released to PyPI**, not treated as a primary “Rust crate” in the repo layout. To publish its sources to crates.io as well:

```bash
cargo publish -p typra-python --dry-run
cargo publish -p typra-python
```

Commit a clean tree before real publishes; omit `--allow-dirty` if you use `cargo publish` defaults.

### PyPI (Python)

The PyPI package name is **`typra`** (`python/typra/pyproject.toml`). The Cargo package in that directory is named **`typra-python`** (implementation detail for crates.io).

1. Install [maturin](https://www.maturin.rs/) and configure PyPI credentials (API token or trusted publishing).
2. Build:

```bash
cd python/typra
maturin build --release
```

3. Publish:

```bash
cd python/typra
maturin publish
```

Version is taken from `Cargo.toml` via `dynamic = ["version"]` in `pyproject.toml`.

## Next implementation steps

1. Schema metadata types in `typra-core`.
2. Append-only segment writer/reader.
3. Validation engine.
4. Collection registration and insert/get APIs.
5. Wire Python module around core operations.
