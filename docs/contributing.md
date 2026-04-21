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

Workspace crates and the PyPI distribution share **`[workspace.package] version`** in the root `Cargo.toml` (currently **0.4.0**). Bump that version when you cut releases, then tag **`vX.Y.Z`** to match.

## Coverage (practical 100%)

We aim for **practical 100%** test coverage over first-party code, with an explicit exclusion allowlist for things that are not meaningfully coverable.

- **Rust**: coverage is computed via `cargo llvm-cov`.
  - Exclusions are explicit and justified. For example, the PyO3 module entrypoint under `python/typra/src/lib.rs` is executed by Python import, not by `cargo test`, so Rust-only coverage runs may exclude it.
  - We primarily track **line coverage** for “practical 100%”; region/branch coverage may remain <100% in cases where the only missed regions are OS-level IO failure paths that are not deterministic to test.
- **Python**: coverage is computed via `pytest-cov` (coverage.py).
  - Virtual environments, `site-packages`, and vendored dependencies are omitted via `.coveragerc`.

CI publishes coverage reports but does not fail builds based on percentage.

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

On push of a tag matching `v*.*.*` (e.g. `v0.1.0`), [`.github/workflows/publish.yml`](../.github/workflows/publish.yml) asserts the tag matches `[workspace.package] version`, runs `./scripts/publish-crates.sh`, then builds and uploads **sdist + wheels** with [maturin-action](https://github.com/PyO3/maturin-action). Python wheels use **PyO3’s stable ABI** (`abi3`, `cp39-abi3`): **one wheel per platform** (manylinux x86_64/aarch64, musllinux x86_64/aarch64, macOS x86_64/arm64, Windows x86_64/arm64), compatible with **CPython 3.9+**. Each upload uses **`twine upload --skip-existing`**. Configure repository **Secrets**:

| Secret | Purpose |
|--------|---------|
| `CARGO_REGISTRY_TOKEN` | crates.io API token |
| `PYPI_API_TOKEN` | PyPI API token (used as `TWINE_PASSWORD` with `TWINE_USERNAME=__token__`) |

The tag **must** match `[workspace.package] version` in the root `Cargo.toml` (e.g. tag `v0.1.0` and `version = "0.1.0"`).

### crates.io (Rust)

Rust crates under `crates/` include **`typra`** (application facade), **`typra-core`**, and **`typra-derive`**. Publish **`typra-core`** first, then **`typra-derive`**, then **`typra`**, then **`typra-python`** (see [`scripts/publish-crates.sh`](../scripts/publish-crates.sh)), because published crates resolve dependencies from **crates.io**, not path deps.

**Before you tag or publish**, from the repo root:

```bash
make check-full
```

1. Log in: `cargo login` with an API token from [crates.io account settings](https://crates.io/settings/tokens).
2. Optionally set `repository = "..."` under `[workspace.package]` in the root `Cargo.toml` (recommended).
3. Publish in order (each step may **`cargo publish -p … --dry-run`** first, but **`--dry-run` only succeeds when crates.io already has the dependencies** for that crate—so the first package’s dry-run is the one you can verify before any upload):

```bash
cargo publish -p typra-core --dry-run   # OK before anything is on crates.io
cargo publish -p typra-core

cargo publish -p typra-derive --dry-run # OK after typra-core 0.x is published
cargo publish -p typra-derive

cargo publish -p typra --dry-run        # OK after typra-core + typra-derive are published
cargo publish -p typra
```

The **`typra-python`** Rust package (PyO3) is still a Cargo workspace member for versioning and `cargo check`, but it is **released to PyPI**, not treated as a primary “Rust crate” in the repo layout. To publish its sources to crates.io as well (after **`typra-core`** is on crates.io):

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

## Next implementation steps (high level)

1. Record encoding and insert/get APIs (`0.5.x` milestone).
2. Validation engine and constraint errors (`0.6.x`).
3. Secondary indexes and simple filters (`0.7.x`).
4. Transactions and crash-safe checkpoints (`0.8.x`).

See [`ROADMAP.md`](../ROADMAP.md) for the full release breakdown.

### File format notes (0.3.x–0.4.x)

Starting with the `0.3.x` on-disk format work, the database file layout includes reserved **Superblock A/B** regions (for crash-safe metadata publication later) and checksummed **append-only segments**. This scaffolding is still internal, but it changes on-disk compatibility behavior:

- A header-only `0.2` file can be upgraded in-place to the `0.3` layout.
- Other `0.2` layouts are rejected rather than guessed, to avoid corrupting unknown data.

`0.3.0` also adds minimal **manifest publication**: a tiny MANIFEST payload is appended as a checksummed segment, then its pointer is published by alternating Superblock A/B with `generation+1`.

**`0.4.0`** adds a persisted **schema catalog**: catalog events are written as **`SegmentType::Schema`** payloads and **replayed on open**. New databases write format **0.4** headers; existing **0.3** files are upgraded **lazily** to **0.4** on the first catalog write (see [`CHANGELOG.md`](../CHANGELOG.md)).
