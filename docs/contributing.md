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

## Fuzzing (hardening)

Typra uses [`cargo-fuzz`](https://github.com/rust-fuzz/cargo-fuzz) for decoder and recovery hardening.

From the repository root:

```bash
rustup toolchain install nightly
cargo +nightly fuzz list
cargo +nightly fuzz run decode_segment_header -- -max_total_time=30
```

Fuzz targets live under `fuzz/fuzz_targets/` and should treat decode errors as success; only panics/UB are failures.

## Versioning

Workspace crates and the PyPI distribution share **`[workspace.package] version`** in the root `Cargo.toml` (currently **1.0.0**). Bump that version when you cut releases, then tag **`vX.Y.Z`** to match.

## Coverage (practical 100%)

We aim for **practical 100%** test coverage over first-party code, with an explicit exclusion allowlist for things that are not meaningfully coverable.

- **Rust**: coverage is computed via `cargo llvm-cov`.
  - Exclusions are explicit and justified. For example, the PyO3 module entrypoint under `python/typra/src/lib.rs` is executed by Python import, not by `cargo test`, so Rust-only coverage runs may exclude it.
  - We primarily track **line coverage** for “practical 100%”; region/branch coverage may remain <100% in cases where the only missed regions are OS-level IO failure paths that are not deterministic to test.
  - The `make coverage-rust-core-branch` target enforces **100% branch coverage** over `crates/typra-core` sources, with an explicit per-file allowlist in the `Makefile` (`COVERAGE_TYPRA_CORE_BRANCH_IGNORE_REGEX_1` … `_4`, default: `db/mod.rs`, `db/recover.rs`, `db/replay.rs`, and `query/planner.rs`—orchestration or generic-heavy code where per-file branch totals are noisy). Tighten or remove that allowlist as tests catch up; line coverage is still reported separately.
- **Python**: coverage is computed via `pytest-cov` (coverage.py).
  - Virtual environments, `site-packages`, and vendored dependencies are omitted via `.coveragerc`.

The **coverage** CI job runs `cargo llvm-cov` for the workspace, then enforces a **minimum line coverage for `typra-core`** via `COVERAGE_TYPRA_CORE_LINES` (see [`Makefile`](../Makefile)). Adjust the threshold only when intentionally changing test scope.

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

That includes **`make verify-doc-examples`**, which checks that command output shown in the root README, **`docs/guide_getting_started.md`**, **`docs/guide_python.md`**, and **`python/typra/README.md`** still matches `cargo run -p typra --example open` and every Python snippet that has a paired **text** output block in those files (update **`scripts/verify-doc-examples.sh`** when intentional output changes).

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

1. ~~Validation engine and constraint errors~~ (**delivered in `0.6.0`**).
2. ~~Secondary indexes and simple filters~~ (**delivered in `0.7.0`**).
3. ~~Transactions~~ (**delivered in `0.8.0`**).
4. ~~Schema evolution tooling, record ops, and compaction prototype~~ (**delivered in `0.9.0`**).
5. ~~DB-API 2.0 + minimal SQL text~~ (**delivered in `0.10.0`**).
6. ~~Pager/buffer pool boundary + checkpoints~~ (**delivered in `0.11.0`**).

See [`ROADMAP.md`](../ROADMAP.md) for the full release breakdown.

### File format notes (0.3.x–0.8.x)

Starting with the `0.3.x` on-disk format work, the database file layout includes reserved **Superblock A/B** regions (for crash-safe metadata publication later) and checksummed **append-only segments**. This scaffolding is still internal, but it changes on-disk compatibility behavior:

- A header-only `0.2` file can be upgraded in-place to the `0.3` layout.
- Other `0.2` layouts are rejected rather than guessed, to avoid corrupting unknown data.

`0.3.0` also adds minimal **manifest publication**: a tiny MANIFEST payload is appended as a checksummed segment, then its pointer is published by alternating Superblock A/B with `generation+1`.

**`0.4.0`** adds a persisted **schema catalog**: catalog events are written as **`SegmentType::Schema`** payloads and **replayed on open**. New databases write format **0.4** headers; existing **0.3** files are upgraded **lazily** to **0.4** on the first catalog write (see [`CHANGELOG.md`](../CHANGELOG.md)).

**`0.5.0`** adds **record** segments (**`SegmentType::Record`**, payload v1), **primary key** on catalog create (catalog wire v2), **`insert` / `get`**, and in-memory **`VecStore`** + snapshot bytes. New databases use format minor **5**; existing **0.4** files are upgraded **lazily** to **0.5** on the first **record** write. See [`06_record_encoding_v1.md`](06_record_encoding_v1.md).

**`0.5.1`** is an internal **Rust-only** refactor: the `Database` implementation lives under **`crates/typra-core/src/db/`** (`open`, `replay`, `write`, `helpers`); the public **`Database`** API and on-disk format are unchanged (see [`CHANGELOG.md`](../CHANGELOG.md)).

**`0.6.0`** adds **validation**, **`RowValue`**, **record payload v2**, and **catalog payload v3** (constraints). See [`07_record_encoding_v2.md`](07_record_encoding_v2.md).

**`0.7.0`** adds **secondary indexes** (catalog **v4** carries `indexes` on create / new schema version), **`SegmentType::Index`** segments, a minimal **query** planner and **`Database::query_iter`**, and Python **`indexes_json`** + **`collection(...).where` / `all(fields=[...])`**. See [`CHANGELOG.md`](../CHANGELOG.md) and [`docs/guide_python.md`](guide_python.md).

**`0.8.0`** adds **transaction framing** (txn marker segments), multi-write **`Database::transaction`**, and crash-tail recovery options (`OpenOptions` / `RecoveryMode`); Python adds **`with db.transaction():`**.
