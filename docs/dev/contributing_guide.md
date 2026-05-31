# Contributing

Welcome! This guide covers repo layout, local development, CI gates, and release workflow.

## Repository layout

```text
typra/
├── Cargo.toml          # workspace manifest + shared version
├── crates/             # Rust crates (crates.io)
│   ├── typra/          # application facade
│   ├── typra-core/     # engine
│   ├── typra-derive/   # proc macros
│   └── typra-cli/      # CLI binary
├── python/typra/       # PyPI package (maturin + PyO3)
└── docs/               # MkDocs site (this book)
```

## Quick start (local dev)

```bash
python3 -m venv .venv
.venv/bin/python -m pip install -U pip
make check-full
```

**`make check-full`** runs Rust checks/tests, builds the Python extension, runs pytest, **`make docs-check`**, and **`make verify-doc-examples`**.

For the **1.0 readiness gate**:

```bash
make check-1p0-ready   # check-full + async facade tests
```

## Rust

```bash
cargo check
cargo test
cargo clippy -- -D warnings   # if configured in CI
```

## Python

```bash
.venv/bin/python -m pip install -U "maturin>=1.5,<2" pytest
cd python/typra && maturin develop --release && pytest -q
```

## Documentation

Docs are built with [MkDocs Material](https://squidfunk.github.io/mkdocs-material/).

```bash
make docs-check    # strict link check
make docs-serve    # local preview (if target exists)
```

### Verified examples

**`make verify-doc-examples`** ensures stdout from snippets in READMEs and guides matches documented output. When you change example behavior, update:

1. The code block in the doc/README
2. The matching ` ```text ` output block
3. **`scripts/verify-doc-examples.sh`** expected heredocs

Covered sources: root README, `docs/guides/quickstart.md`, `docs/guides/python.md`, `python/typra/README.md`, `docs/ops/operations_and_failure_modes.md`.

### Runnable examples

Under **`examples/`** (todo app, CLI notes, FastAPI). Smoke test:

```bash
make examples-smoke   # todo, cli_notes, desktop (isolated data dir)
```

When adding examples, include a README with **problem → solution → run commands → result**, and keep `*.typra` files gitignored (see `examples/.gitignore`). Desktop smoke uses `TYPRA_EXAMPLE_DATA_DIR` — do not write into the user's real app-data folder in CI.

Blog drafts live under **`blog/`** — see [typra-for-application-models.md](https://github.com/eddiethedean/typra/blob/main/blog/typra-for-application-models.md).

Positioning docs: [Why Typra](../guides/why_typra.md), [Comparisons](../comparisons/index.md). Page levels and checklist: [Documentation map](documentation_map.md). Full editorial plan: `docs/TYPRA_DOCUMENTATION_POSITIONING_MASTER_PLAN.md` on GitHub.

## Fuzzing

```bash
rustup toolchain install nightly
cargo +nightly fuzz run decode_segment_header -- -max_total_time=30
```

Targets under `fuzz/fuzz_targets/`. Decode errors are success; panics/UB are failures.

## Benchmarks

```bash
make bench
python scripts/bench_compare.py --base target/criterion-old --new target/criterion
```

CI runs benches weekly (non-blocking). See [bench workflow on GitHub](https://github.com/eddiethedean/typra/blob/main/.github/workflows/bench.yml).

## Coverage

- **Rust**: `cargo llvm-cov` — practical 100% line coverage goal for `typra-core` with documented exclusions
- **Python**: `pytest-cov` via `.coveragerc`

Gap report: `python scripts/coverage_typra_core_gap_rank.py`

## Versioning

Workspace crates and PyPI share **`[workspace.package] version`** in root `Cargo.toml`. Bump on release, tag **`vX.Y.Z`** to match.

## Publishing

Automated from a version tag:

```bash
./scripts/publish-all.sh   # requires CARGO_REGISTRY_TOKEN + MATURIN_PYPI_TOKEN
```

GitHub Actions [publish workflow](https://github.com/eddiethedean/typra/blob/main/.github/workflows/publish.yml) runs on `v*.*.*` tags.

**Before tagging:**

1. `make check-1p0-ready`
2. [Release checklist](../reference/readiness.md#release-cut-checklist-100)
3. Update `CHANGELOG.md`

Publish order for crates.io: **`typra-core`** → **`typra-derive`** → **`typra`** → **`typra-python`**.

## CI

[`.github/workflows/ci.yml`](https://github.com/eddiethedean/typra/blob/main/.github/workflows/ci.yml) — Rust + Python + docs + coverage.

## Where to look next

| Topic | Doc |
|-------|-----|
| File format | [Specifications](../specs/index.md) |
| API contracts | [Compatibility](../reference/compatibility.md) |
| 1.x file-format rules | [Format evolution](../specs/format_evolution.md) · `make test-format-compat` |
| Roadmap | [ROADMAP on GitHub](https://github.com/eddiethedean/typra/blob/main/ROADMAP.md) |
| Full contributing notes (repo root) | [contributing.md on GitHub](https://github.com/eddiethedean/typra/blob/main/docs/contributing.md) |
