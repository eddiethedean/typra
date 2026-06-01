# 1.0 readiness checklist

**Audience:** advanced (release engineering)

This checklist ties ModelVault’s 1.0 contract to concrete tests and documentation. Application developers: [Compatibility](compatibility.md) · [Why ModelVault](../guides/why_modelvault.md).

## File format + recovery

- **1.x reads 1.0-shaped `.modelvault` files (golden fixture + append)**
  - Rust: `crates/modelvault-core/tests/integration/format_back_compat_1x.rs`
  - Fixture: `crates/modelvault-core/tests/fixtures/format/legacy_1_0_minor6.modelvault` (regenerate: `scripts/generate-format-fixtures.sh`)
  - Policy: [`docs/reference/compatibility.md`](compatibility.md), [`docs/specs/format_evolution.md`](../specs/format_evolution.md)
- **Open never panics on malformed snapshots**
  - Rust: `crates/modelvault-core/tests/integration/snapshot_hardening.rs`
- **Strict vs AutoTruncate recovery for torn tails**
  - Rust: `crates/modelvault-core/tests/integration/recovery_torn_commit.rs`
  - Rust: `crates/modelvault-core/tests/integration/transaction_recovery.rs`
  - Rust: `crates/modelvault-core/tests/integration/recovery_txn_mismatch.rs` (txn id mismatch truncates under `AutoTruncate`)
  - Python: `python/modelvault/tests/test_recovery_open.py`
- **Writer locking is robust across crashes**
  - Rust: `crates/modelvault-core/tests/integration/file_locking_crash_release.rs`
- **Cross-process file locking**
  - Rust: `crates/modelvault-core/tests/integration/file_locking.rs`
- **In-process single writable handle per path (0.15+)**
  - Rust: `crates/modelvault-core/tests/integration/writer_registry_dual_open.rs`
- **Legacy format minor 5 segments replay after header upgrade to v6**
  - Rust: `crates/modelvault-core/tests/unit/src_db_replay_tests.rs` (`v6_replay_accepts_legacy_unframed_segments_after_header_upgrade`)

## Schema + records

- **Multi-segment field paths supported end-to-end**
  - Rust: `crates/modelvault-core/tests/integration/schema_paths_multi_segment.rs`
  - Python: `python/modelvault/tests/test_multi_segment_schema_paths.py`
  - Spec: `docs/specs/record_encoding_v3.md`
- **Multi-segment validation (types + constraints) on write**
  - Rust: `crates/modelvault-core/tests/integration/schema_paths_multi_segment.rs`
- **Checkpoint preserves nested (v3) row data**
  - Rust: `crates/modelvault-core/tests/integration/schema_paths_multi_segment.rs`
- **Schema update classification is conservative and migration-aware**
  - Rust: `crates/modelvault-core/tests/integration/schema_compatibility.rs`
  - Rust: `crates/modelvault-core/tests/integration/coverage_db_api_more.rs`

## Indexing + queries

- **Indexed equality predicates use indexes when available**
  - Rust: `crates/modelvault-core/tests/integration/schema_paths_multi_segment.rs`
  - Rust: `crates/modelvault-core/tests/integration/query_planner_coverage.rs`
- **Range predicates, ordering, and limit are correct (including index + residual + limit)**
  - Rust: `crates/modelvault-core/tests/integration/query_range_predicates.rs`
  - Rust: `crates/modelvault-core/tests/integration/query_order_by.rs`
  - Rust: `crates/modelvault-core/tests/integration/query_planner_coverage.rs`

## Transactions, compaction, checkpoints

- **Transactions (Rust + Python)**
  - Rust: `crates/modelvault-core/tests/integration/transaction_recovery.rs`, `coverage_db_core_more.rs`
  - Python: `python/modelvault/tests/test_modelvault.py`, `test_e2e_inventory_workflow.py`, `test_transaction_edges.py`
- **ORDER BY + stale index (0.15+)**
  - Rust: `crates/modelvault-core/tests/unit/src_query_planner_tests.rs` (`external_sort_source_new_errors_when_row_missing`)
- **Compaction**
  - Rust: `crates/modelvault-core/tests/integration/compaction.rs`
  - Python: `python/modelvault/tests/test_migrations_and_compaction.py`
- **Checkpoints**
  - Rust: `crates/modelvault-core/tests/integration/checkpoint_more.rs`, `e2e_production_journey.rs`

## Python surface

- **`modelvault.models` (dataclass/Pydantic)**
  - Python: `python/modelvault/tests/test_models.py`, `test_e2e_production_journey.py`
- **Migrations (`plan_schema_version`, `register_schema_version`, backfill)**
  - Python: `python/modelvault/tests/test_migrations_and_compaction.py`, `test_models.py`
- **Exception mapping is stable and specific**
  - Python: `python/modelvault/tests/test_error_mapping.py`
- **DB-API edge cases and SQL subset**
  - Python: `python/modelvault/tests/test_dbapi_edge_cases.py`, `test_dbapi_sql.py` (includes incremental `fetchmany` / perf regression)
- **FastAPI / AsyncDatabase (0.15+)**
  - Example: `examples/fastapi_app/main.py`
  - Python: `python/modelvault/tests/test_async_database.py`, `test_concurrent_reads.py`
- **Snapshot backup/restore**
  - Python: `python/modelvault/tests/test_snapshots.py`

## Operations + hardening

- **Operational CLI**
  - Rust: `crates/modelvault-cli/tests/cli_smoke.rs`
  - Docs: `docs/reference/cli.md`
- **E2E production journeys**
  - Rust: `crates/modelvault-core/tests/integration/e2e_production_journey.rs`
  - Python: `python/modelvault/tests/test_e2e_production_journey.py`
- **Fuzz harness (decode surfaces)**
  - `fuzz/` targets (see `.github/workflows/fuzz.yml`)

## Observability + CI gates

- **Tracing feature compiles and instruments core paths**
  - Rust: `cargo test -p modelvault-core --features tracing`
  - Docs: `docs/ops/debugging.md`
- **1.0 readiness pipeline**
  - `make check-2p0-ready` (includes async facade tests)
  - CI: `.github/workflows/ci.yml` `readiness` job
- **Pydantic parity in CI**
  - Python: `python/modelvault/tests/test_models.py` (with `pydantic>=2` installed)
- **Async vs sync policy documented**
  - `docs/reference/async_policy.md`
- **Concurrent reads on one handle (Python + Rust async facade)**
  - `python/modelvault/src/db_handle.rs` (`RwLock`); tests: `python/modelvault/tests/test_concurrent_reads.py`

## Model ergonomics

- **Python subset models + catalog compatibility**
  - Python: `python/modelvault/tests/test_models.py`
  - Rust: `crates/modelvault/examples/subset_models.rs`
- **Nested field migration backfill**
  - Rust: `crates/modelvault-core/tests/integration/schema_paths_multi_segment.rs`
  - Python: `python/modelvault/tests/test_migrations_and_compaction.py`

## Property invariants

- **Index vs scan, replay idempotence, unique index**
  - Rust: `crates/modelvault-core/tests/integration/property_invariants.rs`

## Release cut checklist {#release-cut-checklist}

Use this before tagging **`vX.Y.Z`** (e.g. **`v0.15.0`**) and publishing to crates.io / PyPI.

### Pre-flight (local)

1. **Workspace version** — root [`Cargo.toml`](https://github.com/eddiethedean/modelvault/blob/main/Cargo.toml) `[workspace.package] version` matches the intended tag (currently **0.15.1**).
2. **Changelog** — [`CHANGELOG.md`](https://github.com/eddiethedean/modelvault/blob/main/CHANGELOG.md) has a dated section for the release; **`[Unreleased]`** is empty or only future work.
3. **Readiness pipeline** — from repo root:

   ```bash
   make check-2p0-ready
   ```

   This runs `check-full` (ruff, ty, cargo fmt/clippy/test, pytest, doc-example verification, docs build) plus `cargo test -p modelvault --features async`.

4. **Doc examples** — README / guide snippets match verified output (`scripts/verify-doc-examples.sh`).

### Tag and publish

1. **Commit** any last-minute version/changelog/doc fixes on `main`.
2. **Tag** (must match `Cargo.toml` exactly):

   ```bash
   git tag -a v0.15.0 -m "ModelVault 0.15.0"
   git push origin main
   git push origin v0.15.0
   ```

3. **CI publish** — pushing the version tag triggers [`.github/workflows/publish.yml`](https://github.com/eddiethedean/modelvault/blob/main/.github/workflows/publish.yml) (crates.io then PyPI wheels). Requires repository secrets **`CARGO_REGISTRY_TOKEN`** and **`PYPI_API_TOKEN`**.

   Manual fallback: [Contributing → Publishing](../dev/contributing_guide.md#publishing) and `./scripts/publish-all.sh`.

4. **GitHub release** — create a release from the tag using the matching **`[X.Y.Z]`** changelog section.

### Post-publish verification

- **crates.io**: `modelvault`, `modelvault-core`, `modelvault-derive`, and `modelvault-cli` at the released version.
- **PyPI**: `pip index versions modelvault` shows the new version; `pip install "modelvault>=0.15.0,<0.16"` succeeds on your platform.
- **Smoke test**:

  ```bash
  pip install "modelvault>=0.15.0,<0.16"
  python -c "import modelvault; print(modelvault.__version__)"
  ```

### After shipping 0.15.0

Document any post-release fixes in **`[Unreleased]`** or a patch **`0.15.x`** section as appropriate.
