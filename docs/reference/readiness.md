# 1.0 readiness checklist

This checklist ties Typra’s 1.0 contract to concrete tests and documentation.

## File format + recovery

- **Open never panics on malformed snapshots**
  - Rust: `crates/typra-core/tests/integration/snapshot_hardening.rs`
- **Strict vs AutoTruncate recovery for torn tails**
  - Rust: `crates/typra-core/tests/integration/recovery_torn_commit.rs`
  - Rust: `crates/typra-core/tests/integration/transaction_recovery.rs`
- **Writer locking is robust across crashes**
  - Rust: `crates/typra-core/tests/integration/file_locking_crash_release.rs`
- **Cross-process file locking**
  - Rust: `crates/typra-core/tests/integration/file_locking.rs`
- **Legacy format minor 5 segments replay after header upgrade to v6**
  - Rust: `crates/typra-core/tests/unit/src_db_replay_tests.rs` (`v6_replay_accepts_legacy_unframed_segments_after_header_upgrade`)

## Schema + records

- **Multi-segment field paths supported end-to-end**
  - Rust: `crates/typra-core/tests/integration/schema_paths_multi_segment.rs`
  - Python: `python/typra/tests/test_multi_segment_schema_paths.py`
  - Spec: `docs/specs/record_encoding_v3.md`
- **Multi-segment validation (types + constraints) on write**
  - Rust: `crates/typra-core/tests/integration/schema_paths_multi_segment.rs`
- **Checkpoint preserves nested (v3) row data**
  - Rust: `crates/typra-core/tests/integration/schema_paths_multi_segment.rs`
- **Schema update classification is conservative and migration-aware**
  - Rust: `crates/typra-core/tests/integration/schema_compatibility.rs`
  - Rust: `crates/typra-core/tests/integration/coverage_db_api_more.rs`

## Indexing + queries

- **Indexed equality predicates use indexes when available**
  - Rust: `crates/typra-core/tests/integration/schema_paths_multi_segment.rs`
  - Rust: `crates/typra-core/tests/integration/query_planner_coverage.rs`
- **Range predicates, ordering, and limit are correct (including index + residual + limit)**
  - Rust: `crates/typra-core/tests/integration/query_range_predicates.rs`
  - Rust: `crates/typra-core/tests/integration/query_order_by.rs`
  - Rust: `crates/typra-core/tests/integration/query_planner_coverage.rs`

## Transactions, compaction, checkpoints

- **Transactions (Rust + Python)**
  - Rust: `crates/typra-core/tests/integration/transaction_recovery.rs`
  - Python: `python/typra/tests/test_typra.py`, `test_e2e_inventory_workflow.py`
- **Compaction**
  - Rust: `crates/typra-core/tests/integration/compaction.rs`
  - Python: `python/typra/tests/test_migrations_and_compaction.py`
- **Checkpoints**
  - Rust: `crates/typra-core/tests/integration/checkpoint_more.rs`, `e2e_production_journey.rs`

## Python surface

- **`typra.models` (dataclass/Pydantic)**
  - Python: `python/typra/tests/test_models.py`, `test_e2e_production_journey.py`
- **Migrations (`plan_schema_version`, `register_schema_version`, backfill)**
  - Python: `python/typra/tests/test_migrations_and_compaction.py`, `test_models.py`
- **Exception mapping is stable and specific**
  - Python: `python/typra/tests/test_error_mapping.py`
- **DB-API edge cases and SQL subset**
  - Python: `python/typra/tests/test_dbapi_edge_cases.py`, `test_dbapi_sql.py`
- **Snapshot backup/restore**
  - Python: `python/typra/tests/test_snapshots.py`

## Operations + hardening

- **Operational CLI**
  - Rust: `crates/typra-cli/tests/cli_smoke.rs`
  - Docs: `docs/reference/cli.md`
- **E2E production journeys**
  - Rust: `crates/typra-core/tests/integration/e2e_production_journey.rs`
  - Python: `python/typra/tests/test_e2e_production_journey.py`
- **Fuzz harness (decode surfaces)**
  - `fuzz/` targets (see `.github/workflows/fuzz.yml`)

## Observability + CI gates

- **Tracing feature compiles and instruments core paths**
  - Rust: `cargo test -p typra-core --features tracing`
  - Docs: `docs/ops/debugging.md`
- **1.0 readiness pipeline**
  - `make check-1p0-ready` (includes async facade tests)
  - CI: `.github/workflows/ci.yml` `readiness` job
- **Pydantic parity in CI**
  - Python: `python/typra/tests/test_models.py` (with `pydantic>=2` installed)
- **Async vs sync policy documented**
  - `docs/reference/async_policy.md`

## Model ergonomics

- **Python subset models + catalog compatibility**
  - Python: `python/typra/tests/test_models.py`
  - Rust: `crates/typra/examples/subset_models.rs`
- **Nested field migration backfill**
  - Rust: `crates/typra-core/tests/integration/schema_paths_multi_segment.rs`
  - Python: `python/typra/tests/test_migrations_and_compaction.py`

## Property invariants

- **Index vs scan, replay idempotence, unique index**
  - Rust: `crates/typra-core/tests/integration/property_invariants.rs`
