# 1.0 readiness checklist

This checklist ties Typra’s 1.0 contract to concrete tests and documentation.

## File format + recovery

- **Open never panics on malformed snapshots**
  - Rust: `crates/typra-core/tests/snapshot_hardening.rs`
- **Strict vs AutoTruncate recovery for torn tails**
  - Rust: `crates/typra-core/tests/recovery_torn_commit.rs`
  - Rust: `crates/typra-core/tests/transaction_recovery.rs`
- **Writer locking is robust across crashes**
  - Rust: `crates/typra-core/tests/file_locking_crash_release.rs`

## Schema + records

- **Multi-segment field paths supported end-to-end**
  - Rust: `crates/typra-core/tests/schema_paths_multi_segment.rs`
  - Python: `python/typra/tests/test_multi_segment_schema_paths.py`
- **Schema update classification is conservative and migration-aware**
  - Rust: `crates/typra-core/tests/schema_compatibility.rs`

## Indexing + queries

- **Indexed equality predicates use indexes when available**
  - Rust: `crates/typra-core/tests/schema_paths_multi_segment.rs`
  - Rust: `crates/typra-core/tests/query_equality_predicates.rs` (plus related query tests)
- **Range predicates, ordering, and limit are correct**
  - Rust: `crates/typra-core/tests/query_range_predicates.rs`
  - Rust: `crates/typra-core/tests/query_order_by.rs`

## Python surface

- **Exception mapping is stable and specific**
  - Python: `python/typra/tests/test_error_mapping.py`
- **DB-API edge cases are enforced**
  - Python: `python/typra/tests/test_dbapi_edge_cases.py`

