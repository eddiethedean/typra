# Migrating from 0.7.x to 0.8.0

## Summary

0.8.0 adds **transaction framing** on the append log (`TxnBegin` / `TxnCommit` / `TxnAbort`), bumps the on-disk **format minor to 6** for new databases and for existing files on **first transactional write**, and introduces **crash recovery** options when opening a database.

## Compatibility

- **Reading**: databases created with **0.7.x** (format minor **5**) still open; replay uses the **legacy** path until the header is upgraded.
- **Writing**: the first **register** / **insert** (or explicit transaction commit) after upgrade may bump the header to minor **6** and wrap new segments in **txn markers**.
- **Pre-0.8 durability**: for segments written **before** upgrade to minor **6**, the engine does not rewrite history; the historical **index vs record** ordering on disk remains as written. New writes use **atomic** txn batches.

## API (Rust)

- **`Database::transaction(|db| { ... })`**: closure runs with staged writes; **commit** on `Ok`, **rollback** on `Err`.
- **`Database::begin_transaction` / `commit_transaction` / `rollback_transaction`**: for bindings (e.g. Python `with db.transaction():`).
- **`Database::open_with_options(path, OpenOptions { recovery: ... })`** and **`Database::open_in_memory_with_options`**.
- **`RecoveryMode::AutoTruncate`** (default): if the log ends with a **torn segment** or an **uncommitted** transaction, the file is truncated to the last safe committed prefix before replay.
- **`RecoveryMode::Strict`**: same conditions return **`FormatError::UncleanLogTail`** and **do not** modify the file.

## API (Python)

- **`with db.transaction():`** commits on normal exit, rolls back on exception.

## References

- [`docs/02_on_disk_file_format.md`](02_on_disk_file_format.md) — transaction segments and recovery rules.
- [`CHANGELOG.md`](../CHANGELOG.md) — full list of changes.
