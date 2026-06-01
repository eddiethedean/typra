# Operations runbook

**Audience:** intermediate (production)

You chose ModelVault to **deploy as a single file** — this runbook covers running it safely: durability, locking, compaction, backup/restore, and recovery from corruption.

| Related | Link |
|---------|------|
| Why single-file embed | [Why ModelVault](../guides/why_modelvault.md) · [Storage modes](../guides/storage_modes.md) |
| First steps | [Quickstart](../guides/quickstart.md) |
| Python API | [Python guide](../guides/python.md) |
| Format contract | [Compatibility](../reference/compatibility.md) |
| CLI | [CLI reference](../reference/cli.md) |

## Smoke test

Minimal deterministic check — open, register, insert, get:

```python
import modelvault

db = modelvault.Database.open_in_memory()
db.register_collection("books", '[{"path": ["title"], "type": "string"}]', "title")
db.insert("books", {"title": "Hello"})

print("opened:", db.path())
print("names:", db.collection_names())
print("get:", db.get("books", "Hello"))
```

Output:

```text
opened: :memory:
names: ['books']
get: {'title': 'Hello'}
```

## Recovery modes

On open, choose how to handle corrupt or partial writes:

| Mode | Behavior |
|------|----------|
| **`AutoTruncate`** (default for RW) | Truncate torn tails and stop at first mid-log CRC failure; salvage committed prefix |
| **`Strict`** | Fail if recovery would require truncation |

Read-only opens use **`Strict`** by default (never truncate). Under **`AutoTruncate`**, a CRC error in the middle of the log is treated like a torn tail: segments before the failure remain readable; bytes from the failure offset onward are discarded. Nested uncommitted transactions (`TxnBegin` without `Commit`) are truncated as well. A stray **`TxnCommit`** without a matching **`TxnBegin`** (e.g. crash mid-framing) is truncated at that segment offset; committed data before it is kept.

Details: [Compatibility → recovery](../reference/compatibility.md#recovery-modes-contract).

## Locking (single-writer)

ModelVault is **single-writer** embedded:

| Open mode | Lock |
|-----------|------|
| **Read/write** | Exclusive advisory lock on `<db_path>.writer.lock` |
| **Read-only** | Shared advisory lock on same file |

Another process holding the lock → open fails fast. Readers block new writers; writers block new readers.

## In-process concurrency (one handle)

Separate from **file** locking: Python `Database` / `AsyncDatabase` (and Rust `AsyncDatabase` with the `async` feature) use an **`RwLock`** around the in-memory engine view:

| Operation | Lock | Notes |
|-----------|------|-------|
| **Reads** — `get`, `query`, `explain`, `collection_names`, … | Shared | Safe to overlap across threads or `asyncio.gather` on the **same** handle |
| **Writes** — `insert`, `delete`, schema changes, compaction, … | Exclusive | One mutator at a time |
| **Open transaction** | Exclusive for all ops on that handle | Other readers/writers on the same handle wait until commit/rollback |

**Backup tip:** While a writer holds the file lock, use **checkpoint + copy** or `export_snapshot` from a consistent point; concurrent **reads** on one handle do not replace cross-process `read_only` opens for scaling readers across processes.

Policy details: [Async vs sync policy](../reference/async_policy.md#concurrency).

## Compaction

Rewrites the database to a smaller image (live rows + catalog + indexes only).

| API | Notes |
|-----|-------|
| Rust `compact_to`, `compact_in_place` | |
| Python `compact_to`, `compact()` | |
| **`compact_in_place`** | Crash-safe: temp file + atomic replace |

Failed compaction should leave the original file intact.

## Backup, verify, restore

### Backup workflow

1. **Checkpoint** (recommended) — stable state marker
2. **Copy** the `.modelvault` file
3. **Verify** with CLI (recommended)

```bash
modelvault verify /path/to/backup.modelvault
```

Rust: `Database::export_snapshot_to_path(dest)` does checkpoint + copy.

### Restore

1. Stop all writers
2. Replace live file with verified backup (atomic rename preferred)
3. Open with **`Strict`** first if you want fail-fast integrity checks

Rust: `Database::restore_snapshot_to_path(snapshot, dest)`.

## Corruption playbook

When a file may be corrupt or partially written:

```bash
# 1. Inspect header + checkpoint metadata
modelvault inspect /path/to/app.modelvault

# 2. Verify segment framing and catalog
modelvault verify /path/to/app.modelvault

# 3. Dump catalog for support
modelvault dump-catalog /path/to/app.modelvault --json
```

Then choose recovery mode:

- **`Strict`** — never truncates; use when you need certainty
- **`AutoTruncate`** — salvage by truncating unclean tail

## Transactions

Multi-write atomicity uses transaction marker segments. Incomplete txn tails are handled per recovery mode. See [Compatibility](../reference/compatibility.md) for format minor 6 semantics.
