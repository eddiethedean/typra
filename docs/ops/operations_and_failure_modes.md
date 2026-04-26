# Operations & failure modes

This guide is about running Typra safely in real applications: durability, transactions, reopen behavior, compaction, and what to expect when files are corrupted.

It complements:

- [Quickstart](../guides/quickstart.md) (first steps)
- [Python guide](../guides/python.md) (Python API and DB-API subset)
- [Compatibility matrix](../reference/compatibility.md) (compatibility and stability contract)

## Operational smoke test (Python)

This snippet is intentionally small and deterministic. It exercises:

- open in memory
- register
- insert + get

```python
import typra

db = typra.Database.open_in_memory()
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

## Recovery modes (corrupt or partial writes)

Typra supports two recovery modes on open:

- **`AutoTruncate`**: best-effort open by truncating torn tails back to the last known-good committed prefix.
- **`Strict`**: fail-fast; refuses to open if recovery would require truncation.

For the detailed contract, see [Compatibility matrix](../reference/compatibility.md).

## Locking and cross-process safety (writers)

Typra is designed for **single-writer** embedded usage.

- **Writer exclusion**: on-disk opens in read/write mode take an **exclusive advisory lock** on a
  sidecar file named **`<db_path>.writer.lock`**.
  - If another process already holds the lock, open fails fast with an OS error.
- **Read-only opens**: on-disk read-only opens take a **shared advisory lock** on the same sidecar
  lock file.
  - This means **readers block new writers** (and writers block new readers).
  - This is intended to prevent “RO observe while writer is mid-append” races in operational tools.

## Compaction (file rewrite)

Compaction rewrites the database into a smaller image containing only the live logical state (latest rows + catalog + indexes).

- Rust: `Database::compact_to`, `Database::compact_in_place`
- Python: `db.compact_to(...)`, `db.compact()`

Compaction is intended as an operational tool and should preserve query/index behavior.

### Compaction safety

- **`compact_to(dest)`**: writes a new database image at `dest`.
- **`compact_in_place()`**: is **crash-safe**: it writes a full new image to a temp file and then
  atomically replaces the live `.typra` path.

If compaction fails, the original database should remain intact.

## Backup, verify, and restore (recommended workflow)

Typra supports a “copy the bytes” backup approach. For file-backed databases:

### Backup

1. **Write a checkpoint** (optional but recommended): ensures open can use a stable state marker.
2. **Copy the database file** to a backup location.
3. **Verify** the backup file with the CLI (optional but recommended).

Rust helper:

- `Database::export_snapshot_to_path(dest_path)` performs (1) and (2).

CLI verification:

    typra verify /path/to/backup.typra

### Restore

Restore is a file operation:

- Stop writers.
- Replace the live `.typra` file with your verified backup (prefer an atomic rename at the OS level).
- Open the restored database using `Strict` first if you want fail-fast integrity checks.

Rust helper:

- `Database::restore_snapshot_to_path(snapshot_path, dest_path)` performs an atomic replace.

See also the CLI page for `inspect`/`dump-catalog` workflows.

## Corruption / recovery playbook

When you suspect a file is corrupt or partially written:

1. **Inspect** format + checkpoint metadata:

       typra inspect /path/to/app.typra

2. **Verify** segment framing and catalog decode:

       typra verify /path/to/app.typra

3. **Dump catalog** for support/debugging:

       typra dump-catalog /path/to/app.typra --json

4. **Choose recovery mode**:
   - `Strict`: fail-fast; never truncates.
   - `AutoTruncate`: may truncate an unclean tail back to a safe committed prefix.

