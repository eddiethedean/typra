# Typra User Guide: Operations and failure modes

This guide is about running Typra safely in real applications: durability, transactions, reopen behavior, compaction, and what to expect when files are corrupted.

It complements:

- [`guide_getting_started.md`](guide_getting_started.md) (first steps)
- [`guide_python.md`](guide_python.md) (Python API and DB-API subset)
- [`compatibility_matrix.md`](compatibility_matrix.md) (compatibility and stability contract)

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

For the detailed contract, see [`compatibility_matrix.md`](compatibility_matrix.md).

## Compaction (file rewrite)

Compaction rewrites the database into a smaller image containing only the live logical state (latest rows + catalog + indexes).

- Rust: `Database::compact_to`, `Database::compact_in_place`
- Python: `db.compact_to(...)`, `db.compact()`

Compaction is intended as an operational tool and should preserve query/index behavior.

