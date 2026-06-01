# Debugging & tracing

**Audience:** advanced (operators and engine debugging)

Tools for diagnosing validation failures, query plans, and engine behavior.

## Python exception types

Beyond `ValueError`, `OSError`, and `RuntimeError`, ModelVault exposes stable subclasses:

| Class | Typical cause |
|-------|---------------|
| `modelvault.ModelVaultFormatError` | File format / header issues |
| `modelvault.ModelVaultSchemaError` | Catalog / schema mismatch |
| `modelvault.ModelVaultValidationError` | Constraint or type failure on write |
| `modelvault.ModelVaultQueryError` | Invalid query shape |
| `modelvault.ModelVaultTransactionError` | Transaction framing issues |

### Query plans

Use **`explain()`** on collection queries to see whether index lookup was selected:

```python
plan = db.collection("books").where("title", "Hello").explain()
print(plan)  # look for "IndexLookup"
```

## Rust `tracing` (feature-gated)

`modelvault-core` emits spans at open, replay, checkpoint, compaction, and query planning when built with the **`tracing`** feature.

### Build with tracing

```bash
cargo build -p modelvault-core --features tracing
cargo build -p modelvault --features tracing
```

### Subscriber setup

```toml
[dependencies]
modelvault = { version = "0.14", features = ["tracing"] }
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
```

```rust
use tracing_subscriber::EnvFilter;

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let db = modelvault::Database::open("app.modelvault").expect("open");
    let _ = db.collection_names();
}
```

Run with filter:

```bash
RUST_LOG=modelvault_core=debug cargo run --features tracing
```

### Useful targets

| Target | Events |
|--------|--------|
| `modelvault_core::db::open` | File open, recovery |
| `modelvault_core::db::replay` | Segment replay, catalog load |
| `modelvault_core::checkpoint` | Checkpoint encode/decode |
| `modelvault_core::query::planner` | Plan selection, ORDER BY spill |

## Python extension + tracing

Default wheels link `modelvault-core` **without** tracing. Local debug build:

```bash
cd python/modelvault
maturin develop --release --features tracing
```

Tracing output still needs a Rust subscriber in a custom embedding. For most Python debugging, use typed exceptions and **`explain()`**.

## CLI inspection

```bash
modelvault inspect app.modelvault
modelvault dump-catalog app.modelvault --json
```

See [CLI reference](../reference/cli.md).
