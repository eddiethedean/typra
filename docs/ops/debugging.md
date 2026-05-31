# Debugging & tracing

**Audience:** advanced (operators and engine debugging)

Tools for diagnosing validation failures, query plans, and engine behavior.

## Python exception types

Beyond `ValueError`, `OSError`, and `RuntimeError`, Typra exposes stable subclasses:

| Class | Typical cause |
|-------|---------------|
| `typra.TypraFormatError` | File format / header issues |
| `typra.TypraSchemaError` | Catalog / schema mismatch |
| `typra.TypraValidationError` | Constraint or type failure on write |
| `typra.TypraQueryError` | Invalid query shape |
| `typra.TypraTransactionError` | Transaction framing issues |

### Query plans

Use **`explain()`** on collection queries to see whether index lookup was selected:

```python
plan = db.collection("books").where("title", "Hello").explain()
print(plan)  # look for "IndexLookup"
```

## Rust `tracing` (feature-gated)

`typra-core` emits spans at open, replay, checkpoint, compaction, and query planning when built with the **`tracing`** feature.

### Build with tracing

```bash
cargo build -p typra-core --features tracing
cargo build -p typra --features tracing
```

### Subscriber setup

```toml
[dependencies]
typra = { version = "1.0", features = ["tracing"] }
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
```

```rust
use tracing_subscriber::EnvFilter;

fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    let db = typra::Database::open("app.typra").expect("open");
    let _ = db.collection_names();
}
```

Run with filter:

```bash
RUST_LOG=typra_core=debug cargo run --features tracing
```

### Useful targets

| Target | Events |
|--------|--------|
| `typra_core::db::open` | File open, recovery |
| `typra_core::db::replay` | Segment replay, catalog load |
| `typra_core::checkpoint` | Checkpoint encode/decode |
| `typra_core::query::planner` | Plan selection, ORDER BY spill |

## Python extension + tracing

Default wheels link `typra-core` **without** tracing. Local debug build:

```bash
cd python/typra
maturin develop --release --features tracing
```

Tracing output still needs a Rust subscriber in a custom embedding. For most Python debugging, use typed exceptions and **`explain()`**.

## CLI inspection

```bash
typra inspect app.typra
typra dump-catalog app.typra --json
```

See [CLI reference](../reference/cli.md).
