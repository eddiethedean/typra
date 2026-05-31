# Debugging & tracing

## Stable error “kinds” (Python)

Typra raises standard exceptions (`ValueError`, `OSError`, `RuntimeError`) and also exposes more specific subclasses so you can branch reliably:

- `typra.TypraFormatError`
- `typra.TypraSchemaError`
- `typra.TypraValidationError`
- `typra.TypraQueryError`
- `typra.TypraTransactionError`

## Rust `tracing` (feature-gated)

`typra-core` provides optional `tracing` instrumentation behind a feature flag. Spans and events are emitted at open, replay, checkpoint encode/decode, compaction, and query planning boundaries.

### Compile with tracing enabled

```bash
cargo build -p typra-core --features tracing
cargo test -p typra-core --features tracing
```

For the application facade:

```bash
cargo build -p typra --features tracing
```

### Minimal subscriber example

Add `tracing-subscriber` to your application and initialize it before opening a database:

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

Run with a filter:

```bash
RUST_LOG=typra_core=debug cargo run --features tracing
```

Useful targets:

- `typra_core::db::open` — file open and recovery
- `typra_core::db::replay` — segment replay (`replay_tail`, catalog load)
- `typra_core::checkpoint` — checkpoint encode/decode
- `typra_core::query::planner` — query plan selection and ORDER BY spill

### Python extension

The PyO3 extension links `typra-core` without the `tracing` feature by default. To build with tracing for local debugging:

```bash
cd python/typra
maturin develop --release --features tracing
```

Tracing output still requires a Rust subscriber in a custom embedding; the stock wheel build does not install one. For most Python debugging, use structured exception types and `explain()` on queries instead.
