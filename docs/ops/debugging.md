# Debugging & tracing

## Stable error “kinds” (Python)

Typra raises standard exceptions (`ValueError`, `OSError`, `RuntimeError`) and also exposes more specific subclasses so you can branch reliably:

- `typra.TypraFormatError`
- `typra.TypraSchemaError`
- `typra.TypraValidationError`
- `typra.TypraQueryError`
- `typra.TypraTransactionError`

## Rust `tracing` (feature-gated)

`typra-core` provides optional `tracing` instrumentation behind a feature flag.

- **Compile with tracing enabled**:

    cargo build -p typra-core --features tracing

To see spans/events, you must add a subscriber in your application (e.g. `tracing-subscriber`) and configure it as you prefer.

