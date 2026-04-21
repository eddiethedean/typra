## `typra-core` (Rust)

Core engine crate for **Typra**, a typed embedded database.

**Status (0.2.0):** minimal API surface for establishing semver and wiring crates together (e.g. `Database::open`, `DbError`). The storage engine is under development.

### Install

```toml
[dependencies]
typra-core = "0.2"
```

### Notes

- Most applications should depend on `typra` (the facade) instead of `typra-core`.

