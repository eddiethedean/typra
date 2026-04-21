## `typra-derive` (Rust)

Proc-macro derives for **Typra** database models.

### Install

Most users should depend on `typra` (which enables this via the default `derive` feature). If you need the macro crate directly:

```toml
[dependencies]
typra-derive = "0.2"
typra-core = "0.2"
```

### Example

```rust
use typra_core::DbModel;

#[derive(DbModel)]
struct Book {
    title: String,
}
```

### Status (0.2.0)

This derive currently provides a minimal `DbModel` impl. Field attributes and enums are not implemented yet.

