# `typra-core`

[![CI](https://github.com/eddiethedean/typra/actions/workflows/ci.yml/badge.svg)](https://github.com/eddiethedean/typra/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/typra-core.svg)](https://crates.io/crates/typra-core)
[![Docs](https://readthedocs.org/projects/typra/badge/?version=latest)](https://typra.readthedocs.io/en/latest/)

Core engine for **Typra** — an application-focused embedded database: model-driven schemas, validation on write, indexes, migrations, and single-file storage.

Most applications should use the **[`typra`](https://github.com/eddiethedean/typra/blob/main/crates/typra/README.md)** facade instead.

**Documentation:** **[typra.readthedocs.io](https://typra.readthedocs.io/en/latest/)** · rustdoc: [docs.rs/typra-core](https://docs.rs/typra-core)

| | |
|--|--|
| [Why Typra](https://typra.readthedocs.io/en/latest/guides/why_typra/) | Product positioning |
| [On-disk format](https://typra.readthedocs.io/en/latest/specs/on_disk_file_format/) | File layout (advanced) |
| [Compatibility](https://typra.readthedocs.io/en/latest/reference/compatibility/) | Format contracts |

## Install

```toml
[dependencies]
typra-core = "1.0"
```

Optional tracing: `typra-core = { version = "1.0", features = ["tracing"] }`

## Example

```bash
cargo run -p typra --example open   # facade demo
```

```rust
use typra_core::prelude::*;

fn main() -> Result<(), DbError> {
    let db = Database::open_in_memory()?;
    println!("opened: {}", db.path().display());
    Ok(())
}
```

## When to depend on this crate

- Operational tools on the engine (inspect, verify, migrate)
- Minimal dependency graph without proc-macros
- Types the facade does not re-export

## License

MIT — [LICENSE](https://github.com/eddiethedean/typra/blob/main/LICENSE)
