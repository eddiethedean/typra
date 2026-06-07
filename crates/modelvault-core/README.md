# `modelvault-core`

[![CI](https://github.com/eddiethedean/modelvault/actions/workflows/ci.yml/badge.svg)](https://github.com/eddiethedean/modelvault/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/modelvault-core.svg)](https://crates.io/crates/modelvault-core)
[![Docs](https://readthedocs.org/projects/modelvault/badge/?version=latest)](https://modelvault.readthedocs.io/en/latest/)

Core engine for **ModelVault** — an application-focused embedded database: model-driven schemas, validation on write, indexes, migrations, and single-file storage. Read paths use `&self` on `Database`; **cross-thread** sharing is enforced in bindings (`RwLock` in Python / async facade), not inside this crate alone.

Most applications should use the **[`modelvault`](https://github.com/eddiethedean/modelvault/blob/main/crates/modelvault/README.md)** facade instead.

**Documentation:** **[modelvault.readthedocs.io](https://modelvault.readthedocs.io/en/latest/)** · rustdoc: [docs.rs/modelvault-core](https://docs.rs/modelvault-core)

| | |
|--|--|
| [Why ModelVault](https://modelvault.readthedocs.io/en/latest/guides/why_modelvault/) | Product positioning |
| [On-disk format](https://modelvault.readthedocs.io/en/latest/specs/on_disk_file_format/) | File layout (advanced) |
| [Compatibility](https://modelvault.readthedocs.io/en/latest/reference/compatibility/) | Format contracts |

## Install

```toml
[dependencies]
modelvault-core = "0.16"
```

Optional tracing: `modelvault-core = { version = "0.16", features = ["tracing"] }`

## Example

```bash
cargo run -p modelvault --example open   # facade demo
```

```rust
use modelvault_core::prelude::*;

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

MIT — [LICENSE](https://github.com/eddiethedean/modelvault/blob/main/LICENSE)
