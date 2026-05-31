# `typra-core`

[![CI](https://github.com/eddiethedean/typra/actions/workflows/ci.yml/badge.svg)](https://github.com/eddiethedean/typra/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/typra-core.svg)](https://crates.io/crates/typra-core)
[![Docs](https://readthedocs.org/projects/typra/badge/?version=latest)](https://typra.readthedocs.io/en/latest/?badge=latest)

Core engine for **Typra**: typed, embedded, single-file storage with a persisted schema catalog and record encoding (v1 + v2 + v3).

Most applications should use the facade crate **[`typra`](https://github.com/eddiethedean/typra/blob/main/crates/typra/README.md)** instead.

| Resource | Link |
|----------|------|
| **Repository** | [github.com/eddiethedean/typra](https://github.com/eddiethedean/typra) |
| **Facade crate** | [`typra` on crates.io](https://crates.io/crates/typra) |
| **Changelog** | [CHANGELOG.md](https://github.com/eddiethedean/typra/blob/main/CHANGELOG.md) |
| **On-disk format** | [docs/specs/on_disk_file_format.md](https://github.com/eddiethedean/typra/blob/main/docs/specs/on_disk_file_format.md) |
| **Record v3** | [docs/specs/record_encoding_v3.md](https://github.com/eddiethedean/typra/blob/main/docs/specs/record_encoding_v3.md) |
| **Module layout** | [docs/specs/rust_crate_layout.md](https://github.com/eddiethedean/typra/blob/main/docs/specs/rust_crate_layout.md) |

## What ships (v1.0.x)

- **`Database<S: Store>`** with **`FileStore`** (on-disk) and **`VecStore`** (in-memory)
- **Schema catalog** replay: **`primary_field`**, **constraints**, **multi-segment `FieldPath`s**, index definitions
- **Records**: **`RowValue`** / **`ScalarValue`**, payload v1/v2/v3 read compatibility, v3 writes for nested paths
- **CRUD + validation**: **`insert` / `get` / `delete`** with **`ValidationError`**
- **Indexes + queries**: secondary indexes, **`Query`** / **`Predicate`**, **`query_iter`**, subset projections
- **Durability**: transactions, checkpoints, compaction, snapshot bytes, recovery modes
- **SQL adapter** (minimal `SELECT` subset for Python DB-API — prefer typed query AST in Rust apps)

## Guarantees

- [Compatibility and recovery](https://github.com/eddiethedean/typra/blob/main/docs/reference/compatibility.md)
- [Types matrix](https://github.com/eddiethedean/typra/blob/main/docs/reference/types.md)
- [Operations and failure modes](https://github.com/eddiethedean/typra/blob/main/docs/ops/operations_and_failure_modes.md)

## Stability

- **`typra-core` 1.0.x** is **stable and safe to depend on directly** for tooling and custom integrations.
- Crate-root exports (`Database`, schema types, errors) are the primary stable surface.
- **Feature flags** are additive.

## Install

```toml
[dependencies]
typra-core = "1.0"
```

Optional tracing (open/replay/checkpoint/query planning hooks):

```toml
typra-core = { version = "1.0", features = ["tracing"] }
```

See [docs/ops/debugging.md](https://github.com/eddiethedean/typra/blob/main/docs/ops/debugging.md).

## Example

For a runnable introduction, use the facade example:

```bash
cargo run -p typra --example open
```

Minimal direct usage:

```rust
use typra_core::prelude::*;

fn main() -> Result<(), DbError> {
    let mut db = Database::open_in_memory()?;
    println!("opened: {}", db.path().display());
    Ok(())
}
```

## When to use this crate

- Building operational tools on the engine (inspect, verify, migrate)
- Minimal dependency graphs without proc-macros
- Access to module-level types the facade does not re-export

## License

MIT — see [LICENSE](https://github.com/eddiethedean/typra/blob/main/LICENSE).
