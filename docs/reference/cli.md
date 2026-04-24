# CLI

Typra ships a small operational CLI named **`typra`**. It is designed for debugging and integrity checks of `.typra` files.

## Install / run (from source)

From the repo root:

    cargo run -p typra-cli -- inspect ./app.typra

Or build it:

    cargo build -p typra-cli
    ./target/debug/typra inspect ./app.typra

## Commands

### `typra inspect <path>`

Prints a human-readable summary:

- file format major/minor
- selected superblock generation
- manifest and checkpoint offsets
- catalog summary (collections + schema versions + index counts)

### `typra verify <path>`

Runs a read-only integrity scan:

- segment framing and payload checksums
- schema catalog segment decode/apply

Exits non-zero on failure.

### `typra dump-catalog <path> --json`

Print the schema catalog as JSON for debugging/support.

