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

Typical uses:

- Confirm what format minor a file is on.
- Check whether a checkpoint is present (non-zero checkpoint offset/len).
- Quick “what collections exist?” debugging in prod support situations.

### `typra verify <path>`

Runs a read-only integrity scan:

- segment framing and payload checksums
- schema catalog segment decode/apply

Exits non-zero on failure.

Typical uses:

- Verify a database before restoring it from backup.
- Validate a suspicious file before attempting a recovery open.

### `typra dump-catalog <path> --json`

Print the schema catalog as JSON for debugging/support.

Typical uses:

- Attach catalog state to an issue (collection ids, schema versions, fields, index defs).
- Confirm primary keys, constraints, and index definitions.

### `typra checkpoint <path>`

Write a durable checkpoint to the file.

Typical uses:

- Create a “stable state marker” before taking a filesystem-level backup.
- Reduce open/replay time by publishing a checkpoint.

### `typra compact <path> --in-place`

Compact the database in place (crash-safe atomic replace).

### `typra compact <path> --to <dest>`

Write a compacted copy to `<dest>`.

### `typra backup <path> --to <dest> [--verify]`

Create a consistent snapshot at `<dest>` (checkpoint + copy). If `--verify` is set, runs `typra verify`
on the produced snapshot.

