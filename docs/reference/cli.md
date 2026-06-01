# CLI reference

**Audience:** intermediate (operators)

The **`modelvault`** CLI inspects, verifies, checkpoints, compacts, and migrates `.modelvault` files you ship with desktop, CLI, or server apps — built for support and ops, not application hot paths.

Backups and recovery: [Operations runbook](../ops/operations_and_failure_modes.md)

## Install / run

=== "From source (repo)"

    ```bash
    cargo run -p modelvault-cli -- inspect ./app.modelvault
    ```

=== "Build binary"

    ```bash
    cargo build -p modelvault-cli
    ./target/debug/modelvault inspect ./app.modelvault
    ```

## Commands

### `inspect <path>`

Human-readable summary:

- Format major/minor
- Superblock generation
- Manifest and checkpoint offsets
- Catalog summary (collections, schema versions, index counts)

**Use when:** confirming format minor, checkpoint presence, or “what collections exist?”

### `verify <path>`

Read-only integrity scan — segment framing, checksums, catalog decode. Exits non-zero on failure.

**Use when:** validating backups before restore, or checking suspicious files before recovery open.

### `dump-catalog <path> --json`

Schema catalog as JSON.

**Use when:** attaching catalog state to issues, confirming PKs/constraints/indexes.

### `checkpoint <path>`

Write a durable checkpoint.

**Use when:** creating a stable marker before filesystem backup, or reducing replay time on open.

### `compact <path>`

| Flag | Action |
|------|--------|
| **`--in-place`** | Crash-safe atomic replace of live file |
| **`--to <dest>`** | Write compacted copy to destination |

### `backup <path> --to <dest> [--verify]`

Checkpoint + copy. With **`--verify`**, runs `verify` on the snapshot.

### `migrate plan` / `migrate apply`

Schema migration helpers for deployment pipelines.

```bash
modelvault migrate plan ./app.modelvault --collection books --schema-json schema.json
modelvault migrate apply ./app.modelvault --collection books --schema-json schema.json [--force]
```

**`plan`** — JSON describing whether a schema update is safe and required steps.

**`apply`** — register new schema version using engine helpers. If classification is `NeedsMigration`, perform required backfill/rebuild steps, then use **`--force`**.

## Related

- [Operations runbook](../ops/operations_and_failure_modes.md)
- [Compatibility matrix](compatibility.md)
