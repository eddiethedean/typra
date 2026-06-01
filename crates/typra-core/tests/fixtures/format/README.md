# Format compatibility fixtures

Golden `.typra` files used by `tests/integration/format_back_compat_1x.rs` to ensure **1.x** releases keep reading **1.0-shaped** files.

## Regenerate

From the repository root:

```bash
./scripts/generate-format-fixtures.sh
```

Commit updated binaries only when an on-disk change is **intentional** and documented in `CHANGELOG.md` and `docs/reference/compatibility.md`.

## Files

| File | Contents |
|------|----------|
| `typra_1_0_minor6.typra` | Format minor 6; flat collection (record v2) + multi-segment collection (record v3); checkpoint |

The compatibility test compares the **file header** and **segment log** bytes to a live encoder output (skipped under `cfg(coverage)` / llvm-cov, where semantic parity is checked instead). **Superblock** slots may differ only in `generation` / checksum (publish count) without indicating a format change.
