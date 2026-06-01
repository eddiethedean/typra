# Format compatibility fixtures

Golden database files used by `tests/integration/format_back_compat_1x.rs` to ensure **2.x** keeps reading **Typra 1.0-shaped** on-disk data (`.typra` extension; `TDB0` format).

## Regenerate

From the repository root:

```bash
./scripts/generate-format-fixtures.sh
```

Commit updated binaries only when an on-disk change is **intentional** and documented in `CHANGELOG.md` and `docs/reference/compatibility.md`.

## Files

| File | Contents |
|------|----------|
| `legacy_1_0_minor6.typra` | Format minor 6; flat collection (record v2) + multi-segment collection (record v3); checkpoint |

The compatibility test compares the **file header** to a live encoder and checks **semantic parity** (collections and representative rows). Raw segment bytes are not compared because superblock/checkpoint layout can differ across platforms and under llvm-cov without indicating a format change.
