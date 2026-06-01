# Format compatibility fixtures

Golden database files used by `tests/integration/format_back_compat_1x.rs` to ensure **ModelVault 0.14.x** keeps reading **legacy 1.x-shaped** on-disk data (`TDB0` format).

Regenerate committed bytes (when the on-disk contract intentionally changes):

```bash
./scripts/generate-format-fixtures.sh
```

## Files

| File | Description |
|------|-------------|
| `legacy_1_0_minor6.modelvault` | Format minor 6; flat collection (record v2) + multi-segment collection (record v3); checkpoint |
