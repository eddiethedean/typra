# Desktop app data example

**Problem:** A desktop application needs offline settings storage in a user-specific data directory, shipped without a database server.

**Solution:** Open `app.typra` under the platform app-data path (`Application Support` on macOS, `%LOCALAPPDATA%` on Windows, XDG on Linux).

## Run

```bash
.venv/bin/python examples/desktop_app/main.py
.venv/bin/python examples/desktop_app/main.py   # persists theme across runs
```

The script prints the database path so you can inspect the file with `typra` CLI tools.

For CI, `make examples-smoke` sets `TYPRA_EXAMPLE_DATA_DIR` to `examples/desktop_app/.smoke-data` so your real app-data directory is not touched.

## What it demonstrates

- Per-user data directory convention
- On-disk `Database.open` for durable settings
- Dataclass model (no network, no server)

Docs: [Storage modes](https://typra.readthedocs.io/en/latest/guides/storage_modes/) · [Why Typra](https://typra.readthedocs.io/en/latest/guides/why_typra/)
