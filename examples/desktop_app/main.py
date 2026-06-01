#!/usr/bin/env python3
"""Desktop-style app data: store settings next to the app in a user data directory.

Run from repo root after `make python-develop`:
  .venv/bin/python examples/desktop_app/main.py
  .venv/bin/python examples/desktop_app/main.py   # second run: settings persist
"""
from __future__ import annotations

import os
import sys
from dataclasses import dataclass
from pathlib import Path

import modelvault

APP_NAME = "ModelVaultDesktopDemo"


def data_dir() -> Path:
    """OS-appropriate per-user app data (desktop convention).

    Override with ``MODELVAULT_EXAMPLE_DATA_DIR`` for CI smoke tests (isolated directory).
    """
    override = os.environ.get("MODELVAULT_EXAMPLE_DATA_DIR")
    if override:
        return Path(override)
    if sys.platform == "win32":
        base = Path(os.environ.get("LOCALAPPDATA", Path.home() / "AppData" / "Local"))
    elif sys.platform == "darwin":
        base = Path.home() / "Library" / "Application Support"
    else:
        base = Path(os.environ.get("XDG_DATA_HOME", Path.home() / ".local" / "share"))
    return base / APP_NAME


def db_path() -> Path:
    d = data_dir()
    d.mkdir(parents=True, exist_ok=True)
    return d / "app.modelvault"


@dataclass
class AppSettings:
    __modelvault_primary_key__ = "key"

    key: str
    value: str


def main() -> None:
    path = db_path()
    print("database:", path)
    db = modelvault.Database.open(str(path))
    settings = modelvault.models.collection(db, AppSettings)

    row = settings.get("theme")
    if row is None:
        settings.insert(AppSettings(key="theme", value="dark"))
        print("initialized theme=dark")
    else:
        print("loaded theme=", row.value)

    row2 = settings.get("theme")
    assert row2 is not None
    print("confirmed theme=", row2.value)


if __name__ == "__main__":
    main()
