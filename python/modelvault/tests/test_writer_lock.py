"""Writer lock: at most one writable Database per path per process."""

from __future__ import annotations

from pathlib import Path

import pytest

import modelvault


def test_second_writable_open_in_same_process_fails(tmp_path: Path) -> None:
    path = tmp_path / "db.modelvault"
    db1 = modelvault.Database.open(str(path))
    with pytest.raises(OSError, match="already open"):
        modelvault.Database.open(str(path))
    del db1
