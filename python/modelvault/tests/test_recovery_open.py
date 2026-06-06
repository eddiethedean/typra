"""Python recovery= maps to core RecoveryMode."""

from __future__ import annotations

from pathlib import Path

import pytest

import modelvault


def test_open_accepts_recovery_auto_truncate(tmp_path: Path) -> None:
    path = tmp_path / "db.modelvault"
    db = modelvault.Database.open(str(path), recovery="auto_truncate")
    db.register_collection(
        "t",
        '[{"path": ["id"], "type": "int64"}]',
        "id",
    )
    db.insert("t", {"id": 1})


def test_auto_truncate_recovers_torn_tail_preserving_data(tmp_path: Path) -> None:
    path = tmp_path / "db.modelvault"
    db = modelvault.Database.open(str(path))
    db.register_collection("t", '[{"path": ["id"], "type": "int64"}]', "id")
    db.insert("t", {"id": 42})
    del db

    data = path.read_bytes()
    assert len(data) > 10
    path.write_bytes(data[:-7])

    with pytest.raises((modelvault.ModelVaultFormatError, OSError)):
        modelvault.Database.open(str(path), recovery="strict")

    db2 = modelvault.Database.open(str(path), recovery="auto_truncate")
    assert db2.get("t", 42) == {"id": 42}


def test_strict_recovery_rejects_corrupt_tail(tmp_path: Path) -> None:
    path = tmp_path / "db.modelvault"
    db = modelvault.Database.open(str(path))
    db.register_collection("t", '[{"path": ["id"], "type": "int64"}]', "id")
    db.insert("t", {"id": 1})
    del db

    path.write_bytes(path.read_bytes() + b"garbage")

    with pytest.raises(modelvault.ModelVaultFormatError):
        modelvault.Database.open(str(path), recovery="strict")
