"""Python recovery= maps to core RecoveryMode."""

from __future__ import annotations

import modelvault


def test_open_accepts_recovery_auto_truncate(tmp_path) -> None:
    path = tmp_path / "db.modelvault"
    db = modelvault.Database.open(str(path), recovery="auto_truncate")
    db.register_collection(
        "t",
        '[{"path": ["id"], "type": "int64"}]',
        "id",
    )
    db.insert("t", {"id": 1})
