from __future__ import annotations

from pathlib import Path

import modelvault


def test_export_snapshot_and_open_snapshot_roundtrip(tmp_path: Path) -> None:
    src = tmp_path / "src.modelvault"
    snap = tmp_path / "snap.modelvault"

    db = modelvault.Database.open(str(src))
    db.register_collection("books", '[{"path": ["id"], "type": "int64"}]', "id")
    db.insert("books", {"id": 1})

    db.export_snapshot(str(snap))

    mem = modelvault.Database.open_snapshot(str(snap))
    got = mem.get("books", 1)
    assert got is not None
    assert got["id"] == 1


def test_restore_snapshot_to_path_roundtrip(tmp_path: Path) -> None:
    src = tmp_path / "src.modelvault"
    snap = tmp_path / "snap.modelvault"
    restored = tmp_path / "restored.modelvault"

    db = modelvault.Database.open(str(src))
    db.register_collection("books", '[{"path": ["id"], "type": "int64"}]', "id")
    db.insert("books", {"id": 1})
    db.export_snapshot(str(snap))

    modelvault.Database.restore_snapshot(str(snap), str(restored))

    db2 = modelvault.Database.open(str(restored))
    got = db2.get("books", 1)
    assert got is not None
    assert got["id"] == 1
