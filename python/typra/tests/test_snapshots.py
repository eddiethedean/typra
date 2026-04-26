from __future__ import annotations

from pathlib import Path

import typra


def test_export_snapshot_and_open_snapshot_roundtrip(tmp_path: Path) -> None:
    src = tmp_path / "src.typra"
    snap = tmp_path / "snap.typra"

    db = typra.Database.open(str(src))
    db.register_collection("books", '[{"path": ["id"], "type": "int64"}]', "id")
    db.insert("books", {"id": 1})

    db.export_snapshot(str(snap))

    mem = typra.Database.open_snapshot(str(snap))
    got = mem.get("books", 1)
    assert got is not None
    assert got["id"] == 1


def test_restore_snapshot_to_path_roundtrip(tmp_path: Path) -> None:
    src = tmp_path / "src.typra"
    snap = tmp_path / "snap.typra"
    restored = tmp_path / "restored.typra"

    db = typra.Database.open(str(src))
    db.register_collection("books", '[{"path": ["id"], "type": "int64"}]', "id")
    db.insert("books", {"id": 1})
    db.export_snapshot(str(snap))

    typra.Database.restore_snapshot(str(snap), str(restored))

    db2 = typra.Database.open(str(restored))
    got = db2.get("books", 1)
    assert got is not None
    assert got["id"] == 1
