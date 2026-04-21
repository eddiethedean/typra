"""Insert / get / snapshot for Python bindings."""

from __future__ import annotations

import typra


def test_insert_get_file(tmp_path) -> None:
    path = tmp_path / "ig.typra"
    db = typra.Database.open(str(path))
    fields = """[
      {"path": ["title"], "type": "string"},
      {"path": ["year"], "type": "int64"}
    ]"""
    db.register_collection("books", fields, "title")
    db.insert("books", {"title": "Rust", "year": 2024})
    row = db.get("books", "Rust")
    assert row is not None
    assert row["title"] == "Rust"
    assert row["year"] == 2024


def test_mem_snapshot_roundtrip() -> None:
    db = typra.Database.open_in_memory()
    db.register_collection(
        "t",
        '[{"path": ["k"], "type": "string"}]',
        "k",
    )
    db.insert("t", {"k": "x"})
    snap = db.snapshot_bytes()
    db2 = typra.Database.open_snapshot_bytes(snap)
    r = db2.get("t", "x")
    assert r is not None
    assert r["k"] == "x"


def test_snapshot_rejects_on_file_db(tmp_path) -> None:
    db = typra.Database.open(str(tmp_path / "f.typra"))
    try:
        db.snapshot_bytes()
    except ValueError:
        pass
    else:
        raise AssertionError("expected ValueError")
