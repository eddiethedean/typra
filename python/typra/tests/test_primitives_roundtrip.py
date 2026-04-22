"""Per-type coverage: each v1 primitive roundtrips through ``insert`` and ``get``."""

from __future__ import annotations

import uuid

import typra


def test_all_primitives_roundtrip(tmp_path) -> None:
    path = tmp_path / "prim.typra"
    fields = """[
      {"path": ["id"], "type": "int64"},
      {"path": ["u"], "type": "uint64"},
      {"path": ["f"], "type": "float64"},
      {"path": ["s"], "type": "string"},
      {"path": ["b"], "type": "bytes"},
      {"path": ["g"], "type": "uuid"},
      {"path": ["t"], "type": "timestamp"},
      {"path": ["x"], "type": "bool"}
    ]"""
    db = typra.Database.open(str(path))
    db.register_collection("p", fields, "id")
    row = {
        "id": -1,
        "u": 42,
        "f": 2.5,
        "s": "hi",
        "b": b"\x00\xff",
        "g": uuid.UUID("12345678-1234-5678-1234-567812345678"),
        "t": 1_700_000_000_000_000,
        "x": True,
    }
    db.insert("p", row)
    got = db.get("p", -1)
    assert got is not None
    assert got["id"] == -1
    assert got["u"] == 42
    assert got["f"] == 2.5
    assert got["s"] == "hi"
    assert got["b"] == b"\x00\xff"
    assert got["g"] == uuid.UUID("12345678-1234-5678-1234-567812345678")
    assert got["t"] == 1_700_000_000_000_000
    assert got["x"] is True
