"""Insert validation, nested rows, constraints, and parity with Rust error classes (ValueError)."""

from __future__ import annotations

import pytest

import typra


def test_nested_object_and_list_roundtrip(tmp_path) -> None:
    db = typra.Database.open(str(tmp_path / "nest.typra"))
    fields = """[
      {"path": ["id"], "type": "string"},
      {"path": ["meta"], "type": {"object": [
        {"path": ["label"], "type": "string"}
      ]}},
      {"path": ["tags"], "type": {"list": "string"}},
      {"path": ["mode"], "type": {"enum": ["read", "write"]}}
    ]"""
    db.register_collection("c", fields, "id")
    db.insert(
        "c",
        {
            "id": "a1",
            "meta": {"label": "hello"},
            "tags": ["x", "y"],
            "mode": "write",
        },
    )
    row = db.get("c", "a1")
    assert row is not None
    assert row["id"] == "a1"
    assert row["meta"] == {"label": "hello"}
    assert row["tags"] == ["x", "y"]
    assert row["mode"] == "write"


def test_optional_field_omitted_and_null(tmp_path) -> None:
    db = typra.Database.open(str(tmp_path / "opt.typra"))
    fields = """[
      {"path": ["k"], "type": "string"},
      {"path": ["extra"], "type": {"optional": "string"}}
    ]"""
    db.register_collection("t", fields, "k")
    db.insert("t", {"k": "key1"})
    r = db.get("t", "key1")
    assert r is not None
    assert r.get("extra") is None


def test_constraint_min_int_fails(tmp_path) -> None:
    db = typra.Database.open(str(tmp_path / "min.typra"))
    fields = """[
      {"path": ["name"], "type": "string"},
      {"path": ["year"], "type": "int64", "constraints": [{"min_i64": 2000}]}
    ]"""
    db.register_collection("b", fields, "name")
    with pytest.raises(ValueError):
        db.insert("b", {"name": "x", "year": 1990})


def test_enum_invalid_variant_fails(tmp_path) -> None:
    db = typra.Database.open(str(tmp_path / "enum.typra"))
    fields = """[
      {"path": ["id"], "type": "string"},
      {"path": ["s"], "type": {"enum": ["on", "off"]}}
    ]"""
    db.register_collection("e", fields, "id")
    with pytest.raises(ValueError):
        db.insert("e", {"id": "1", "s": "maybe"})


def test_unknown_top_level_key_fails(tmp_path) -> None:
    db = typra.Database.open(str(tmp_path / "unk.typra"))
    fields = '[{"path": ["a"], "type": "string"}, {"path": ["b"], "type": "int64"}]'
    db.register_collection("t", fields, "a")
    with pytest.raises(ValueError):
        db.insert("t", {"a": "k", "b": 1, "nope": True})
