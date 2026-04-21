"""Tests for ``fields_json`` parsing and composite types."""

from __future__ import annotations

import pytest

import typra


def test_register_all_primitive_types_in_one_collection(tmp_path) -> None:
    db = typra.Database.open(str(tmp_path / "prim.typra"))
    payload = (
        '[{"path": ["a"], "type": "bool"},'
        '{"path": ["b"], "type": "int64"},'
        '{"path": ["c"], "type": "uint64"},'
        '{"path": ["d"], "type": "float64"},'
        '{"path": ["e"], "type": "string"},'
        '{"path": ["f"], "type": "bytes"},'
        '{"path": ["g"], "type": "uuid"},'
        '{"path": ["h"], "type": "timestamp"}]'
    )
    db.register_collection("t", payload)
    assert db.collection_names() == ["t"]


def test_register_optional_list_object_enum(tmp_path) -> None:
    db = typra.Database.open(str(tmp_path / "nest.typra"))
    fields = """[
      {"path": ["opt"], "type": {"optional": "string"}},
      {"path": ["items"], "type": {"list": "int64"}},
      {"path": ["meta"], "type": {"object": [
        {"path": ["x"], "type": "string"}
      ]}},
      {"path": ["state"], "type": {"enum": ["on", "off"]}}
    ]"""
    db.register_collection("complex", fields)
    path = tmp_path / "nest.typra"
    del db
    db2 = typra.Database.open(str(path))
    assert db2.collection_names() == ["complex"]


def test_empty_fields_array_allowed(tmp_path) -> None:
    db = typra.Database.open(str(tmp_path / "emptyfields.typra"))
    db.register_collection("empty_schema", "[]")
    assert db.collection_names() == ["empty_schema"]


@pytest.mark.parametrize(
    ("bad_json", "needle"),
    [
        ("{}", "array"),
        ("[true]", "object"),
        ('[{"path": ["a"]}]', "type"),
        ('[{"type": "string"}]', "path"),
        ('[{"path": "a", "type": "string"}]', "array"),
        ('[{"path": [1], "type": "string"}]', "string"),
        ('[{"path": ["a"], "type": 1}]', "string or object"),
        ('[{"path": [""], "type": "string"}]', "path"),
        ('[{"path": ["a"], "type": {"enum": [1, 2]}}]', "string"),
        ('[{"path": ["a"], "type": {"unknown": true}}]', "unsupported"),
    ],
)
def test_fields_json_validation_errors(tmp_path, bad_json: str, needle: str) -> None:
    db = typra.Database.open(str(tmp_path / "val.typra"))
    with pytest.raises(ValueError) as exc:
        db.register_collection("x", bad_json)
    assert needle.lower() in str(exc.value).lower()


def test_nested_path_segments(tmp_path) -> None:
    db = typra.Database.open(str(tmp_path / "deep.typra"))
    fields = '[{"path": ["profile", "addr", "zip"], "type": "string"}]'
    db.register_collection("users", fields)
    assert db.collection_names() == ["users"]
