"""Schema registration via JSON: primitives, composites, and validation errors from ``register_collection``."""

from __future__ import annotations

import uuid

import pytest

import modelvault


def test_register_all_primitive_types_in_one_collection(tmp_path) -> None:
    db = modelvault.Database.open(str(tmp_path / "prim.modelvault"))
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
    db.register_collection("t", payload, "a")
    row = {
        "a": True,
        "b": -1,
        "c": 42,
        "d": 2.5,
        "e": "hi",
        "f": b"\x00\xff",
        "g": uuid.UUID("12345678-1234-5678-1234-567812345678"),
        "h": 1_700_000_000_000_000,
    }
    db.insert("t", row)
    got = db.get("t", True)
    assert got is not None
    assert got["b"] == -1
    assert got["g"] == uuid.UUID("12345678-1234-5678-1234-567812345678")


def test_register_optional_list_object_enum(tmp_path) -> None:
    db = modelvault.Database.open(str(tmp_path / "nest.modelvault"))
    fields = """[
      {"path": ["opt"], "type": {"optional": "string"}},
      {"path": ["items"], "type": {"list": "int64"}},
      {"path": ["meta"], "type": {"object": [
        {"path": ["x"], "type": "string"}
      ]}},
      {"path": ["state"], "type": {"enum": ["on", "off"]}}
    ]"""
    db.register_collection("complex", fields, "opt")
    path = tmp_path / "nest.modelvault"
    del db
    db2 = modelvault.Database.open(str(path))
    assert db2.collection_names() == ["complex"]


def test_empty_fields_array_rejected_for_primary(tmp_path) -> None:
    db = modelvault.Database.open(str(tmp_path / "emptyfields.modelvault"))
    with pytest.raises(modelvault.ModelVaultSchemaError, match="primary field"):
        db.register_collection("empty_schema", "[]", "id")


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
    db = modelvault.Database.open(str(tmp_path / "val.modelvault"))
    with pytest.raises(ValueError) as exc:
        db.register_collection("x", bad_json, "a")
    assert needle.lower() in str(exc.value).lower()


def test_nested_path_segments_rejected_as_primary(tmp_path) -> None:
    db = modelvault.Database.open(str(tmp_path / "deep.modelvault"))
    fields = '[{"path": ["profile", "addr", "zip"], "type": "string"}]'
    with pytest.raises(modelvault.ModelVaultSchemaError, match="primary field"):
        db.register_collection("users", fields, "profile")
