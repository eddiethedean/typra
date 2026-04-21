"""Tests for the native `typra` extension module."""

from __future__ import annotations

import re

import pytest

import typra


def test_module_docstring() -> None:
    assert isinstance(typra.__doc__, str)
    assert len(typra.__doc__) > 0
    assert "Typra" in typra.__doc__
    assert "Database" in typra.__doc__
    assert "register_collection" in typra.__doc__


def test_version_is_semver() -> None:
    v = typra.__version__
    assert isinstance(v, str)
    # Workspace / Cargo release versions (e.g. 0.1.0)
    assert re.match(r"^\d+\.\d+\.\d+", v), f"unexpected __version__: {v!r}"


def test_version_parts_numeric() -> None:
    major, minor, patch, *_rest = typra.__version__.split(".")
    assert major.isdigit()
    assert minor.isdigit()
    assert patch.split("+")[0].split("-")[0].isdigit()


def test_module_has_expected_attributes() -> None:
    assert hasattr(typra, "__version__")
    assert hasattr(typra, "__doc__")


@pytest.mark.parametrize("name", ("__version__", "__doc__"))
def test_attributes_are_not_none(name: str) -> None:
    assert getattr(typra, name) is not None


def test_register_collection_invalid_json_raises(tmp_path) -> None:
    db = typra.Database.open(str(tmp_path / "badjson.typra"))
    with pytest.raises(ValueError, match="."):
        db.register_collection("x", "not json")


def test_register_collection_not_array_raises(tmp_path) -> None:
    db = typra.Database.open(str(tmp_path / "notarr.typra"))
    with pytest.raises(ValueError, match="."):
        db.register_collection("x", '{"path": ["a"], "type": "string"}')


def test_register_collection_unknown_primitive_type_raises(tmp_path) -> None:
    db = typra.Database.open(str(tmp_path / "badtype.typra"))
    with pytest.raises(ValueError) as excinfo:
        db.register_collection("x", '[{"path": ["a"], "type": "not_a_primitive"}]')
    assert (
        "not_a_primitive" in str(excinfo.value)
        or "unknown" in str(excinfo.value).lower()
    )


def test_register_duplicate_collection_name_raises(tmp_path) -> None:
    path = tmp_path / "dup.typra"
    db = typra.Database.open(str(path))
    fields = '[{"path": ["t"], "type": "string"}]'
    db.register_collection("same", fields)
    with pytest.raises(ValueError, match="."):
        db.register_collection("same", fields)


def test_database_register_collection_roundtrip(tmp_path) -> None:
    path = tmp_path / "t.typra"
    db = typra.Database.open(str(path))
    assert path.exists()
    fields = '[{"path": ["title"], "type": "string"}]'
    cid, ver = db.register_collection("books", fields)
    assert cid == 1
    assert ver == 1
    assert db.collection_names() == ["books"]
    del db

    db2 = typra.Database.open(str(path))
    assert db2.collection_names() == ["books"]
