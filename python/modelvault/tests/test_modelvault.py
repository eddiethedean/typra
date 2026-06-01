"""Smoke tests for the ``modelvault`` native module: metadata, exports, and ``Database`` constructors."""

from __future__ import annotations

import re

import pytest

import modelvault


def test_module_docstring() -> None:
    assert isinstance(modelvault.__doc__, str)
    assert len(modelvault.__doc__) > 0
    assert "ModelVault" in modelvault.__doc__
    assert "Database" in modelvault.__doc__
    assert "register_collection" in modelvault.__doc__


def test_version_is_semver() -> None:
    v = modelvault.__version__
    assert isinstance(v, str)
    # Workspace / Cargo release versions (e.g. 0.15.0)
    assert re.match(r"^\d+\.\d+\.\d+", v), f"unexpected __version__: {v!r}"


def test_version_parts_numeric() -> None:
    major, minor, patch, *_rest = modelvault.__version__.split(".")
    assert major.isdigit()
    assert minor.isdigit()
    assert patch.split("+")[0].split("-")[0].isdigit()


def test_module_has_expected_attributes() -> None:
    assert hasattr(modelvault, "__version__")
    assert hasattr(modelvault, "__doc__")


@pytest.mark.parametrize("name", ("__version__", "__doc__"))
def test_attributes_are_not_none(name: str) -> None:
    assert getattr(modelvault, name) is not None


def test_register_collection_invalid_json_raises(tmp_path) -> None:
    db = modelvault.Database.open(str(tmp_path / "badjson.modelvault"))
    with pytest.raises(ValueError, match="."):
        db.register_collection("x", "not json", "a")


def test_register_collection_not_array_raises(tmp_path) -> None:
    db = modelvault.Database.open(str(tmp_path / "notarr.modelvault"))
    with pytest.raises(ValueError, match="."):
        db.register_collection("x", '{"path": ["a"], "type": "string"}', "a")


def test_register_collection_unknown_primitive_type_raises(tmp_path) -> None:
    db = modelvault.Database.open(str(tmp_path / "badtype.modelvault"))
    with pytest.raises(ValueError) as excinfo:
        db.register_collection("x", '[{"path": ["a"], "type": "not_a_primitive"}]', "a")
    assert (
        "not_a_primitive" in str(excinfo.value)
        or "unknown" in str(excinfo.value).lower()
    )


def test_register_duplicate_collection_name_raises(tmp_path) -> None:
    path = tmp_path / "dup.modelvault"
    db = modelvault.Database.open(str(path))
    fields = '[{"path": ["t"], "type": "string"}]'
    db.register_collection("same", fields, "t")
    with pytest.raises(ValueError, match="."):
        db.register_collection("same", fields, "t")


def test_database_register_collection_roundtrip(tmp_path) -> None:
    path = tmp_path / "t.modelvault"
    db = modelvault.Database.open(str(path))
    assert path.exists()
    fields = '[{"path": ["title"], "type": "string"}]'
    cid, ver = db.register_collection("books", fields, "title")
    assert cid == 1
    assert ver == 1
    assert db.collection_names() == ["books"]
    del db

    db2 = modelvault.Database.open(str(path))
    assert db2.collection_names() == ["books"]


def test_transaction_context_manager_commits(tmp_path) -> None:
    path = tmp_path / "txnctx.modelvault"
    db = modelvault.Database.open(str(path))
    fields = '[{"path": ["title"], "type": "string"}]'
    db.register_collection("books", fields, "title")
    with db.transaction():
        db.insert("books", {"title": "one"})
        db.insert("books", {"title": "two"})
    assert db.get("books", "one") == {"title": "one"}
    assert db.get("books", "two") == {"title": "two"}


def test_transaction_context_manager_rolls_back_on_exception(tmp_path) -> None:
    path = tmp_path / "txnabort.modelvault"
    db = modelvault.Database.open(str(path))
    fields = '[{"path": ["title"], "type": "string"}]'
    db.register_collection("books", fields, "title")
    with pytest.raises(RuntimeError):
        with db.transaction():
            db.insert("books", {"title": "gone"})
            raise RuntimeError("user abort")
    assert db.get("books", "gone") is None
