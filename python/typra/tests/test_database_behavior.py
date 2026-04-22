"""Paths, reopen persistence, and catalog behavior across ``Database.open`` and in-memory APIs."""

from __future__ import annotations

from pathlib import Path

import pytest

import typra


def test_database_path_reflects_open_path(tmp_path) -> None:
    path = tmp_path / "nested" / "db.typra"
    path.parent.mkdir(parents=True)
    db = typra.Database.open(str(path))
    assert Path(db.path()).resolve() == path.resolve()


def test_open_directory_raises_oserror(tmp_path) -> None:
    d = tmp_path / "dir"
    d.mkdir()
    with pytest.raises(OSError):
        typra.Database.open(str(d))


def test_open_without_parent_directory_raises_oserror(tmp_path) -> None:
    path = tmp_path / "missing" / "parent" / "db.typra"
    with pytest.raises(OSError):
        typra.Database.open(str(path))


def test_register_trims_whitespace_around_collection_name(tmp_path) -> None:
    db = typra.Database.open(str(tmp_path / "trim.typra"))
    fields = '[{"path": ["x"], "type": "string"}]'
    db.register_collection("  books  ", fields, "x")
    assert db.collection_names() == ["books"]


def test_collection_names_are_sorted_alphabetically(tmp_path) -> None:
    db = typra.Database.open(str(tmp_path / "sort.typra"))
    f = '[{"path": ["a"], "type": "string"}]'
    db.register_collection("zebra", f, "a")
    db.register_collection("apple", f, "a")
    db.register_collection("mango", f, "a")
    assert db.collection_names() == ["apple", "mango", "zebra"]


def test_multiple_collections_stable_ids(tmp_path) -> None:
    db = typra.Database.open(str(tmp_path / "ids.typra"))
    f = '[{"path": ["a"], "type": "string"}]'
    assert db.register_collection("a", f, "a") == (1, 1)
    assert db.register_collection("b", f, "a") == (2, 1)
    assert db.register_collection("c", f, "a") == (3, 1)


def test_register_empty_name_after_trim_raises(tmp_path) -> None:
    db = typra.Database.open(str(tmp_path / "emptyname.typra"))
    with pytest.raises(ValueError, match="."):
        db.register_collection("", '[{"path": ["a"], "type": "string"}]', "a")


def test_register_whitespace_only_name_raises(tmp_path) -> None:
    db = typra.Database.open(str(tmp_path / "wsname.typra"))
    with pytest.raises(ValueError, match="."):
        db.register_collection("   ", '[{"path": ["a"], "type": "string"}]', "a")


def test_reopen_preserves_multiple_collections(tmp_path) -> None:
    path = tmp_path / "multi.typra"
    f = '[{"path": ["k"], "type": "int64"}]'
    db = typra.Database.open(str(path))
    db.register_collection("first", f, "k")
    db.register_collection("second", f, "k")
    del db

    db2 = typra.Database.open(str(path))
    assert db2.collection_names() == ["first", "second"]
    cid, ver = db2.register_collection("third", f, "k")
    assert cid == 3
    assert ver == 1
