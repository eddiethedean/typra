from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

import typra


def test_register_indexes_json_explain_and_subset_projection() -> None:
    db = typra.Database.open_in_memory()
    fields = (
        '[{"path": ["title"], "type": "string"}, {"path": ["year"], "type": "int64"}]'
    )
    indexes = '[{"name": "title_idx", "path": ["title"], "kind": "index"}]'
    db.register_collection("books", fields, "title", indexes)
    db.insert("books", {"title": "Hello", "year": 2020})

    explain = db.collection("books").where("title", "Hello").explain()
    assert "IndexLookup" in explain

    rows = db.collection("books").where("title", "Hello").all(fields=["title"])
    assert rows == [{"title": "Hello"}]


def test_unique_index_violation_is_value_error() -> None:
    db = typra.Database.open_in_memory()
    fields = (
        '[{"path": ["id"], "type": "int64"}, {"path": ["email"], "type": "string"}]'
    )
    indexes = '[{"name": "email_u", "path": ["email"], "kind": "unique"}]'
    db.register_collection("users", fields, "id", indexes)
    db.insert("users", {"id": 1, "email": "a@x.test"})
    with pytest.raises(ValueError):
        db.insert("users", {"id": 2, "email": "a@x.test"})


def test_indexes_survive_reopen_on_disk() -> None:
    with tempfile.TemporaryDirectory() as d:
        path = Path(d) / "t.typra"
        db = typra.Database.open(str(path))
        fields = '[{"path": ["title"], "type": "string"}, {"path": ["year"], "type": "int64"}]'
        indexes = '[{"name": "title_idx", "path": ["title"], "kind": "index"}]'
        db.register_collection("books", fields, "title", indexes)
        db.insert("books", {"title": "X", "year": 1})

        db2 = typra.Database.open(str(path))
        explain = db2.collection("books").where("title", "X").explain()
        assert "IndexLookup" in explain


def test_nested_object_field_can_be_indexed_and_queried() -> None:
    db = typra.Database.open_in_memory()
    fields = """[
      {"path": ["id"], "type": "int64"},
      {"path": ["profile"], "type": {"object": [
        {"path": ["email"], "type": "string"}
      ]}}
    ]"""
    indexes = '[{"name": "email_idx", "path": ["profile", "email"], "kind": "index"}]'
    db.register_collection("users", fields, "id", indexes)
    db.insert("users", {"id": 1, "profile": {"email": "a@x.test"}})

    q = db.collection("users").where(("profile", "email"), "a@x.test")
    assert q.all() == [{"id": 1, "profile": {"email": "a@x.test"}}]
    assert "IndexLookup" in q.explain()


def test_and_where_builds_conjunctive_predicate() -> None:
    db = typra.Database.open_in_memory()
    fields = (
        '[{"path": ["title"], "type": "string"}, {"path": ["year"], "type": "int64"}]'
    )
    indexes = (
        '[{"name": "title_idx", "path": ["title"], "kind": "index"},'
        '{"name": "year_idx", "path": ["year"], "kind": "index"}]'
    )
    db.register_collection("books", fields, "title", indexes)
    db.insert("books", {"title": "A", "year": 2000})
    db.insert("books", {"title": "B", "year": 2000})

    q = db.collection("books").where("title", "A").and_where("year", 2000)
    rows = q.all()
    assert rows == [{"title": "A", "year": 2000}]
    assert "IndexLookup" in q.explain()
