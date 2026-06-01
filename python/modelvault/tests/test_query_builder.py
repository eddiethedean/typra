from __future__ import annotations

import modelvault


def test_query_builder_where_limit_all_and_explain() -> None:
    db = modelvault.Database.open_in_memory()
    fields = (
        '[{"path": ["title"], "type": "string"}, {"path": ["year"], "type": "int64"}]'
    )
    db.register_collection("books", fields, "title")

    db.insert("books", {"title": "Hello", "year": 2020})
    db.insert("books", {"title": "World", "year": 2021})

    q = db.collection("books").where("title", "Hello").limit(10)
    explain = q.explain()
    assert "Plan:" in explain

    rows = q.all()
    assert rows == [{"title": "Hello", "year": 2020}]


def test_query_builder_range_order_and_or() -> None:
    db = modelvault.Database.open_in_memory()
    fields = (
        '[{"path": ["title"], "type": "string"}, {"path": ["year"], "type": "int64"}]'
    )
    db.register_collection("books", fields, "title")
    db.insert("books", {"title": "A", "year": 2019})
    db.insert("books", {"title": "B", "year": 2021})
    db.insert("books", {"title": "C", "year": 2023})

    rows = (
        db.collection("books").gte_where("year", 2021).order_by("year", desc=True).all()
    )
    assert [r["title"] for r in rows] == ["C", "B"]

    q1 = db.collection("books").where("title", "A")
    q2 = db.collection("books").where("title", "C")
    rows = q1.or_where(q2).all()
    assert {r["title"] for r in rows} == {"A", "C"}
