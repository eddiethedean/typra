from __future__ import annotations

import typra


def test_query_builder_where_limit_all_and_explain() -> None:
    db = typra.Database.open_in_memory()
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
