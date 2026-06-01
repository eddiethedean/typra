"""ModelCollection.get accepts model instances (parity with delete)."""

from __future__ import annotations

from dataclasses import dataclass

import modelvault


@dataclass
class Book:
    __modelvault_primary_key__ = "title"

    title: str
    year: int


def test_get_accepts_model_instance() -> None:
    db = modelvault.Database.open_in_memory()
    books = modelvault.models.collection(db, Book)
    b = Book(title="Hello", year=2020)
    books.insert(b)
    got = books.get(b)
    assert got is not None
    assert got.title == "Hello"
