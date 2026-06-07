"""Regression tests for security/correctness audit fixes."""

from __future__ import annotations

import enum
import sys
import threading
from dataclasses import dataclass
from typing import Optional

import modelvault
import pytest


def test_write_blocked_during_open_transaction_from_other_thread() -> None:
    """C1: autocommit writes from another thread must not run during an open transaction."""
    db = modelvault.Database.open_in_memory()
    db.register_collection(
        "t",
        '[{"path": ["k"], "type": "string"}]',
        "k",
    )
    entered = threading.Event()
    errors: list[BaseException] = []

    def foreign_insert() -> None:
        try:
            entered.wait(timeout=2.0)
            db.insert("t", {"k": "foreign"})
        except BaseException as e:  # noqa: BLE001
            errors.append(e)

    with db.transaction():
        entered.set()
        t = threading.Thread(target=foreign_insert)
        t.start()
        t.join(timeout=2.0)
        assert not t.is_alive()

    assert len(errors) == 1
    assert isinstance(errors[0], RuntimeError)
    assert "transaction is open" in str(errors[0])


@dataclass
class Book:
    __modelvault_collection__ = "books"
    __modelvault_primary_key__ = "title"

    title: str
    year: int


@pytest.mark.asyncio
async def test_async_model_collection_all_returns_instances() -> None:
    """H4: AsyncModelCollection.all() hydrates model instances."""
    db = await modelvault.AsyncDatabase.open_in_memory()
    books = modelvault.models.async_collection(db, Book)
    await books.insert(Book(title="Audit", year=2026))
    rows = await books.all()
    assert len(rows) == 1
    assert isinstance(rows[0], Book)
    assert rows[0].title == "Audit"


class Mode(enum.Enum):
    READ = "read"
    WRITE = "write"


@dataclass
class Doc:
    __modelvault_collection__ = "docs"
    __modelvault_primary_key__ = "id"

    id: str
    mode: Mode


def test_enum_schema_uses_member_value() -> None:
    """H5: Python Enum fields register variant values, not member names."""
    db = modelvault.Database.open_in_memory()
    docs = modelvault.models.collection(db, Doc)
    docs.insert(Doc(id="a", mode=Mode.READ))
    got = docs.get("a")
    assert got is not None
    assert got.mode in (Mode.READ, "read")


@dataclass
class Item:
    __modelvault_collection__ = "items"
    __modelvault_primary_key__ = "id"

    id: str
    note: Optional[str] = None


def test_where_on_optional_field() -> None:
    """M11: optional scalar fields work in where()."""
    db = modelvault.Database.open_in_memory()
    items = modelvault.models.collection(db, Item)
    items.insert(Item(id="a", note="hello"))
    items.insert(Item(id="b"))
    rows = items.where("note", "hello").all()
    assert len(rows) == 1
    assert rows[0].id == "a"


@dataclass
class Modern:
    __modelvault_collection__ = "modern"
    __modelvault_primary_key__ = "id"

    id: str
    rating: float | None = None


@pytest.mark.skipif(
    sys.version_info < (3, 10),
    reason="PEP 604 union syntax requires Python 3.10+",
)
def test_pep604_optional_union_schema() -> None:
    """M12: PEP 604 ``float | None`` maps to optional schema."""
    db = modelvault.Database.open_in_memory()
    modern = modelvault.models.collection(db, Modern)
    modern.insert(Modern(id="x", rating=1.5))
    got = modern.get("x")
    assert got is not None
    assert got.rating == 1.5
