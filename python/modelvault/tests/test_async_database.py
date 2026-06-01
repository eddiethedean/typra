"""Asyncio API: AsyncDatabase, transactions, and modelvault.models.async_collection."""

from __future__ import annotations

from dataclasses import dataclass

import modelvault
import pytest


@pytest.mark.asyncio
async def test_async_in_memory_insert_get() -> None:
    db = await modelvault.AsyncDatabase.open_in_memory()
    fields = """[
      {"path": ["title"], "type": "string"},
      {"path": ["year"], "type": "int64"}
    ]"""
    await db.register_collection("books", fields, "title")
    await db.insert("books", {"title": "Async", "year": 2025})
    row = await db.get("books", "Async")
    assert row is not None
    assert row["title"] == "Async"
    assert row["year"] == 2025


@pytest.mark.asyncio
async def test_async_transaction_commit_and_rollback() -> None:
    db = await modelvault.AsyncDatabase.open_in_memory()
    await db.register_collection(
        "t",
        '[{"path": ["k"], "type": "string"}]',
        "k",
    )
    async with db.transaction():
        await db.insert("t", {"k": "committed"})
    assert await db.get("t", "committed") is not None

    with pytest.raises(ValueError, match="rollback"):
        async with db.transaction():
            await db.insert("t", {"k": "rolled"})
            raise ValueError("rollback me")
    assert await db.get("t", "rolled") is None


@dataclass
class Book:
    __modelvault_collection__ = "books"
    __modelvault_primary_key__ = "title"

    title: str
    year: int


@pytest.mark.asyncio
async def test_async_model_collection_roundtrip() -> None:
    db = await modelvault.AsyncDatabase.open_in_memory()
    books = modelvault.models.async_collection(db, Book)
    await books.insert(Book(title="Myth", year=1))
    got = await books.get("Myth")
    assert got is not None
    assert got.title == "Myth"
    rows = await books.where(Book.title, "Myth").all()
    assert len(rows) == 1
    assert rows[0].year == 1


@dataclass
class User:
    __modelvault_collection__ = "users"
    __modelvault_primary_key__ = "id"

    id: int
    name: str


@pytest.mark.asyncio
async def test_async_model_query_select() -> None:
    db = await modelvault.AsyncDatabase.open_in_memory()
    users = modelvault.models.async_collection(db, User)
    await users.insert(User(id=1, name="Ada"))
    rows = await users.where("id", 1).select(["id", "name"]).all()
    assert len(rows) == 1
    assert rows[0].id == 1
    assert rows[0].name == "Ada"
