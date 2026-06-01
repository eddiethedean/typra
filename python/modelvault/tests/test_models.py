from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Annotated, Optional

import gc
import pytest

import modelvault


@dataclass
class Book:
    __modelvault_primary_key__ = "title"
    __modelvault_indexes__ = [
        modelvault.models.index("year"),
        modelvault.models.unique("title"),
    ]

    title: str
    year: Annotated[int, modelvault.models.constrained(min_i64=0)]
    rating: Optional[float] = None


@dataclass
class OrderLine:
    __modelvault_primary_key__ = "id"

    id: int
    sku: str


def test_models_dataclass_register_insert_get_and_query_roundtrip() -> None:
    db = modelvault.Database.open_in_memory()

    books = modelvault.models.collection(db, Book)
    assert books.name == "books"

    books.insert(Book(title="Hello", year=2020, rating=4.5))
    got = books.get("Hello")
    assert got is not None
    assert got.title == "Hello"
    assert got.year == 2020
    assert got.rating == 4.5

    rows = books.where("title", "Hello").all()
    assert len(rows) == 1
    assert rows[0].title == "Hello"

    rows2 = books.where(Book.title, "Hello").all()
    assert len(rows2) == 1

    books.update("Hello", {"rating": 5.0})
    got2 = books.get("Hello")
    assert got2 is not None
    assert got2.rating == 5.0


def test_models_constraints_surface_engine_value_error() -> None:
    db = modelvault.Database.open_in_memory()
    books = modelvault.models.collection(db, Book)
    with pytest.raises(ValueError):
        books.insert(Book(title="Bad", year=-1))


def test_models_plan_and_apply_schema_version() -> None:
    db = modelvault.Database.open_in_memory()
    _ = modelvault.models.collection(db, Book)
    _plan = modelvault.models.plan(db, Book)
    ver = modelvault.models.apply(db, Book, force=False)
    assert isinstance(ver, int)


def test_models_default_collection_naming_snake_case_plural() -> None:
    db = modelvault.Database.open_in_memory()
    ol = modelvault.models.collection(db, OrderLine)
    assert ol.name == "order_lines"


def test_models_requires_explicit_primary_key_marker() -> None:
    db = modelvault.Database.open_in_memory()

    @dataclass
    class MissingPk:
        x: int

    with pytest.raises(ValueError):
        modelvault.models.collection(db, MissingPk)


def test_models_pydantic_optional_if_installed() -> None:
    pydantic = pytest.importorskip("pydantic")

    class User(pydantic.BaseModel):
        __modelvault_primary_key__ = "id"
        __modelvault_indexes__ = [modelvault.models.unique("id")]

        id: int
        name: str

    db = modelvault.Database.open_in_memory()
    users = modelvault.models.collection(db, User)
    assert users.name == "users"

    users.insert(User(id=1, name="Ada"))
    got = users.get(1)
    assert got is not None
    assert got.id == 1
    assert got.name == "Ada"


def test_models_pydantic_constraints_update_select_and_plan_apply_if_installed() -> (
    None
):
    pydantic = pytest.importorskip("pydantic")

    class User(pydantic.BaseModel):
        __modelvault_primary_key__ = "id"
        __modelvault_indexes__ = [
            modelvault.models.unique("id"),
            modelvault.models.index("age"),
        ]

        id: int
        age: Annotated[int, modelvault.models.constrained(min_i64=0)]
        name: str

    db = modelvault.Database.open_in_memory()
    users = modelvault.models.collection(db, User)

    with pytest.raises(ValueError):
        users.insert(User(id=1, age=-1, name="Bad"))

    users.insert(User(id=1, age=10, name="Ada"))
    users.update(1, {"name": "Ada2"})
    got = users.get(1)
    assert got is not None
    assert got.name == "Ada2"

    rows = users.where("id", 1).select(["id", "name"]).all()
    assert len(rows) == 1
    assert rows[0].id == 1
    assert rows[0].name == "Ada2"

    _plan = modelvault.models.plan(db, User)
    ver = modelvault.models.apply(db, User, force=False)
    assert isinstance(ver, int)


@dataclass
class BookFull:
    __modelvault_primary_key__ = "id"
    __modelvault_collection__ = "books"

    id: int
    title: str
    year: int


@dataclass
class BookTitle:
    __modelvault_primary_key__ = "id"
    __modelvault_collection__ = "books"

    id: int
    title: str


def test_models_subset_class_targets_same_collection() -> None:
    db = modelvault.Database.open_in_memory()
    full = modelvault.models.collection(db, BookFull)
    full.insert(BookFull(id=1, title="Hello", year=2020))
    books = modelvault.models.collection(db, BookTitle)
    got = books.get(1)
    assert got is not None
    assert got.title == "Hello"
    rows = books.where("id", 1).all()
    assert len(rows) == 1
    assert rows[0].title == "Hello"


def test_models_incompatible_primary_key_raises_schema_error() -> None:
    db = modelvault.Database.open_in_memory()
    modelvault.models.collection(db, BookFull)

    @dataclass
    class BadBook:
        __modelvault_primary_key__ = "title"
        __modelvault_collection__ = "books"

        title: str
        year: int

    with pytest.raises(modelvault.ModelVaultSchemaError):
        modelvault.models.collection(db, BadBook)


def test_models_datetime_insert_and_roundtrip() -> None:
    @dataclass
    class Event:
        __modelvault_primary_key__ = "id"
        id: str
        published_at: datetime

    from datetime import timezone

    db = modelvault.Database.open_in_memory()
    events = modelvault.models.collection(db, Event)
    when = datetime(2020, 1, 2, 3, 4, 5, tzinfo=timezone.utc)
    events.insert(Event(id="e1", published_at=when))
    got = events.get("e1")
    assert got is not None
    assert got.published_at == when


def test_models_delete_by_pk_and_object() -> None:
    db = modelvault.Database.open_in_memory()
    books = modelvault.models.collection(db, Book)
    books.insert(Book(title="Hello", year=2020))
    books.delete("Hello")
    assert books.get("Hello") is None

    books.insert(Book(title="World", year=2021))
    books.delete(Book(title="World", year=2021))
    assert books.get("World") is None


def test_database_delete_and_read_only(tmp_path) -> None:
    path = tmp_path / "t.modelvault"
    db = modelvault.Database.open(str(path))
    fields = '[{"path":["id"],"type":"string"},{"path":["v"],"type":"int64"}]'
    db.register_collection("t", fields, "id")
    db.insert("t", {"id": "k1", "v": 1})
    db.delete("t", "k1")
    assert db.get("t", "k1") is None
    del db
    gc.collect()

    db2 = modelvault.Database.open(str(path), read_only=True)
    assert db2.get("t", "k1") is None


def test_database_rebuild_indexes(tmp_path) -> None:
    path = tmp_path / "idx.modelvault"
    db = modelvault.Database.open(str(path))
    fields = '[{"path":["id"],"type":"string"},{"path":["year"],"type":"int64"}]'
    indexes = '[{"name": "year_idx", "path": ["year"], "kind": "index"}]'
    db.register_collection("books", fields, "id", indexes)
    db.insert("books", {"id": "a", "year": 2020})
    db.rebuild_indexes("books")
    rows = db.collection("books").where("year", 2020).all()
    assert len(rows) == 1
    assert rows[0]["id"] == "a"
