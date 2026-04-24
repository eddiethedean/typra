from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, Optional

import pytest

import typra


@dataclass
class Book:
    __typra_primary_key__ = "title"
    __typra_indexes__ = [
        typra.models.index("year"),
        typra.models.unique("title"),
    ]

    title: str
    year: Annotated[int, typra.models.constrained(min_i64=0)]
    rating: Optional[float] = None


@dataclass
class OrderLine:
    __typra_primary_key__ = "id"

    id: int
    sku: str


def test_models_dataclass_register_insert_get_and_query_roundtrip() -> None:
    db = typra.Database.open_in_memory()

    books = typra.models.collection(db, Book)
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
    db = typra.Database.open_in_memory()
    books = typra.models.collection(db, Book)
    with pytest.raises(ValueError):
        books.insert(Book(title="Bad", year=-1))


def test_models_plan_and_apply_schema_version() -> None:
    db = typra.Database.open_in_memory()
    _ = typra.models.collection(db, Book)
    _plan = typra.models.plan(db, Book)
    ver = typra.models.apply(db, Book, force=False)
    assert isinstance(ver, int)


def test_models_default_collection_naming_snake_case_plural() -> None:
    db = typra.Database.open_in_memory()
    ol = typra.models.collection(db, OrderLine)
    assert ol.name == "order_lines"


def test_models_requires_explicit_primary_key_marker() -> None:
    db = typra.Database.open_in_memory()

    @dataclass
    class MissingPk:
        x: int

    with pytest.raises(ValueError):
        typra.models.collection(db, MissingPk)


def test_models_pydantic_optional_if_installed() -> None:
    pydantic = pytest.importorskip("pydantic")

    class User(pydantic.BaseModel):
        __typra_primary_key__ = "id"
        __typra_indexes__ = [typra.models.unique("id")]

        id: int
        name: str

    db = typra.Database.open_in_memory()
    users = typra.models.collection(db, User)
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
        __typra_primary_key__ = "id"
        __typra_indexes__ = [
            typra.models.unique("id"),
            typra.models.index("age"),
        ]

        id: int
        age: Annotated[int, typra.models.constrained(min_i64=0)]
        name: str

    db = typra.Database.open_in_memory()
    users = typra.models.collection(db, User)

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

    _plan = typra.models.plan(db, User)
    ver = typra.models.apply(db, User, force=False)
    assert isinstance(ver, int)
