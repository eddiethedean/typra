import pathlib
from typing import Any, cast

import pytest

import typra


def test_dbapi_accepts_list_params(tmp_path: pathlib.Path) -> None:
    p = tmp_path / "t.typra"
    db = typra.Database.open(str(p))
    fields = """[
      {"path": ["id"], "type": "string"},
      {"path": ["year"], "type": "int64"},
      {"path": ["note"], "type": {"optional": "string"}}
    ]"""
    db.register_collection("t", fields, "id")
    db.insert("t", {"id": "k1", "year": 2020})

    conn = typra.dbapi.connect(str(p))
    cur = conn.cursor()
    cur.execute("SELECT id, note FROM t WHERE id = ?", ["k1"])
    assert cur.fetchone() == ("k1", None)


def test_dbapi_rejects_params_not_list_or_tuple(tmp_path: pathlib.Path) -> None:
    p = tmp_path / "t.typra"
    typra.Database.open(str(p))
    conn = typra.dbapi.connect(str(p))
    cur = conn.cursor()
    with pytest.raises(ValueError, match="params must be a tuple or list"):
        # Intentionally wrong type; runtime should reject it, regardless of static types.
        bad_params = cast(Any, {"id": "k"})
        cur.execute("SELECT * FROM t", params=bad_params)


def test_dbapi_execute_after_cursor_close_raises(tmp_path: pathlib.Path) -> None:
    p = tmp_path / "t.typra"
    typra.Database.open(str(p))
    conn = typra.dbapi.connect(str(p))
    cur = conn.cursor()
    cur.close()
    with pytest.raises(RuntimeError, match="cursor is closed"):
        cur.execute("SELECT * FROM t")


def test_dbapi_connection_close_prevents_new_cursor(tmp_path: pathlib.Path) -> None:
    p = tmp_path / "t.typra"
    typra.Database.open(str(p))
    conn = typra.dbapi.connect(str(p))
    conn.close()
    with pytest.raises(RuntimeError, match="connection is closed"):
        conn.cursor()


def test_dbapi_select_star_column_order_matches_schema_field_order(
    tmp_path: pathlib.Path,
) -> None:
    p = tmp_path / "t.typra"
    db = typra.Database.open(str(p))

    # Intentionally non-alphabetical field order; `SELECT *` should follow schema order.
    fields = """[
      {"path": ["b"], "type": "string"},
      {"path": ["a"], "type": "int64"},
      {"path": ["c"], "type": "bool"}
    ]"""
    db.register_collection("t", fields, "b")
    db.insert("t", {"b": "k", "a": 1, "c": True})

    conn = typra.dbapi.connect(str(p))
    cur = conn.cursor()
    cur.execute("SELECT * FROM t WHERE b = ?", ("k",))
    assert cur.description is not None
    assert [d[0] for d in cur.description] == ["b", "a", "c"]
    assert cur.fetchone() == ("k", 1, True)
