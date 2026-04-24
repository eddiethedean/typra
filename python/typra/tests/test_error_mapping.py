import pathlib

import pytest

import typra


def test_open_garbage_file_raises_typra_format_error(tmp_path: pathlib.Path) -> None:
    p = tmp_path / "bad.typra"
    p.write_bytes(b"this is not a typra file")
    with pytest.raises(typra.TypraFormatError) as e:
        typra.Database.open(str(p))
    assert isinstance(e.value, ValueError)


def test_register_invalid_primary_key_raises_typra_schema_error() -> None:
    db = typra.Database.open_in_memory()
    fields_json = '[{"path":["title"],"type":"string"}]'
    with pytest.raises(typra.TypraSchemaError) as e:
        db.register_collection("books", fields_json, "id")
    assert isinstance(e.value, ValueError)


def test_insert_type_mismatch_raises_typra_validation_error(
    tmp_path: pathlib.Path,
) -> None:
    db = typra.Database.open(str(tmp_path / "t.typra"))
    # Use a constraint violation (correct type, invalid value) so the error comes from the engine
    # and is mapped via `DbError::Validation`.
    fields_json = """
    [
      {"path": ["id"], "type": "string"},
      {"path": ["year"], "type": "int64", "constraints": [{"min_i64": 2000}]}
    ]
    """
    db.register_collection("events", fields_json, "id")
    with pytest.raises(typra.TypraValidationError) as e:
        db.insert("events", {"id": "e1", "year": 1990})
    assert isinstance(e.value, ValueError)


def test_nested_transaction_raises_typra_transaction_error(
    tmp_path: pathlib.Path,
) -> None:
    db = typra.Database.open(str(tmp_path / "t.typra"))
    fields_json = '[{"path":["id"],"type":"string"}]'
    db.register_collection("x", fields_json, "id")
    with db.transaction():
        with pytest.raises(typra.TypraTransactionError) as e:
            with db.transaction():
                pass
        assert isinstance(e.value, RuntimeError)


def test_dbapi_parse_error_raises_typra_query_error(tmp_path: pathlib.Path) -> None:
    # parse_select runs before any DB access; this isolates the error mapping behavior.
    p = tmp_path / "t.typra"
    typra.Database.open(str(p))
    conn = typra.dbapi.connect(str(p))
    cur = conn.cursor()
    with pytest.raises(typra.TypraQueryError) as e:
        cur.execute("SELECT FROM")
    assert isinstance(e.value, ValueError)
