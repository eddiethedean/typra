import pathlib

import pytest

import modelvault


def test_open_garbage_file_raises_modelvault_format_error(
    tmp_path: pathlib.Path,
) -> None:
    p = tmp_path / "bad.modelvault"
    p.write_bytes(b"this is not a modelvault file")
    with pytest.raises(modelvault.ModelVaultFormatError) as e:
        modelvault.Database.open(str(p))
    assert isinstance(e.value, ValueError)


def test_register_invalid_primary_key_raises_modelvault_schema_error() -> None:
    db = modelvault.Database.open_in_memory()
    fields_json = '[{"path":["title"],"type":"string"}]'
    with pytest.raises(modelvault.ModelVaultSchemaError) as e:
        db.register_collection("books", fields_json, "id")
    assert isinstance(e.value, ValueError)


def test_insert_type_mismatch_raises_modelvault_validation_error(
    tmp_path: pathlib.Path,
) -> None:
    db = modelvault.Database.open(str(tmp_path / "t.modelvault"))
    # Use a constraint violation (correct type, invalid value) so the error comes from the engine
    # and is mapped via `DbError::Validation`.
    fields_json = """
    [
      {"path": ["id"], "type": "string"},
      {"path": ["year"], "type": "int64", "constraints": [{"min_i64": 2000}]}
    ]
    """
    db.register_collection("events", fields_json, "id")
    with pytest.raises(modelvault.ModelVaultValidationError) as e:
        db.insert("events", {"id": "e1", "year": 1990})
    assert isinstance(e.value, ValueError)


def test_nested_transaction_raises_modelvault_transaction_error(
    tmp_path: pathlib.Path,
) -> None:
    db = modelvault.Database.open(str(tmp_path / "t.modelvault"))
    fields_json = '[{"path":["id"],"type":"string"}]'
    db.register_collection("x", fields_json, "id")
    with db.transaction():
        with pytest.raises(modelvault.ModelVaultTransactionError) as e:
            with db.transaction():
                pass
        assert isinstance(e.value, RuntimeError)


def test_dbapi_parse_error_raises_modelvault_query_error(
    tmp_path: pathlib.Path,
) -> None:
    # parse_select runs before any DB access; this isolates the error mapping behavior.
    p = tmp_path / "t.modelvault"
    modelvault.Database.open(str(p))
    conn = modelvault.dbapi.connect(str(p))
    cur = conn.cursor()
    with pytest.raises(modelvault.ModelVaultQueryError) as e:
        cur.execute("SELECT FROM")
    assert isinstance(e.value, ValueError)


def test_unique_index_violation_raises_modelvault_schema_error() -> None:
    db = modelvault.Database.open_in_memory()
    fields = (
        '[{"path": ["id"], "type": "string"}, {"path": ["title"], "type": "string"}]'
    )
    indexes = '[{"name": "title_unique", "path": ["title"], "kind": "unique"}]'
    db.register_collection("books", fields, "id", indexes)
    db.insert("books", {"id": "a", "title": "A"})
    with pytest.raises(modelvault.ModelVaultSchemaError):
        db.insert("books", {"id": "b", "title": "A"})


def test_compact_to_maps_format_errors(tmp_path: pathlib.Path) -> None:
    db = modelvault.Database.open_in_memory()
    with pytest.raises(ValueError):
        db.compact_to(str(tmp_path / "out.modelvault"))
