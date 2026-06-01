"""Transaction context manager and txn_depth behavior."""

import modelvault


def test_transaction_rollback_on_exception_allows_reads(tmp_path) -> None:
    path = tmp_path / "t.modelvault"
    db = modelvault.Database.open(str(path))
    db.register_collection(
        "items",
        '[{"path": ["id"], "type": "int64"}]',
        "id",
    )
    try:
        with db.transaction():
            db.insert("items", {"id": 1})
            raise RuntimeError("abort")
    except RuntimeError:
        pass
    assert db.get("items", 1) is None
    db.insert("items", {"id": 2})
    assert db.get("items", 2) is not None


def test_transaction_commit_allows_subsequent_reads(tmp_path) -> None:
    path = tmp_path / "t.modelvault"
    db = modelvault.Database.open(str(path))
    db.register_collection(
        "items",
        '[{"path": ["id"], "type": "int64"}]',
        "id",
    )
    with db.transaction():
        db.insert("items", {"id": 1})
    assert db.get("items", 1) is not None
