"""Integration-style tests aligned with docs/guide_python.md realistic examples."""

from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

import typra

ORDER_FIELDS = """[
  {"path": ["id"], "type": "int64"},
  {"path": ["sku"], "type": "string"},
  {"path": ["qty"], "type": "int64"},
  {"path": ["status"], "type": "string"}
]"""

ORDER_INDEXES = """[
  {"name": "sku_idx", "path": ["sku"], "kind": "index"},
  {"name": "status_idx", "path": ["status"], "kind": "index"}
]"""


def test_disk_roundtrip_indexed_conjunctive_query_and_subset() -> None:
    """Same scenario as the 'Realistic workflow' section in docs/guide_python.md."""
    with tempfile.TemporaryDirectory() as d:
        path = Path(d) / "app.typra"
        db = typra.Database.open(str(path))
        db.register_collection("order_lines", ORDER_FIELDS, "id", ORDER_INDEXES)
        for oid, sku, qty, st in [
            (1, "SKU-A", 2, "open"),
            (2, "SKU-B", 1, "shipped"),
            (3, "SKU-A", 4, "open"),
        ]:
            db.insert(
                "order_lines",
                {"id": oid, "sku": sku, "qty": qty, "status": st},
            )
        q = (
            db.collection("order_lines")
            .where("status", "open")
            .and_where("sku", "SKU-A")
            .limit(10)
        )
        assert "IndexLookup" in q.explain()
        rows = sorted(q.all(), key=lambda r: r["id"])
        assert rows == [
            {"id": 1, "sku": "SKU-A", "qty": 2, "status": "open"},
            {"id": 3, "sku": "SKU-A", "qty": 4, "status": "open"},
        ]
        short = sorted(
            db.collection("order_lines")
            .where("status", "open")
            .all(fields=["id", "qty"]),
            key=lambda r: r["id"],
        )
        assert short == [{"id": 1, "qty": 2}, {"id": 3, "qty": 4}]

        db2 = typra.Database.open(str(path))
        assert db2.get("order_lines", 1) == {
            "id": 1,
            "sku": "SKU-A",
            "qty": 2,
            "status": "open",
        }
        q2 = db2.collection("order_lines").where("sku", "SKU-A")
        assert len(q2.all()) == 2


def test_snapshot_preserves_indexes_and_queries() -> None:
    db = typra.Database.open_in_memory()
    db.register_collection("order_lines", ORDER_FIELDS, "id", ORDER_INDEXES)
    db.insert("order_lines", {"id": 10, "sku": "X", "qty": 1, "status": "open"})
    blob = db.snapshot_bytes()
    db2 = typra.Database.open_snapshot_bytes(blob)
    assert "IndexLookup" in db2.collection("order_lines").where("sku", "X").explain()
    row = db2.get("order_lines", 10)
    assert row is not None
    assert row["qty"] == 1


def test_unique_index_enforced_across_distinct_primary_keys() -> None:
    db = typra.Database.open_in_memory()
    fields = (
        '[{"path": ["id"], "type": "int64"}, {"path": ["email"], "type": "string"}]'
    )
    indexes = '[{"name": "email_u", "path": ["email"], "kind": "unique"}]'
    db.register_collection("accounts", fields, "id", indexes)
    db.insert("accounts", {"id": 1, "email": "a@example.test"})
    with pytest.raises(ValueError):
        db.insert("accounts", {"id": 2, "email": "a@example.test"})


def test_limit_applies_after_predicate_on_collection_scan() -> None:
    """`limit` is on the query builder (after `where`); full collection scans use `all()` on `collection`."""
    db = typra.Database.open_in_memory()
    db.register_collection(
        "order_lines",
        ORDER_FIELDS,
        "id",
        ORDER_INDEXES,
    )
    for oid, sku, qty, st in [
        (1, "S", 1, "open"),
        (2, "S", 2, "open"),
        (3, "S", 3, "open"),
    ]:
        db.insert("order_lines", {"id": oid, "sku": sku, "qty": qty, "status": st})
    assert len(db.collection("order_lines").all()) == 3
    limited = db.collection("order_lines").where("status", "open").limit(2).all()
    assert len(limited) == 2
