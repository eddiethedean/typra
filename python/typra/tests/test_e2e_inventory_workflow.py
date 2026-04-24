from __future__ import annotations

import json

import pytest

import typra


def test_e2e_inventory_like_workflow_txn_query_reopen_compact_snapshot(
    tmp_path,
) -> None:
    path = tmp_path / "inventory.typra"
    snap = tmp_path / "inventory.snapshot.typra"
    compacted = tmp_path / "inventory.compacted.typra"

    db = typra.Database.open(str(path))
    products_fields = json.dumps(
        [
            {"path": ["sku"], "type": "string"},
            {"path": ["name"], "type": "string"},
            {"path": ["category"], "type": "string"},
            {"path": ["price"], "type": "int64"},
            {"path": ["created_at"], "type": "int64"},
        ]
    )
    products_indexes = json.dumps(
        [
            {"name": "name_unique", "path": ["name"], "kind": "unique"},
            {"name": "category_idx", "path": ["category"], "kind": "index"},
        ]
    )
    db.register_collection(
        "products", products_fields, "sku", indexes_json=products_indexes
    )

    # Transaction rollback on unique index violation should discard all writes.
    with pytest.raises(ValueError):
        with db.transaction():
            db.insert(
                "products",
                {
                    "sku": "sku1",
                    "name": "Widget",
                    "category": "tools",
                    "price": 199,
                    "created_at": 10,
                },
            )
            db.insert(
                "products",
                {
                    "sku": "sku2",
                    "name": "Widget",  # violates unique index
                    "category": "tools",
                    "price": 299,
                    "created_at": 20,
                },
            )

    assert db.get("products", "sku1") is None

    with db.transaction():
        db.insert(
            "products",
            {
                "sku": "sku1",
                "name": "Widget",
                "category": "tools",
                "price": 199,
                "created_at": 10,
            },
        )
        db.insert(
            "products",
            {
                "sku": "sku2",
                "name": "Gadget",
                "category": "tools",
                "price": 299,
                "created_at": 20,
            },
        )
        db.insert(
            "products",
            {
                "sku": "sku3",
                "name": "Book",
                "category": "media",
                "price": 25,
                "created_at": 30,
            },
        )
        db.insert(
            "products",
            {
                "sku": "sku4",
                "name": "Premium",
                "category": "tools",
                "price": 499,
                "created_at": 40,
            },
        )

    # Range + order_by + limit via DB-API.
    conn = typra.dbapi.connect(str(path))
    cur = conn.cursor()
    cur.execute(
        "SELECT sku, created_at FROM products WHERE price >= ? AND price < ? ORDER BY created_at DESC LIMIT 2",
        (100, 400),
    )
    assert cur.fetchall() == [("sku2", 20), ("sku1", 10)]

    # Reopen and run a category query.
    db2 = typra.Database.open(str(path))
    conn2 = typra.dbapi.connect(str(path))
    cur2 = conn2.cursor()
    cur2.execute(
        "SELECT sku FROM products WHERE category = ? ORDER BY price ASC",
        ("tools",),
    )
    assert [r[0] for r in cur2.fetchall()] == ["sku1", "sku2", "sku4"]

    # Compaction APIs.
    db2.compact_to(str(compacted))
    db3 = typra.Database.open(str(compacted))
    got3 = db3.get("products", "sku3")
    assert got3 is not None
    assert got3["name"] == "Book"

    db2.compact()
    db4 = typra.Database.open(str(path))
    got2 = db4.get("products", "sku2")
    assert got2 is not None
    assert got2["name"] == "Gadget"

    # Snapshot export/import.
    db4.export_snapshot(str(snap))
    snap_db = typra.Database.open_snapshot(str(snap))
    got4 = snap_db.get("products", "sku4")
    assert got4 is not None
    assert got4["price"] == 499
