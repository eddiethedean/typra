from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import typra


@dataclass
class Item:
    __typra_primary_key__ = "id"
    __typra_indexes__ = [typra.models.index("tag")]

    id: int
    tag: str
    note: Optional[str] = None


def test_production_journey_models_reopen_plan_apply_compact_snapshot(
    tmp_path: Path,
) -> None:
    path = tmp_path / "app.typra"
    snap = tmp_path / "snap.typra"

    db = typra.Database.open(str(path))
    items = typra.models.collection(db, Item)

    with db.transaction():
        items.insert(Item(id=1, tag="a"))
        items.insert(Item(id=2, tag="b"))
        items.insert(Item(id=3, tag="a"))

    q = items.where(Item.tag, "a")
    assert "IndexLookup" in q.explain()
    assert len(q.all()) == 2

    # Plan/apply schema (no-op or bump depending on engine semantics).
    _plan = typra.models.plan(db, Item)
    _ver = typra.models.apply(db, Item, force=False)

    # If apply registered a new schema version, compact rewrites the file so all latest rows
    # are consistent with the current catalog version.
    db.compact()

    # Reopen and read back.
    db2 = typra.Database.open(str(path))
    items2 = typra.models.collection(db2, Item)
    got = items2.get(2)
    assert got is not None
    assert got.tag == "b"

    # Snapshot export.
    db2.export_snapshot(str(snap))
    mem = typra.Database.open_snapshot(str(snap))
    items3 = typra.models.collection(mem, Item)
    assert items3.get(1) is not None
