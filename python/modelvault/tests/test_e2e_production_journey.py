from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import modelvault


@dataclass
class Item:
    __modelvault_primary_key__ = "id"
    __modelvault_indexes__ = [modelvault.models.index("tag")]

    id: int
    tag: str
    note: Optional[str] = None


def test_production_journey_models_reopen_plan_apply_compact_snapshot(
    tmp_path: Path,
) -> None:
    path = tmp_path / "app.modelvault"
    snap = tmp_path / "snap.modelvault"

    db = modelvault.Database.open(str(path))
    items = modelvault.models.collection(db, Item)

    with db.transaction():
        items.insert(Item(id=1, tag="a"))
        items.insert(Item(id=2, tag="b"))
        items.insert(Item(id=3, tag="a"))

    q = items.where(Item.tag, "a")
    assert "IndexLookup" in q.explain()
    assert len(q.all()) == 2

    # Plan/apply schema (no-op or bump depending on engine semantics).
    _plan = modelvault.models.plan(db, Item)
    _ver = modelvault.models.apply(db, Item, force=False)

    # If apply registered a new schema version, compact rewrites the file so all latest rows
    # are consistent with the current catalog version.
    db.compact()

    # Handle refreshed in place after compact.
    items2 = modelvault.models.collection(db, Item)
    got = items2.get(2)
    assert got is not None
    assert got.tag == "b"

    # Snapshot export.
    db.export_snapshot(str(snap))
    mem = modelvault.Database.open_snapshot(str(snap))
    items3 = modelvault.models.collection(mem, Item)
    assert items3.get(1) is not None
