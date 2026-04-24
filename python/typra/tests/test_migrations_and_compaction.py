from __future__ import annotations

import json

import pytest

import typra


def test_plan_and_register_schema_version_add_required_field_needs_migration(
    tmp_path,
) -> None:
    db = typra.Database.open(str(tmp_path / "m.typra"))
    fields_v1 = json.dumps([{"path": ["id"], "type": "int64"}])
    db.register_collection("t", fields_v1, "id")

    fields_v2_required = json.dumps(
        [{"path": ["id"], "type": "int64"}, {"path": ["x"], "type": "int64"}]
    )
    plan = db.plan_schema_version("t", fields_v2_required)
    assert plan["change"] == "needs_migration"

    with pytest.raises(ValueError):
        db.register_schema_version("t", fields_v2_required)

    # Escape hatch should work.
    v = db.register_schema_version("t", fields_v2_required, force=True)
    assert v == 2

    # Backfill then insert should succeed.
    db.backfill_top_level_field("t", "x", 0)
    db.insert("t", {"id": 1, "x": 5})
    got = db.get("t", 1)
    assert got is not None
    assert got["x"] == 5


def test_compaction_apis(tmp_path) -> None:
    src = tmp_path / "src.typra"
    dst = tmp_path / "dst.typra"
    db = typra.Database.open(str(src))
    fields = json.dumps(
        [{"path": ["id"], "type": "int64"}, {"path": ["tag"], "type": "string"}]
    )
    indexes = json.dumps([{"name": "tag_idx", "path": ["tag"], "kind": "index"}])
    db.register_collection("t", fields, "id", indexes_json=indexes)
    db.insert("t", {"id": 1, "tag": "a"})
    db.insert("t", {"id": 2, "tag": "b"})

    db.compact_to(str(dst))
    db2 = typra.Database.open(str(dst))
    got2 = db2.get("t", 2)
    assert got2 is not None
    assert got2["tag"] == "b"

    db.compact()
    db3 = typra.Database.open(str(src))
    got3 = db3.get("t", 1)
    assert got3 is not None
    assert got3["tag"] == "a"
