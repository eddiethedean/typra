import json

import typra


def test_fields_json_multi_segment_paths_roundtrip_and_index_query(tmp_path) -> None:
    p = tmp_path / "m.typra"
    db = typra.Database.open(str(p))

    fields = json.dumps(
        [
            {"path": ["id"], "type": "string"},
            {"path": ["profile", "timezone"], "type": "string"},
            {"path": ["profile", "age"], "type": "int64"},
        ]
    )
    indexes = json.dumps(
        [{"name": "tz_idx", "path": ["profile", "timezone"], "kind": "index"}]
    )
    db.register_collection("users", fields, "id", indexes_json=indexes)

    db.insert(
        "users",
        {"id": "u1", "profile": {"timezone": "UTC", "age": 30}},
    )

    got = db.get("users", "u1")
    assert got is not None
    assert got["profile"]["timezone"] == "UTC"
    assert got["profile"]["age"] == 30

    # Query builder supports nested paths via dotted string.
    rows = db.collection("users").where("profile.timezone", "UTC").all()
    assert len(rows) == 1
    assert rows[0]["id"] == "u1"

    # Reopen should preserve nested structure.
    db2 = typra.Database.open(str(p))
    got2 = db2.get("users", "u1")
    assert got2 is not None
    assert got2["profile"]["timezone"] == "UTC"
