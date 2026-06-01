"""Concurrent reads on one handle (sync threads + asyncio gather)."""

from __future__ import annotations

import asyncio
import concurrent.futures

import pytest

import modelvault


def _setup_sync_db(n: int = 300) -> modelvault.Database:
    db = modelvault.Database.open_in_memory()
    db.register_collection(
        "items",
        '[{"path": ["id"], "type": "string"}, {"path": ["n"], "type": "int64"}]',
        "id",
    )
    for i in range(n):
        db.insert("items", {"id": f"k{i}", "n": i})
    return db


def test_sync_parallel_gets_return_correct_rows() -> None:
    db = _setup_sync_db(400)
    keys = [f"k{i}" for i in range(400)]

    def one_get(k: str) -> int:
        row = db.get("items", k)
        assert row is not None
        return row["n"]

    with concurrent.futures.ThreadPoolExecutor(max_workers=32) as pool:
        results = list(pool.map(one_get, keys))
    assert results == list(range(400))


@pytest.mark.asyncio
async def test_async_gather_gets_return_correct_rows() -> None:
    db = await modelvault.AsyncDatabase.open_in_memory()
    await db.register_collection(
        "items",
        '[{"path": ["id"], "type": "string"}, {"path": ["n"], "type": "int64"}]',
        "id",
    )
    for i in range(200):
        await db.insert("items", {"id": f"k{i}", "n": i})

    keys = [f"k{i}" for i in range(200)]

    async def one_get(k: str) -> int:
        row = await db.get("items", k)
        assert row is not None
        return row["n"]

    gathered = await asyncio.gather(*(one_get(k) for k in keys))
    assert gathered == list(range(200))
