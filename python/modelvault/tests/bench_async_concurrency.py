#!/usr/bin/env python3
"""Ad-hoc concurrency benchmark: parallel reads vs serialized writes.

Run from repo root after `make python-develop`:
  .venv/bin/python python/modelvault/tests/bench_async_concurrency.py
"""

from __future__ import annotations

import asyncio
import concurrent.futures
import time

import modelvault

FIELDS = """[
  {"path": ["k"], "type": "string"},
  {"path": ["v"], "type": "int64"}
]"""
N = 40
N_READ = 200
N_HEAVY = 800
SLEEP_SEC = 0.25


async def setup_db() -> modelvault.AsyncDatabase:
    db = await modelvault.AsyncDatabase.open_in_memory()
    await db.register_collection("t", FIELDS, "k")
    for i in range(N):
        await db.insert("t", {"k": f"k{i:03d}", "v": i})
    return db


async def many_gets_sequential(db: modelvault.AsyncDatabase, n: int = N) -> float:
    t0 = time.perf_counter()
    for i in range(n):
        await db.get("t", f"k{i:03d}")
    return time.perf_counter() - t0


async def many_gets_gather(db: modelvault.AsyncDatabase, n: int = N) -> float:
    t0 = time.perf_counter()
    await asyncio.gather(*[db.get("t", f"k{i:03d}") for i in range(n)])
    return time.perf_counter() - t0


async def many_inserts_gather(
    db: modelvault.AsyncDatabase, n: int, prefix: str
) -> float:
    t0 = time.perf_counter()
    await asyncio.gather(
        *[db.insert("t", {"k": f"{prefix}{i:06d}", "v": i}) for i in range(n)]
    )
    return time.perf_counter() - t0


async def many_inserts_sequential(
    db: modelvault.AsyncDatabase, n: int, prefix: str
) -> float:
    t0 = time.perf_counter()
    for i in range(n):
        await db.insert("t", {"k": f"{prefix}{i:06d}", "v": i})
    return time.perf_counter() - t0


def sync_many_gets_threaded(db: modelvault.Database, n: int) -> float:
    keys = [f"k{i:03d}" for i in range(n)]

    def one(k: str) -> None:
        assert db.get("t", k) is not None

    t0 = time.perf_counter()
    with concurrent.futures.ThreadPoolExecutor(max_workers=32) as pool:
        list(pool.map(one, keys))
    return time.perf_counter() - t0


def sync_many_gets_sequential(db: modelvault.Database, n: int) -> float:
    t0 = time.perf_counter()
    for i in range(n):
        assert db.get("t", f"k{i:03d}") is not None
    return time.perf_counter() - t0


async def event_loop_ticks_during_db_work(
    db: modelvault.AsyncDatabase,
) -> tuple[float, int]:
    """If the event loop is blocked, ticker gets ~0 ticks during DB batch."""

    async def db_batch() -> None:
        for i in range(N):
            await db.insert("t", {"k": f"batch{i:03d}", "v": i})

    ticks = 0
    done = asyncio.Event()

    async def ticker() -> None:
        nonlocal ticks
        while not done.is_set():
            await asyncio.sleep(0.005)
            ticks += 1

    tick_task = asyncio.create_task(ticker())
    t0 = time.perf_counter()
    await db_batch()
    db_elapsed = time.perf_counter() - t0
    done.set()
    await tick_task
    return db_elapsed, ticks


async def pure_sleep_baseline() -> int:
    """Upper bound: how many ticks in same wall time with no DB."""
    ticks = 0
    t0 = time.perf_counter()
    target = 0.15
    while time.perf_counter() - t0 < target:
        await asyncio.sleep(0.005)
        ticks += 1
    return ticks


async def overlap_sleep_with_db(
    db: modelvault.AsyncDatabase,
) -> tuple[float, float, float]:
    """If event loop is free, gather(sleep, db_batch) ~ max(sleep, db_time). If blocked, ~ sum."""

    async def sleep_task() -> None:
        await asyncio.sleep(SLEEP_SEC)

    async def db_task() -> None:
        for i in range(N_HEAVY):
            await db.insert("t", {"k": f"h{i:06d}", "v": i})

    t0 = time.perf_counter()
    await asyncio.gather(sleep_task(), db_task())
    combined = time.perf_counter() - t0

    t1 = time.perf_counter()
    await sleep_task()
    sleep_only = time.perf_counter() - t1

    t2 = time.perf_counter()
    await db_task()
    db_only = time.perf_counter() - t2

    return combined, sleep_only, db_only


async def main() -> None:
    print("=== ModelVault AsyncDatabase concurrency probe ===\n")

    db = await setup_db()

    gather_t = await many_gets_gather(db, N)
    seq_t = await many_gets_sequential(db, N)
    print(f"AsyncDatabase — N={N} get (read lock)")
    print(f"  asyncio.gather:              {gather_t:.4f}s")
    print(f"  sequential await:            {seq_t:.4f}s")
    ratio = gather_t / seq_t if seq_t > 0 else 0
    print(f"  gather/sequential ratio:     {ratio:.2f}x")
    if ratio < 0.85:
        print("  -> READS: gather faster (concurrent shared lock + thread pool)")
    elif ratio > 1.15:
        print("  -> gather slower (scheduling overhead dominates)")
    else:
        print("  -> similar (work too small to measure parallelism)")

    gather_r = await many_gets_gather(db, N_READ)
    seq_r = await many_gets_sequential(db, N_READ)
    ratio_r = gather_r / seq_r if seq_r > 0 else 0
    print(f"\nAsyncDatabase — N={N_READ} get (read lock)")
    print(f"  gather: {gather_r:.4f}s  sequential: {seq_r:.4f}s  ratio: {ratio_r:.2f}x")

    w_gather = await many_inserts_gather(db, N, "wg")
    w_seq = await many_inserts_sequential(db, N, "ws")
    w_ratio = w_gather / w_seq if w_seq > 0 else 0
    print(f"\nAsyncDatabase — N={N} insert (write lock)")
    print(f"  gather: {w_gather:.4f}s  sequential: {w_seq:.4f}s  ratio: {w_ratio:.2f}x")
    if 0.85 <= w_ratio <= 1.15:
        print("  -> WRITES: no speedup from gather (exclusive lock serializes)")
    elif w_ratio < 0.85:
        print("  -> WRITES: unexpected parallel speedup (investigate)")
    else:
        print("  -> WRITES: gather slower (contention / scheduling)")

    print("\n--- sync Database.get from threads (read lock) ---")
    sdb = modelvault.Database.open_in_memory()
    sdb.register_collection("t", FIELDS, "k")
    for i in range(N_READ):
        sdb.insert("t", {"k": f"k{i:03d}", "v": i})
    thr_t = sync_many_gets_threaded(sdb, N_READ)
    s_seq_t = sync_many_gets_sequential(sdb, N_READ)
    s_ratio = thr_t / s_seq_t if s_seq_t > 0 else 0
    print(f"N={N_READ} get")
    print(f"  ThreadPoolExecutor(32): {thr_t:.4f}s")
    print(f"  single-threaded:        {s_seq_t:.4f}s")
    print(f"  threaded/sequential:    {s_ratio:.2f}x")
    if s_ratio < 0.85:
        print("  -> READS: threads run gets in parallel on shared RwLock")
    else:
        print("  -> reads not faster threaded (too fast or overhead)")

    db_t, ticks = await event_loop_ticks_during_db_work(db)
    baseline = await pure_sleep_baseline()
    print(f"\nEvent loop progress during {N} inserts ({db_t:.4f}s)")
    print(f"  ticker ticks during DB work: {ticks}")
    print(f"  ticker ticks in {0.15:.2f}s pure sleep baseline: {baseline}")
    if ticks >= baseline * 0.25:
        print("  -> event loop NOT blocked (async API releases GIL during engine work)")
    else:
        print("  -> event loop appears blocked (unexpected for spawn_blocking design)")

    # Sync Database in asyncio without AsyncDatabase (comparison)
    print("\n--- sync Database + asyncio.to_thread (reference) ---")

    def sync_work() -> None:
        sdb = modelvault.Database.open_in_memory()
        sdb.register_collection("t", FIELDS, "k")
        for i in range(N):
            sdb.insert("t", {"k": f"s{i:03d}", "v": i})

    ticks2 = 0
    done2 = asyncio.Event()

    async def ticker2() -> None:
        nonlocal ticks2
        while not done2.is_set():
            await asyncio.sleep(0.005)
            ticks2 += 1

    t2 = asyncio.create_task(ticker2())
    t0 = time.perf_counter()
    await asyncio.to_thread(sync_work)
    sync_elapsed = time.perf_counter() - t0
    done2.set()
    await t2
    print(f"  sync work via to_thread: {sync_elapsed:.4f}s, ticks during: {ticks2}")

    print(f"\n--- overlap: {SLEEP_SEC}s sleep vs {N_HEAVY} inserts (AsyncDatabase) ---")
    combined, sleep_only, db_only = await overlap_sleep_with_db(db)
    serial_estimate = sleep_only + db_only
    print(f"  sleep alone:           {sleep_only:.4f}s")
    print(f"  {N_HEAVY} inserts alone:  {db_only:.4f}s")
    print(f"  sequential estimate:   {serial_estimate:.4f}s (sleep + inserts)")
    print(f"  gather (parallel):     {combined:.4f}s")
    if combined < serial_estimate * 0.9:
        print(
            "  -> OVERLAP: sleep and DB work run concurrently (event loop + thread pool)"
        )
    else:
        print("  -> NO meaningful overlap (work is too fast or loop blocked)")


if __name__ == "__main__":
    asyncio.run(main())
