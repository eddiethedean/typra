#!/usr/bin/env python3
"""Write per-file line miss counts for typra-core (matches `cargo llvm-cov --fail-under-lines` basis).

Reads `cargo llvm-cov` JSON export with `data[].files[].summary.lines` (use `--json --summary-only`).
Default input: `target/coverage/typra-core-summary.json`.

Example:
  CI=1 cargo llvm-cov -p typra-core --all-features --json --summary-only \\
    --output-path target/coverage/typra-core-summary.json
  python3 scripts/typra_core_coverage_miss_summary.py
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--json",
        type=Path,
        default=Path("target/coverage/typra-core-summary.json"),
        help="Path to llvm-cov JSON (--json --summary-only)",
    )
    ap.add_argument(
        "--out",
        type=Path,
        default=Path("target/coverage/typra-core-miss-by-file.txt"),
        help="Output path (clustered miss list)",
    )
    args = ap.parse_args()

    repo_root = Path(os.getcwd())
    data = json.loads(args.json.read_text())["data"][0]["files"]
    rows: list[tuple[int, str, int, int]] = []
    total_miss = 0
    for f in data:
        path = Path(f["filename"])
        try:
            rel = path.relative_to(repo_root)
        except ValueError:
            rel = path
        rel_s = str(rel).replace("\\", "/")
        if not rel_s.startswith("crates/typra-core/"):
            continue
        s = f["summary"]["lines"]
        n = int(s["count"]) - int(s["covered"])
        if n <= 0:
            continue
        total_miss += n
        rows.append((n, rel_s, int(s["count"]), int(s["covered"])))

    rows.sort(key=lambda t: (-t[0], t[1]))
    lines = [
        f"typra-core line misses (llvm summary.lines, total {total_miss})\n",
        "Per-file: missed  hit%  path\n",
    ]
    for miss, rel_s, found, hit in rows:
        pct = 100.0 * hit / found if found else 100.0
        lines.append(f"{miss:4d}  {pct:6.2f}%  {rel_s}\n")

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text("".join(lines))
    print(f"Wrote {args.out} ({total_miss} missed lines across {len(rows)} files)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
