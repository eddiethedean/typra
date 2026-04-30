#!/usr/bin/env python3
"""Rank typra-core source files by uncovered lines (needs prior `cargo llvm-cov ... --lcov`)."""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

_SCRIPT_DIR = Path(__file__).resolve().parent
if str(_SCRIPT_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPT_DIR))

from coverage_core import parse_lcov_files  # noqa: E402


def _repo_relative(sf: str, repo_root: Path) -> str:
    p = Path(sf)
    if p.is_absolute():
        try:
            return str(p.relative_to(repo_root))
        except ValueError:
            return sf
    return sf


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--lcov",
        type=Path,
        default=Path("target/coverage/typra-core.lcov"),
        help="Path to LCOV for typra-core (default: target/coverage/typra-core.lcov)",
    )
    args = ap.parse_args()

    repo_root = Path(os.getcwd())
    missed: dict[str, int] = {}
    found: dict[str, int] = {}
    for f in parse_lcov_files(args.lcov):
        rel = _repo_relative(f.path, repo_root)
        if not rel.startswith("crates/typra-core/"):
            continue
        n_miss = f.lines_found - f.lines_hit
        missed[rel] = n_miss
        found[rel] = f.lines_found

    rows = sorted(missed.items(), key=lambda x: (-x[1], x[0]))
    total_missed = sum(missed.values())
    print(f"typra-core uncovered lines (total {total_missed})\n")
    for f, n in rows:
        if n == 0:
            continue
        fd = found[f]
        hit = fd - n
        pct = 100.0 * hit / fd if fd else 100.0
        print(f"{n:4d} missed  {pct:6.2f}% hit  {f}")

    out = Path(os.environ.get("TYPRA_COVERAGE_GAP_TXT", "target/coverage/typra-core-gaps.txt"))
    out.parent.mkdir(parents=True, exist_ok=True)
    lines = [
        f"Total uncovered lines: {total_missed}\n",
        "\n",
    ]
    for f, n in rows:
        if n:
            lines.append(f"{n:4d}  {f}\n")
    out.write_text("".join(lines))
    print(f"\nWrote {out}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
