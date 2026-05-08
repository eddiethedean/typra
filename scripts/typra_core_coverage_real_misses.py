#!/usr/bin/env python3
"""Report *actual* uncovered source lines for typra-core from llvm-cov JSON.

Why this exists:
- `cargo llvm-cov report --show-missing-lines` (summary table) can show "missed lines"
  that are not attributable to concrete source line numbers (mapping artifacts).
- The per-line (text/html) views are the ground truth for "what line is uncovered".

This script uses the JSON export's `files[].segments` to compute uncovered lines:
- A line is considered uncovered if it has at least one non-gap, count-bearing region-entry
  segment with execution count == 0, and has no count > 0 segment on the same line.

Usage:
  cargo llvm-cov report -p typra-core --json --output-path target/coverage/typra-core.json
  python3 scripts/typra_core_coverage_real_misses.py --json target/coverage/typra-core.json
"""

from __future__ import annotations

import argparse
import json
import os
from collections import defaultdict
from pathlib import Path


def _repo_relative(path_s: str, repo_root: Path) -> str:
    p = Path(path_s)
    if p.is_absolute():
        try:
            return str(p.relative_to(repo_root)).replace("\\", "/")
        except ValueError:
            return str(p).replace("\\", "/")
    return path_s.replace("\\", "/")


def _real_missed_lines(segments: list[list]) -> set[int]:
    # Segment shape (llvm-cov export):
    # [line, col, count, hasCount, isRegionEntry, isGap]
    hit: set[int] = set()
    miss: set[int] = set()
    for line, _col, count, has_count, is_region_entry, is_gap in segments:
        if not has_count or is_gap:
            continue
        if count > 0:
            hit.add(int(line))
        elif is_region_entry:
            miss.add(int(line))
    return {l for l in miss if l not in hit}

def _is_non_executable_source_line(line: str) -> bool:
    s = line.strip()
    if s == "":
        return True
    # If the line contains no identifiers/literals at all, it's almost certainly structural.
    if not any((ch.isalnum() or ch == "_") for ch in s):
        return True
    # Ignore common structural-only lines that llvm-cov may attribute regions to.
    if all(ch in "{}();,[]" for ch in s):
        return True
    return False


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--json",
        type=Path,
        default=Path("target/coverage/typra-core.json"),
        help="Path to llvm-cov JSON export (cargo llvm-cov report --json)",
    )
    ap.add_argument(
        "--out",
        type=Path,
        default=Path("target/coverage/typra-core-real-misses.txt"),
        help="Output path",
    )
    args = ap.parse_args()

    repo_root = Path(os.getcwd())
    doc = json.loads(args.json.read_text())
    files = doc["data"][0]["files"]

    misses_by_file: dict[str, set[int]] = {}
    for f in files:
        rel = _repo_relative(f["filename"], repo_root)
        if not rel.startswith("crates/typra-core/"):
            continue
        missed = _real_missed_lines(f.get("segments") or [])
        if not missed:
            continue

        # Filter out lines that are structurally non-executable (e.g. lone `}` lines).
        src_path = repo_root / rel
        try:
            src_lines = src_path.read_text().splitlines()
        except OSError:
            src_lines = []
        filtered: set[int] = set()
        for ln in missed:
            if 1 <= ln <= len(src_lines) and _is_non_executable_source_line(src_lines[ln - 1]):
                continue
            filtered.add(ln)
        missed = filtered
        if missed:
            misses_by_file[rel] = missed

    total = sum(len(v) for v in misses_by_file.values())
    lines: list[str] = []
    lines.append(f"typra-core REAL uncovered source lines (total {total})\n\n")
    for path in sorted(misses_by_file):
        ms = sorted(misses_by_file[path])
        # Render compactly but still grep-friendly.
        preview = ", ".join(str(x) for x in ms[:40])
        more = f", ... (+{len(ms)-40})" if len(ms) > 40 else ""
        lines.append(f"{path}: {preview}{more}\n")

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text("".join(lines))
    print(f"Wrote {args.out} ({total} uncovered lines across {len(misses_by_file)} files)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

