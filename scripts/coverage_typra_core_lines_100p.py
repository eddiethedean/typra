#!/usr/bin/env python3
"""Fail if any executable line in crates/typra-core/src is not hit (LCOV DA count 0)."""
from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class _Acc:
    missed: list[int] = field(default_factory=list)


def _as_repo_relative(sf: str, repo_root: Path) -> str:
    p = Path(sf)
    if p.is_absolute():
        try:
            return str(p.relative_to(repo_root))
        except ValueError:
            return str(p)
    return sf


def _parse_missed(
    lcov: Path, repo_root: Path, path_prefix: str
) -> tuple[dict[str, _Acc], tuple[int, int]]:
    by_file: dict[str, _Acc] = {}
    path_prefix = path_prefix.replace("\\", "/")
    found = 0
    hit = 0
    cur: str | None = None

    def in_scope(sf: str) -> bool:
        s = _as_repo_relative(sf, repo_root).replace("\\", "/")
        return path_prefix in s and s.endswith(".rs")

    for raw in lcov.read_text().splitlines():
        if raw.startswith("SF:"):
            cur = raw.removeprefix("SF:")
        elif raw == "end_of_record":
            cur = None
        elif cur and in_scope(cur) and raw.startswith("DA:"):
            parts = raw[3:].split(",", 2)
            line_no = int(parts[0])
            count = int(parts[1].split(",")[0]) if len(parts) > 1 and parts[1] else 0
            rel = _as_repo_relative(cur, repo_root)
            found += 1
            if count > 0:
                hit += 1
            else:
                by_file.setdefault(rel, _Acc()).missed.append(line_no)

    for acc in by_file.values():
        acc.missed.sort()
    return by_file, (hit, found)


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Enforce 100% line hit coverage for first-party typra-core sources."
    )
    ap.add_argument("lcov_path", type=Path)
    ap.add_argument(
        "--repo-root",
        type=Path,
        default=Path(os.getcwd()),
        help="Project root (default: cwd).",
    )
    ap.add_argument(
        "--src-prefix",
        type=str,
        default="crates/typra-core/src",
        help="Only files whose SF path contains this (repo-relative) substring are checked.",
    )
    ap.add_argument(
        "--min-pct",
        type=float,
        default=100.0,
        help="Minimum allowed line coverage (default: 100).",
    )
    args = ap.parse_args()

    repo = args.repo_root.resolve()
    missed, (hit, found) = _parse_missed(args.lcov_path.resolve(), repo, args.src_prefix)
    nmiss = found - hit
    pct = 100.0 if found == 0 else 100.0 * (hit / found)

    if nmiss == 0:
        print(
            f"[typra-core-lines-100] OK: 100% (hit {hit} / {found} lines) under {args.src_prefix}"
        )
        return 0

    print(
        f"[typra-core-lines-100] line coverage: {pct:.2f}% (hit {hit} / found {found}, missed {nmiss})",
        file=sys.stderr,
    )
    for sf in sorted(missed):
        if not missed[sf].missed:
            continue
        rel = _as_repo_relative(sf, repo)
        lines = ", ".join(str(n) for n in missed[sf].missed[:200])
        more = len(missed[sf].missed) - 200
        extra = f" ... (+{more} more)" if more > 0 else ""
        print(f"  {rel}:", lines + extra, file=sys.stderr)
    return 1


if __name__ == "__main__":
    raise SystemExit(main())
