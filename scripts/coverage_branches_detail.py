from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class BranchRec:
    line: int
    col: int
    end_line: int
    end_col: int
    count_a: int
    count_b: int


def _as_repo_relative(path: str, repo_root: Path) -> str:
    p = Path(path)
    if p.is_absolute():
        try:
            return str(p.relative_to(repo_root))
        except ValueError:
            return str(p)
    return path


def _parse_branch_rec(r: list[int]) -> BranchRec:
    # Format from llvm-cov export: [line, col, end_line, end_col, count_a, count_b, 0, 0, 4]
    return BranchRec(
        line=r[0],
        col=r[1],
        end_line=r[2],
        end_col=r[3],
        count_a=r[4],
        count_b=r[5],
    )


def _is_missing(rec: BranchRec) -> bool:
    # Treat this as a decision with two outcomes; "missing" means one side never executed.
    return (rec.count_a == 0) ^ (rec.count_b == 0) or (rec.count_a == 0 and rec.count_b == 0)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("json_path", type=Path)
    ap.add_argument("--repo-root", type=Path, required=True)
    ap.add_argument("--crate-path", type=str, required=True)
    ap.add_argument("--top-functions", type=int, default=50)
    args = ap.parse_args()

    repo_root = args.repo_root.resolve()
    raw = json.loads(args.json_path.read_text())
    data0 = raw["data"][0]

    crate_prefix = args.crate_path.rstrip("/") + "/"

    print("[coverage-branches-detail] per-file branch summary (llvm-cov):")
    for f in data0["files"]:
        rel = _as_repo_relative(f["filename"], repo_root)
        if not (rel == args.crate_path or rel.startswith(crate_prefix)):
            continue
        sb = f["summary"].get("branches") or {}
        print(
            f"  - {rel}: {sb.get('percent', 0):6.2f}% "
            f"(covered {sb.get('covered', 0)}/{sb.get('count', 0)}, notcovered {sb.get('notcovered', 0)})"
        )

    # Function hotspots (derived from per-function branch records)
    print("\n[coverage-branches-detail] top functions with missing decision outcomes (derived):")
    hotspots: list[tuple[int, str, str]] = []
    for fn in data0["functions"]:
        files = fn.get("filenames") or []
        if not files:
            continue
        rel0 = _as_repo_relative(files[0], repo_root)
        if not (rel0 == args.crate_path or rel0.startswith(crate_prefix)):
            continue
        br = fn.get("branches") or []
        if not br:
            continue
        missing = 0
        for r in br:
            rec = _parse_branch_rec(r)
            if _is_missing(rec):
                missing += 1
        if missing:
            hotspots.append((missing, rel0, fn.get("name", "<unnamed>")))

    hotspots.sort(key=lambda x: (-x[0], x[1], x[2]))
    for missing, rel, name in hotspots[: args.top_functions]:
        print(f"  - missing {missing:3d}: {rel} :: {name}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

