from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class BranchTotals:
    covered: int
    count: int

    @property
    def pct(self) -> float:
        if self.count == 0:
            return 100.0
        return 100.0 * (self.covered / self.count)


def _as_repo_relative(path: str, repo_root: Path) -> str:
    p = Path(path)
    if p.is_absolute():
        try:
            return str(p.relative_to(repo_root))
        except ValueError:
            return str(p)
    return path


def _extract_branch_totals(file_entry: dict) -> BranchTotals | None:
    """
    cargo-llvm-cov uses `llvm-cov export -format=text` for --json. In LLVM's JSON schema,
    branch counts may appear under a "branches" object at the per-file summary level.
    We only need covered/total counts, not per-branch locations.
    """
    summary = file_entry.get("summary")
    if not isinstance(summary, dict):
        return None
    summary_branches = summary.get("branches")
    if not isinstance(summary_branches, dict):
        return None
    covered = summary_branches.get("covered")
    count = summary_branches.get("count")
    if isinstance(covered, int) and isinstance(count, int):
        return BranchTotals(covered=covered, count=count)
    return None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("json_path", type=Path)
    ap.add_argument("--repo-root", type=Path, required=True)
    ap.add_argument("--crate-path", type=str, required=True)
    ap.add_argument("--min-branch-pct", type=float, required=True)
    args = ap.parse_args()

    raw = json.loads(args.json_path.read_text())

    data = raw.get("data")
    if not isinstance(data, list) or not data:
        print("[coverage-branches] FAIL: missing 'data' array in llvm-cov json", file=sys.stderr)
        return 2

    totals = BranchTotals(covered=0, count=0)
    missing: list[tuple[str, BranchTotals]] = []

    # Schema: data[0].files: [{ filename, summary: { branches: {covered,count} } }, ...]
    files = data[0].get("files")
    if not isinstance(files, list):
        print("[coverage-branches] FAIL: missing 'files' array in llvm-cov json", file=sys.stderr)
        return 2

    crate_prefix = args.crate_path.rstrip("/") + "/"
    for f in files:
        if not isinstance(f, dict):
            continue
        filename = f.get("filename")
        if not isinstance(filename, str):
            continue
        rel = _as_repo_relative(filename, args.repo_root)
        if not (rel == args.crate_path or rel.startswith(crate_prefix)):
            continue

        summary = f.get("summary")
        if not isinstance(summary, dict):
            continue
        bt = _extract_branch_totals(f)
        if bt is None:
            # If branch coverage isn't present (toolchain mismatch), fail loudly.
            print(
                f"[coverage-branches] FAIL: no branch data for {rel} (is --branch supported?)",
                file=sys.stderr,
            )
            return 2

        totals = BranchTotals(covered=totals.covered + bt.covered, count=totals.count + bt.count)
        if bt.covered != bt.count:
            missing.append((rel, bt))

    pct = totals.pct
    print(
        f"[coverage-branches] typra-core: {pct:6.2f}% (covered {totals.covered} / total {totals.count}), "
        f"min {args.min_branch_pct:.2f}%"
    )

    if pct + 1e-9 < args.min_branch_pct:
        print("[coverage-branches] FAIL: below minimum branch coverage", file=sys.stderr)
        for rel, bt in sorted(missing, key=lambda x: x[0]):
            print(
                f"[coverage-branches]   missing: {rel} ({bt.pct:6.2f}% = {bt.covered}/{bt.count})",
                file=sys.stderr,
            )
        return 1

    if missing:
        # Even if pct hits 100 (shouldn't), still surface per-file gaps.
        for rel, bt in sorted(missing, key=lambda x: x[0]):
            print(f"[coverage-branches]   missing: {rel} ({bt.covered}/{bt.count})")
        return 1

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

