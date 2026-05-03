from __future__ import annotations

import argparse
import os
import sys
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class LcovFile:
    path: str
    lines_hit: int
    lines_found: int

    @property
    def line_pct(self) -> float:
        if self.lines_found == 0:
            return 100.0
        return 100.0 * (self.lines_hit / self.lines_found)


def parse_lcov_files(path: Path) -> list[LcovFile]:
    files: list[LcovFile] = []

    cur_path: str | None = None
    da_hit = 0
    da_found = 0
    lf_lh_hit: int | None = None
    lf_lh_found: int | None = None

    def flush() -> None:
        nonlocal cur_path, da_hit, da_found, lf_lh_hit, lf_lh_found
        if cur_path is not None:
            # Prefer LF/LH when present: matches `cargo llvm-cov --fail-under-lines`
            # (DA-only aggregation can disagree when instrumentation attributes differ).
            if lf_lh_hit is not None and lf_lh_found is not None:
                files.append(LcovFile(cur_path, lf_lh_hit, lf_lh_found))
            elif da_found > 0:
                files.append(LcovFile(cur_path, da_hit, da_found))
            else:
                files.append(LcovFile(cur_path, 0, 0))
        cur_path = None
        da_hit = 0
        da_found = 0
        lf_lh_hit = None
        lf_lh_found = None

    for raw in path.read_text().splitlines():
        if raw.startswith("SF:"):
            flush()
            cur_path = raw.removeprefix("SF:")
        elif raw.startswith("DA:"):
            # DA:<line>,<count>[,<checksum>]
            parts = raw.removeprefix("DA:").split(",", 2)
            if len(parts) >= 2:
                da_found += 1
                if int(parts[1]) > 0:
                    da_hit += 1
        elif raw.startswith("LF:"):
            lf_lh_found = int(raw.removeprefix("LF:").strip())
        elif raw.startswith("LH:"):
            lf_lh_hit = int(raw.removeprefix("LH:").strip())
        elif raw == "end_of_record":
            flush()

    flush()
    return files


def _bucket_for(rel: str) -> str | None:
    # "Core logic" areas we gate.
    if rel == "crates/typra-core/src/db/mod.rs":
        return "db"
    if rel.startswith("crates/typra-core/src/query/"):
        return "query"
    if rel == "crates/typra-core/src/index.rs":
        return "index"
    if rel == "crates/typra-core/src/validation.rs":
        return "validation"
    return None


def _as_repo_relative(sf: str, repo_root: Path) -> str:
    p = Path(sf)
    if p.is_absolute():
        try:
            return str(p.relative_to(repo_root))
        except ValueError:
            return str(p)
    return sf


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("lcov_path", type=Path)
    ap.add_argument("--db-min-lines", type=float, required=True)
    ap.add_argument("--query-min-lines", type=float, required=True)
    ap.add_argument("--index-min-lines", type=float, required=True)
    ap.add_argument("--validation-min-lines", type=float, required=True)
    args = ap.parse_args()

    repo_root = Path(os.getcwd())
    files = parse_lcov_files(args.lcov_path)

    buckets: dict[str, list[LcovFile]] = {"db": [], "query": [], "index": [], "validation": []}

    for f in files:
        rel = _as_repo_relative(f.path, repo_root)
        b = _bucket_for(rel)
        if b is not None:
            buckets[b].append(LcovFile(rel, f.lines_hit, f.lines_found))

    thresholds = {
        "db": args.db_min_lines,
        "query": args.query_min_lines,
        "index": args.index_min_lines,
        "validation": args.validation_min_lines,
    }

    failed: list[str] = []
    for name, flist in buckets.items():
        hit = sum(x.lines_hit for x in flist)
        found = sum(x.lines_found for x in flist)
        pct = 100.0 if found == 0 else 100.0 * (hit / found)
        thr = thresholds[name]
        print(f"[coverage-core] {name:10s}: {pct:6.2f}% (hit {hit} / found {found}), min {thr:.2f}%")
        if pct + 1e-9 < thr:
            failed.append(name)

    if failed:
        print(f"[coverage-core] FAIL: below threshold in {', '.join(failed)}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

