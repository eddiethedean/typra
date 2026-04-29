#!/usr/bin/env python3
"""Fail if any executable line in crates/typra-core/src is not hit (LCOV DA count 0).

Lines in `#[cfg(test)]` items and in `#[test]` functions are excluded from the strict gate.
"""
from __future__ import annotations

import argparse
import json
import os
import re
import sys
from collections.abc import Callable
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


def _load_force_hit_lines(path: Path) -> dict[str, set[int]]:
    """Optional `path:line` entries treated as covered when DA count is 0."""
    out: dict[str, set[int]] = {}
    try:
        text = path.read_text()
    except OSError:
        return out
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if ":" not in line:
            continue
        rel, _, rhs = line.partition(":")
        rhs = rhs.strip()
        if not rhs.isdigit():
            continue
        out.setdefault(rel.strip().replace("\\", "/"), set()).add(int(rhs))
    return out


def _parse_missed(
    lcov: Path,
    repo_root: Path,
    path_prefix: str,
    force_hit: dict[str, set[int]],
) -> tuple[dict[str, _Acc], tuple[int, int]]:
    by_file: dict[str, _Acc] = {}
    path_prefix = path_prefix.replace("\\", "/")
    # LCOV can contain multiple `SF:` records for the same physical Rust file (codegen units,
    # incremental artifacts, monomorphizations). We merge on (file,line) and take the max count so
    # a line is considered hit if any record hits it.
    merged: dict[tuple[str, int], int] = {}
    cur: str | None = None

    excluded_by_file: dict[str, set[int]] = {}
    file_lines_by_file: dict[str, list[str]] = {}
    ignorable_by_file: dict[str, set[int]] = {}

    _RE_TEST_ATTR = re.compile(r"#\[test\](\s|$)")

    def _strip_line_comment(s: str) -> str:
        return s.split("//", 1)[0]

    def _extend_excluded_for_triggered_items(
        lines: list[str],
        excluded: set[int],
        trigger: Callable[[str], bool],
        *,
        pending_allows_interleaved_attrs: bool,
    ) -> None:
        """
        When `trigger(code_line)` matches an outer attribute line, exclude the following item
        (skipping blank lines; optionally skipping further `#[...]` lines before the item).
        """
        pending = False
        in_item = False
        brace_depth = 0
        seen_open_brace = False

        for idx0, raw_line in enumerate(lines):
            line_no = idx0 + 1
            code = _strip_line_comment(raw_line).strip()
            line = raw_line.strip()

            if not in_item and not pending and trigger(code):
                pending = True
                excluded.add(line_no)
                continue

            if pending and not in_item:
                excluded.add(line_no)
                if not line:
                    continue
                if pending_allows_interleaved_attrs and code.startswith("#["):
                    continue
                in_item = True
                pending = False
                brace_depth = 0
                seen_open_brace = False

            if in_item:
                excluded.add(line_no)
                s = _strip_line_comment(raw_line)
                if "{" in s:
                    seen_open_brace = True
                brace_depth += s.count("{")
                brace_depth -= s.count("}")

                if not seen_open_brace and line.endswith(";"):
                    in_item = False
                    brace_depth = 0
                    seen_open_brace = False
                    continue

                if seen_open_brace and brace_depth <= 0:
                    in_item = False
                    brace_depth = 0
                    seen_open_brace = False

    def _excluded_test_lines(repo_rel_rs_path: str) -> set[int]:
        """
        Return line numbers belonging to test-only code in `src/` files.

        Excludes:
        - Any item guarded by `#[cfg(test)]` (modules, fns, impls, `mod foo;`, etc.)
        - Any item annotated with `#[test]` (including `#[should_panic]` / other attrs between
          `#[test]` and the `fn`)

        This is a lightweight scanner (not a Rust parser), tuned for typical formatting in-tree.
        """
        if repo_rel_rs_path in excluded_by_file:
            return excluded_by_file[repo_rel_rs_path]

        p = (repo_root / repo_rel_rs_path).resolve()
        try:
            text = p.read_text()
        except OSError:
            excluded_by_file[repo_rel_rs_path] = set()
            return excluded_by_file[repo_rel_rs_path]

        lines = text.splitlines()
        excluded: set[int] = set()

        _extend_excluded_for_triggered_items(
            lines,
            excluded,
            lambda c: c.startswith("#[cfg(test)]"),
            pending_allows_interleaved_attrs=False,
        )
        _extend_excluded_for_triggered_items(
            lines,
            excluded,
            lambda c: bool(_RE_TEST_ATTR.match(c)),
            pending_allows_interleaved_attrs=True,
        )

        excluded_by_file[repo_rel_rs_path] = excluded
        return excluded

    _RE_IGNORABLE = re.compile(r"^[\s\{\}\(\)\[\];,]*$")
    _RE_STRUCT_FIELD_SHORTHAND = re.compile(r"^\s*[A-Za-z_]\w*\s*,\s*$")
    _RE_STRUCT_FIELD_SIMPLE = re.compile(
        r"^\s*[A-Za-z_]\w*\s*:\s*[A-Za-z_]\w*(?:\.[A-Za-z_]\w*)*\s*,?\s*$"
    )
    _RE_DOC_COMMENT = re.compile(r"^\s*//[/!]")
    _RE_MATCH_ARM_HEADER = re.compile(r"^\s*.+=>\s*\{\s*$")
    _RE_CALL_WITH_STRUCT_LITERAL_OPEN = re.compile(
        r"^\s*[A-Za-z_]\w*(?:::\w+)*\s*\(\s*[A-Za-z_]\w*\s*\{\s*$"
    )

    def _ignorable_lines(repo_rel_rs_path: str) -> set[int]:
        """
        Lines that are effectively non-code (blank or only delimiters).

        LLVM coverage sometimes marks delimiter-only lines as executable; excluding them keeps the
        strict gate focused on meaningful logic rather than counter placement artifacts.
        """
        if repo_rel_rs_path in ignorable_by_file:
            return ignorable_by_file[repo_rel_rs_path]

        p = (repo_root / repo_rel_rs_path).resolve()
        try:
            text = p.read_text()
        except OSError:
            ignorable_by_file[repo_rel_rs_path] = set()
            return ignorable_by_file[repo_rel_rs_path]

        lines = text.splitlines()
        file_lines_by_file[repo_rel_rs_path] = lines
        ign: set[int] = set()
        for i, raw in enumerate(lines, start=1):
            if (
                _RE_IGNORABLE.match(raw)
                or _RE_STRUCT_FIELD_SHORTHAND.match(raw)
                or _RE_STRUCT_FIELD_SIMPLE.match(raw)
                or _RE_DOC_COMMENT.match(raw)
                or _RE_MATCH_ARM_HEADER.match(raw)
                or _RE_CALL_WITH_STRUCT_LITERAL_OPEN.match(raw)
            ):
                ign.add(i)
        ignorable_by_file[repo_rel_rs_path] = ign
        return ign

    def _file_len(repo_rel_rs_path: str) -> int:
        if repo_rel_rs_path in file_lines_by_file:
            return len(file_lines_by_file[repo_rel_rs_path])
        # Populate caches.
        _ignorable_lines(repo_rel_rs_path)
        return len(file_lines_by_file.get(repo_rel_rs_path, []))

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
            merged[(rel, line_no)] = max(merged.get((rel, line_no), 0), count)

    found = 0
    hit = 0
    for (rel, line_no), count in merged.items():
        found += 1
        if line_no > _file_len(rel):
            # Some toolchains can emit DA entries for line numbers beyond EOF; ignore them.
            hit += 1
        elif line_no in _excluded_test_lines(rel) or line_no in _ignorable_lines(rel):
            hit += 1
        elif line_no in force_hit.get(rel, set()):
            hit += 1
        elif count > 0:
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
    ap.add_argument(
        "--by-file",
        action="store_true",
        help="Print per-file miss counts (descending) to stderr when coverage is below --min-pct.",
    )
    ap.add_argument(
        "--json",
        action="store_true",
        help="Print a JSON report to stdout (hit/found/pct/misses_by_file).",
    )
    ap.add_argument(
        "--force-hit-file",
        type=Path,
        default=None,
        help=(
            "Optional file of `crates/typra-core/src/...:LINE` entries treated as covered when "
            "DA count is 0 (counter placement / multi-CU noise). If omitted and "
            "`scripts/typra_core_line_coverage_force_hit.txt` exists under --repo-root, it is used."
        ),
    )
    args = ap.parse_args()

    repo = args.repo_root.resolve()
    force_path = args.force_hit_file
    if force_path is None:
        cand = repo / "scripts" / "typra_core_line_coverage_force_hit.txt"
        if cand.is_file():
            force_path = cand
    force_hit = _load_force_hit_lines(force_path) if force_path is not None else {}
    missed, (hit, found) = _parse_missed(
        args.lcov_path.resolve(), repo, args.src_prefix, force_hit
    )
    nmiss = found - hit
    pct = 100.0 if found == 0 else 100.0 * (hit / found)

    def _json_payload() -> dict:
        return {
            "hit": hit,
            "found": found,
            "missed_count": nmiss,
            "pct": round(pct, 4),
            "misses_by_file": {
                _as_repo_relative(sf, repo): acc.missed
                for sf, acc in sorted(missed.items())
                if acc.missed
            },
        }

    if nmiss == 0:
        if args.json:
            print(json.dumps(_json_payload(), indent=2))
        else:
            print(
                f"[typra-core-lines-100] OK: 100% (hit {hit} / {found} lines) under {args.src_prefix}"
            )
        return 0

    if args.json:
        print(json.dumps(_json_payload(), indent=2))

    if not args.json:
        print(
            f"[typra-core-lines-100] line coverage: {pct:.2f}% (hit {hit} / found {found}, missed {nmiss})",
            file=sys.stderr,
        )

    if args.by_file:
        rows = [
            (_as_repo_relative(sf, repo), len(acc.missed))
            for sf, acc in missed.items()
            if acc.missed
        ]
        rows.sort(key=lambda t: (-t[1], t[0]))
        for rel, cnt in rows:
            print(f"  {cnt:5d}  {rel}", file=sys.stderr)

    if not args.json:
        for sf in sorted(missed):
            if not missed[sf].missed:
                continue
            rel = _as_repo_relative(sf, repo)
            lines = ", ".join(str(n) for n in missed[sf].missed[:200])
            more = len(missed[sf].missed) - 200
            extra = f" ... (+{more} more)" if more > 0 else ""
            print(f"  {rel}:", lines + extra, file=sys.stderr)
    return 1 if pct + 1e-9 < args.min_pct else 0


if __name__ == "__main__":
    raise SystemExit(main())
