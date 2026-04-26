#!/usr/bin/env python3
"""
Compare two Criterion result directories (very small helper).

Usage:
  python scripts/bench_compare.py --base path/to/base/criterion --new path/to/new/criterion
"""

from __future__ import annotations

import argparse
import json
import pathlib
from dataclasses import dataclass


@dataclass(frozen=True)
class Bench:
    name: str
    mean: float


def read_benches(root: pathlib.Path) -> dict[str, Bench]:
    out: dict[str, Bench] = {}
    for estimates in root.glob("**/new/estimates.json"):
        try:
            d = json.loads(estimates.read_text())
            mean = float(d["mean"]["point_estimate"])
        except Exception:
            continue
        name = str(estimates.parent.parent.relative_to(root)).replace("\\", "/")
        out[name] = Bench(name=name, mean=mean)
    return out


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--base", type=pathlib.Path, required=True)
    ap.add_argument("--new", type=pathlib.Path, required=True)
    ap.add_argument("--warn-pct", type=float, default=10.0)
    args = ap.parse_args()

    base = read_benches(args.base)
    new = read_benches(args.new)

    names = sorted(set(base) & set(new))
    if not names:
        print("No overlapping benches found.")
        return 2

    worst = None
    for n in names:
        b = base[n].mean
        x = new[n].mean
        if b <= 0:
            continue
        pct = (x - b) / b * 100.0
        status = "OK"
        if pct >= args.warn_pct:
            status = "REGRESSION"
        print(f"{status:10s} {pct:+8.2f}%  {n}  (base={b:.6g}, new={x:.6g})")
        if worst is None or pct > worst[0]:
            worst = (pct, n)

    if worst:
        print(f"\nWorst delta: {worst[0]:+.2f}% at {worst[1]}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

