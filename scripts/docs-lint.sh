#!/usr/bin/env bash
# Lightweight documentation drift checks.
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

fail() {
  echo "$1" >&2
  exit 1
}

# Ensure we are not accidentally committing generated output.
if git ls-files "site/**" | grep -q .; then
  fail "site/ is tracked by git; remove it from the index (build output only)."
fi

# Stale version strings / install ranges that should not appear in 1.x docs.
STALE_PATTERNS=(
  "typra>=0\\."
  "<0\\."
  "typra = \"0\\."
  "Status \\(v0\\."
)

for pat in "${STALE_PATTERNS[@]}"; do
  if grep -R --line-number -E "$pat" README.md docs python/typra/README.md crates/typra/README.md >/dev/null 2>&1; then
    echo "Found stale doc pattern: $pat" >&2
    grep -R --line-number -E "$pat" README.md docs python/typra/README.md crates/typra/README.md >&2 || true
    exit 1
  fi
done

echo "docs-lint: OK"

