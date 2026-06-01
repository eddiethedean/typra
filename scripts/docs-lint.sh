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

# Stale branding / install ranges after the ModelVault rebrand (0.14.x).
STALE_PATTERNS=(
  "\\btypra\\b"
  "modelvault>=1\\.0\\.0,<2"
  "modelvault = \"1\\.0\""
  "pip install \"typra"
)

DOC_PATHS=(README.md docs python/modelvault/README.md crates/modelvault/README.md)
DOC_EXCLUDE=(--exclude=MODELVAULT_REBRAND_PLAN.md)

for pat in "${STALE_PATTERNS[@]}"; do
  if grep -R --line-number -E "$pat" "${DOC_EXCLUDE[@]}" "${DOC_PATHS[@]}" >/dev/null 2>&1; then
    echo "Found stale doc pattern: $pat" >&2
    grep -R --line-number -E "$pat" "${DOC_EXCLUDE[@]}" "${DOC_PATHS[@]}" >&2 || true
    exit 1
  fi
done

echo "docs-lint: OK"

