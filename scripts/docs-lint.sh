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

# Stale branding / install ranges after the ModelVault rebrand (0.16.x).
STALE_PATTERNS=(
  "\\btypra\\b"
  "modelvault>=1\\.0\\.0,<2"
  "modelvault = \"1\\.0\""
  "modelvault-core = \"1\\.0\""
  "modelvault-derive = \"1\\.0\""
  "version = \"1\\.0\""
  "pip install \"typra"
  "modelvault = \"0\\.15\""
  "modelvault-core = \"0\\.15\""
  "modelvault-derive = \"0\\.15\""
  "version = \"0\\.15\""
)

DOC_PATHS=(
  README.md
  docs
  python/modelvault/README.md
  crates/modelvault/README.md
  crates/modelvault-core/README.md
  crates/modelvault-derive/README.md
)
for pat in "${STALE_PATTERNS[@]}"; do
  if grep -R --line-number -E "$pat" "${DOC_PATHS[@]}" >/dev/null 2>&1; then
    echo "Found stale doc pattern: $pat" >&2
    grep -R --line-number -E "$pat" "${DOC_PATHS[@]}" >&2 || true
    exit 1
  fi
done

# No legacy product name in tracked sources (except guard patterns in this script).
if git grep -ni typra -- . \
  ':(exclude)scripts/docs-lint.sh' \
  ':(exclude)examples/.gitignore' \
  ':(exclude)CHANGELOG.md' \
  >/dev/null 2>&1; then
  echo "Found legacy 'typra' reference in tracked files:" >&2
  git grep -ni typra -- . ':(exclude)scripts/docs-lint.sh' >&2 || true
  exit 1
fi

echo "docs-lint: OK"

