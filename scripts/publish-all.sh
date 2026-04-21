#!/usr/bin/env bash
# Publish all Typra Rust crates to crates.io and the Python wheel to PyPI.
# Run from a shell where your credentials are exported (or configure the same
# variables for Cursor / CI).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# --- PyPI (maturin) ------------------------------------------------------
if [[ -z "${MATURIN_PYPI_TOKEN:-}" ]]; then
  if [[ "${TWINE_USERNAME:-}" == "__token__" && -n "${TWINE_PASSWORD:-}" ]]; then
    export MATURIN_PYPI_TOKEN="$TWINE_PASSWORD"
  elif [[ -n "${PYPI_TOKEN:-}" ]]; then
    export MATURIN_PYPI_TOKEN="$PYPI_TOKEN"
  elif [[ -n "${PYPI_API_TOKEN:-}" ]]; then
    export MATURIN_PYPI_TOKEN="$PYPI_API_TOKEN"
  fi
fi
if [[ -z "${MATURIN_PYPI_TOKEN:-}" ]]; then
  echo "error: set MATURIN_PYPI_TOKEN, PYPI_API_TOKEN, PYPI_TOKEN, or TWINE_USERNAME=__token__ with TWINE_PASSWORD" >&2
  exit 1
fi

export MATURIN_NON_INTERACTIVE="${MATURIN_NON_INTERACTIVE:-true}"

"$ROOT/scripts/publish-crates.sh"

echo "Publishing Python package to PyPI (current platform)..."
(cd "$ROOT/python/typra" && maturin publish --skip-existing)

echo "Done."
