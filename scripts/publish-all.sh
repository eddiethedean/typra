#!/usr/bin/env bash
# Publish all Typra Rust crates to crates.io and the Python wheel to PyPI.
# Run from a shell where your credentials are exported (or configure the same
# variables for Cursor / CI).
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

# --- crates.io -----------------------------------------------------------
# Cargo only reads CARGO_REGISTRY_TOKEN (see `cargo login` / crates.io API tokens).
if [[ -z "${CARGO_REGISTRY_TOKEN:-}" && -n "${CRATES_IO_TOKEN:-}" ]]; then
  export CARGO_REGISTRY_TOKEN="$CRATES_IO_TOKEN"
fi
if [[ -z "${CARGO_REGISTRY_TOKEN:-}" ]]; then
  echo "error: set CARGO_REGISTRY_TOKEN (or CRATES_IO_TOKEN) for crates.io" >&2
  exit 1
fi

# --- PyPI (maturin) ------------------------------------------------------
# Prefer MATURIN_PYPI_TOKEN. If you use twine-style API tokens:
#   TWINE_USERNAME=__token__  TWINE_PASSWORD=pypi-...
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

# Maturin expects "true" or "false", not 1/0.
export MATURIN_NON_INTERACTIVE="${MATURIN_NON_INTERACTIVE:-true}"

# Idempotent re-runs (e.g. re-pushed tag): crates.io rejects duplicate versions.
cargo_publish_allow_duplicate() {
  local pkg=$1
  local out ec
  set +e
  out=$(cargo publish -p "$pkg" 2>&1)
  ec=$?
  set -e
  if [[ "$ec" -eq 0 ]]; then
    printf '%s\n' "$out"
    return 0
  fi
  if printf '%s\n' "$out" | grep -q 'already exists'; then
    echo "Note: $pkg is already on crates.io; skipping." >&2
    return 0
  fi
  printf '%s\n' "$out" >&2
  return "$ec"
}

echo "Publishing Rust crates to crates.io..."
cargo_publish_allow_duplicate typra-core
cargo_publish_allow_duplicate typra-derive
cargo_publish_allow_duplicate typra-python

echo "Publishing Python package to PyPI..."
(cd "$ROOT/python/typra" && maturin publish --skip-existing)

echo "Done."
