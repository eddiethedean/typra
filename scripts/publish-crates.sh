#!/usr/bin/env bash
# Publish Typra Rust workspace crates to crates.io (typra-core, typra-derive, typra, typra-python).
# Requires CARGO_REGISTRY_TOKEN (or CRATES_IO_TOKEN). Idempotent: skips if version already exists.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"

if [[ -z "${CARGO_REGISTRY_TOKEN:-}" && -n "${CRATES_IO_TOKEN:-}" ]]; then
  export CARGO_REGISTRY_TOKEN="$CRATES_IO_TOKEN"
fi
if [[ -z "${CARGO_REGISTRY_TOKEN:-}" ]]; then
  echo "error: set CARGO_REGISTRY_TOKEN (or CRATES_IO_TOKEN) for crates.io" >&2
  exit 1
fi

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
cargo_publish_allow_duplicate typra
cargo_publish_allow_duplicate typra-python

echo "Crates publish done."
