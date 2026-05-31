#!/usr/bin/env bash
# Regenerate committed .typra format fixtures (see crates/typra-core/tests/fixtures/format/).
set -euo pipefail
cd "$(dirname "$0")/.."
cargo test -p typra-core --test format_back_compat_1x export_format_fixtures -- --ignored --exact
echo "Wrote fixtures under crates/typra-core/tests/fixtures/format/"
