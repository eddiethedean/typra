#!/usr/bin/env bash
# Regenerate committed .modelvault format fixtures (see crates/modelvault-core/tests/fixtures/format/).
set -euo pipefail
cd "$(dirname "$0")/.."
cargo test -p modelvault-core --test format_back_compat_1x export_format_fixtures --all-features -- --ignored --exact
echo "Wrote fixtures under crates/modelvault-core/tests/fixtures/format/"
