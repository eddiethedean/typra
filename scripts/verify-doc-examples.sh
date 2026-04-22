#!/usr/bin/env bash
# Verifies stdout from the minimal Rust and Python snippets shown in README / guides.
# Covered: root README (Rust + Python), docs/guide_getting_started.md (Rust cmd + Python),
# docs/guide_python.md (quick start), python/typra/README.md (quick start).
# When outputs change intentionally, update the expected heredocs here and the matching ```text blocks.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"
# Default venv interpreter: Unix uses .venv/bin/python; Windows uses .venv/Scripts/python.exe
if [[ -z "${PYTHON:-}" ]]; then
  if [[ -x "$ROOT/.venv/bin/python" ]]; then
    PYTHON="$ROOT/.venv/bin/python"
  elif [[ -f "$ROOT/.venv/Scripts/python.exe" ]]; then
    PYTHON="$ROOT/.venv/Scripts/python.exe"
  else
    PYTHON="$ROOT/.venv/bin/python"
  fi
fi

strip_cr() {
  tr -d '\r'
}

fail() {
  echo "$1" >&2
  exit 1
}

{ [[ -x "$PYTHON" ]] || [[ -f "$PYTHON" ]]; } || fail "Need a venv with the extension built (e.g. make python-develop). PYTHON=$PYTHON"

# --- Rust: crates/typra/examples/open.rs (also embedded in README + guide_getting_started) ---
read -r -d '' EXPECT_RUST <<'EOF' || true
opened: :memory:
registered collection id=1 version=1

EOF
ACTUAL_RUST=$(cargo run -q -p typra --example open | strip_cr)
[[ "$ACTUAL_RUST" == "$EXPECT_RUST" ]] || {
  echo "Rust example output mismatch. Update scripts/verify-doc-examples.sh and docs (guide_getting_started, root README, crates/typra/README, guide_python)." >&2
  diff -u <(printf '%s' "$EXPECT_RUST") <(printf '%s' "$ACTUAL_RUST") >&2 || true
  exit 1
}

# --- Python: docs/guide_getting_started.md "Run it (from this repo)" ---
read -r -d '' EXPECT_PY_GUIDE <<'EOF' || true
registered collection_id= 1 schema_version= 1
get: {'title': 'Hello'}
typra 0.6.0

EOF
ACTUAL_PY_GUIDE=$("$PYTHON" <<'PY' | strip_cr
import typra

db = typra.Database.open_in_memory()
cid, ver = db.register_collection(
    "books",
    '[{"path": ["title"], "type": "string"}]',
    "title",
)
print("registered collection_id=", cid, "schema_version=", ver)
db.insert("books", {"title": "Hello"})
print("get:", db.get("books", "Hello"))
print("typra", typra.__version__)
PY
)
[[ "$ACTUAL_PY_GUIDE" == "$EXPECT_PY_GUIDE" ]] || {
  echo "Python (guide_getting_started) output mismatch." >&2
  diff -u <(printf '%s' "$EXPECT_PY_GUIDE") <(printf '%s' "$ACTUAL_PY_GUIDE") >&2 || true
  exit 1
}

# --- Python: root README.md (Python section) ---
read -r -d '' EXPECT_PY_ROOT <<'EOF' || true
{'title': 'Hello'}
0.6.0

EOF
ACTUAL_PY_ROOT=$("$PYTHON" <<'PY' | strip_cr
import typra

db = typra.Database.open_in_memory()
_, _ = db.register_collection(
    "books",
    '[{"path": ["title"], "type": "string"}]',
    "title",
)
db.insert("books", {"title": "Hello"})
print(db.get("books", "Hello"))
print(typra.__version__)
PY
)
[[ "$ACTUAL_PY_ROOT" == "$EXPECT_PY_ROOT" ]] || {
  echo "Python (root README) output mismatch." >&2
  diff -u <(printf '%s' "$EXPECT_PY_ROOT") <(printf '%s' "$ACTUAL_PY_ROOT") >&2 || true
  exit 1
}

# --- Python: python/typra/README.md quick start ---
read -r -d '' EXPECT_PY_PKG <<'EOF' || true
registered 1 1
{'title': 'Typra'}
0.6.0

EOF
ACTUAL_PY_PKG=$("$PYTHON" <<'PY' | strip_cr
import typra

db = typra.Database.open_in_memory()
cid, ver = db.register_collection(
    "books",
    '[{"path": ["title"], "type": "string"}]',
    "title",
)
print("registered", cid, ver)
db.insert("books", {"title": "Typra"})
print(db.get("books", "Typra"))
print(typra.__version__)
PY
)
[[ "$ACTUAL_PY_PKG" == "$EXPECT_PY_PKG" ]] || {
  echo "Python (python/typra/README) output mismatch." >&2
  diff -u <(printf '%s' "$EXPECT_PY_PKG") <(printf '%s' "$ACTUAL_PY_PKG") >&2 || true
  exit 1
}

# --- Python: docs/guide_python.md Quick start ---
read -r -d '' EXPECT_PY_GUIDE_PYTHON <<'EOF' || true
path: :memory:
collection_id: 1 schema_version: 1
collection_names: ['books']

EOF
ACTUAL_PY_GUIDE_PYTHON=$("$PYTHON" <<'PY' | strip_cr
import typra

db = typra.Database.open_in_memory()
cid, ver = db.register_collection(
    "books",
    '[{"path": ["title"], "type": "string"}]',
    "title",
)
print("path:", db.path())
print("collection_id:", cid, "schema_version:", ver)
print("collection_names:", db.collection_names())
PY
)
[[ "$ACTUAL_PY_GUIDE_PYTHON" == "$EXPECT_PY_GUIDE_PYTHON" ]] || {
  echo "Python (guide_python quick start) output mismatch." >&2
  diff -u <(printf '%s' "$EXPECT_PY_GUIDE_PYTHON") <(printf '%s' "$ACTUAL_PY_GUIDE_PYTHON") >&2 || true
  exit 1
}

echo "verify-doc-examples: OK (Rust open + 4 Python snippets)"
