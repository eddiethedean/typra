#!/usr/bin/env bash
# Verifies stdout from the minimal Rust and Python snippets shown in README / guides.
# Covered: root README (Rust + Python), docs/guide_getting_started.md (Rust cmd + Python),
# docs/guide_python.md (quick start + query + realistic workflow + fields_json example),
# python/typra/README.md (quick start + indexed sketch + fields_json nested/multi examples).
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
typra 0.12.0

EOF
ACTUAL_PY_GUIDE=$("$PYTHON" <<'PY' | strip_cr
# Setup: module and in-memory database.
import typra

db = typra.Database.open_in_memory()
cid, ver = db.register_collection(
    "books",
    '[{"path": ["title"], "type": "string"}]',
    "title",
)
# Example: insert one row, read it back, print package version.
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
0.12.0

EOF
ACTUAL_PY_ROOT=$("$PYTHON" <<'PY' | strip_cr
# Setup: module, in-memory DB, and `books` collection (PK `title`).
import typra

db = typra.Database.open_in_memory()
_, _ = db.register_collection(
    "books",
    '[{"path": ["title"], "type": "string"}]',
    "title",
)
# Example: insert and read one row; print version.
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
0.12.0

EOF
ACTUAL_PY_PKG=$("$PYTHON" <<'PY' | strip_cr
# Setup: module, in-memory DB, and `books` collection (PK `title`).
import typra

db = typra.Database.open_in_memory()
cid, ver = db.register_collection(
    "books",
    '[{"path": ["title"], "type": "string"}]',
    "title",
)
# Example: insert one row, read it back, print package version.
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
# Setup: module, in-memory DB, and one collection.
import typra

db = typra.Database.open_in_memory()
cid, ver = db.register_collection(
    "books",
    '[{"path": ["title"], "type": "string"}]',
    "title",
)
# Example: show path, registration ids, and registered names.
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

# --- Python: docs/guide_python.md "Query example" ---
read -r -d '' EXPECT_PY_GUIDE_QUERY <<'EOF' || true
index_lookup: True
rows: [{'title': 'Hello'}]

EOF
ACTUAL_PY_GUIDE_QUERY=$("$PYTHON" <<'PY' | strip_cr
# Setup: in-memory DB, schema, index, and one row.
import typra

db = typra.Database.open_in_memory()
fields = (
    '[{"path": ["title"], "type": "string"}, {"path": ["year"], "type": "int64"}]'
)
indexes = '[{"name": "title_idx", "path": ["title"], "kind": "index"}]'
db.register_collection("books", fields, "title", indexes)
db.insert("books", {"title": "Hello", "year": 2020})
# Example: indexed equality query with subset projection.
explain = db.collection("books").where("title", "Hello").explain()
rows = db.collection("books").where("title", "Hello").all(fields=["title"])
print("index_lookup:", "IndexLookup" in explain)
print("rows:", rows)
PY
)
[[ "$ACTUAL_PY_GUIDE_QUERY" == "$EXPECT_PY_GUIDE_QUERY" ]] || {
  echo "Python (guide_python query example) output mismatch." >&2
  diff -u <(printf '%s' "$EXPECT_PY_GUIDE_QUERY") <(printf '%s' "$ACTUAL_PY_GUIDE_QUERY") >&2 || true
  exit 1
}

# --- Python: docs/guide_python.md "Realistic workflow: indexed queries on disk" ---
read -r -d '' EXPECT_PY_GUIDE_WORKFLOW <<'EOF' || true
indexed: True
matches: 2
rows: [{'id': 1, 'qty': 2, 'sku': 'SKU-A', 'status': 'open'}, {'id': 3, 'qty': 4, 'sku': 'SKU-A', 'status': 'open'}]
subset: [{'id': 1, 'qty': 2}, {'id': 3, 'qty': 4}]
reopen_qty: 2

EOF
ACTUAL_PY_GUIDE_WORKFLOW=$("$PYTHON" <<'PY' | strip_cr
# Setup: temp on-disk file, collection with indexes, and sample rows.
import tempfile
from pathlib import Path

import typra

with tempfile.TemporaryDirectory() as d:
    path = Path(d) / "app.typra"
    db = typra.Database.open(str(path))
    fields = """[
      {"path": ["id"], "type": "int64"},
      {"path": ["sku"], "type": "string"},
      {"path": ["qty"], "type": "int64"},
      {"path": ["status"], "type": "string"}
    ]"""
    indexes = """[
      {"name": "sku_idx", "path": ["sku"], "kind": "index"},
      {"name": "status_idx", "path": ["status"], "kind": "index"}
    ]"""
    db.register_collection("order_lines", fields, "id", indexes)
    for oid, sku, qty, st in [
        (1, "SKU-A", 2, "open"),
        (2, "SKU-B", 1, "shipped"),
        (3, "SKU-A", 4, "open"),
    ]:
        db.insert("order_lines", {"id": oid, "sku": sku, "qty": qty, "status": st})
    # Example: conjunctive query, subset projection, reopen and `get` by PK.
    q = (
        db.collection("order_lines")
        .where("status", "open")
        .and_where("sku", "SKU-A")
        .limit(10)
    )
    rows = sorted(q.all(), key=lambda r: r["id"])
    print("indexed:", "IndexLookup" in q.explain())
    print("matches:", len(rows))
    print("rows:", rows)
    short = sorted(
        db.collection("order_lines").where("status", "open").all(
            fields=["id", "qty"]
        ),
        key=lambda r: r["id"],
    )
    print("subset:", short)
    db2 = typra.Database.open(str(path))
    row = db2.get("order_lines", 1)
    print("reopen_qty:", row["qty"] if row else None)
PY
)
[[ "$ACTUAL_PY_GUIDE_WORKFLOW" == "$EXPECT_PY_GUIDE_WORKFLOW" ]] || {
  echo "Python (guide_python realistic workflow) output mismatch." >&2
  diff -u <(printf '%s' "$EXPECT_PY_GUIDE_WORKFLOW") <(printf '%s' "$ACTUAL_PY_GUIDE_WORKFLOW") >&2 || true
  exit 1
}

# --- Python: python/typra/README.md "Indexed query (sketch)" ---
read -r -d '' EXPECT_PY_PKG_INDEXED <<'EOF' || true
[{'id': 1, 'sku': 'abc'}]

EOF
ACTUAL_PY_PKG_INDEXED=$("$PYTHON" <<'PY' | strip_cr
# Setup: in-memory DB, indexed collection, one row.
import typra

db = typra.Database.open_in_memory()
fields = '[{"path": ["id"], "type": "int64"}, {"path": ["sku"], "type": "string"}]'
indexes = '[{"name": "sku_idx", "path": ["sku"], "kind": "index"}]'
db.register_collection("items", fields, "id", indexes)
db.insert("items", {"id": 1, "sku": "abc"})
# Example: equality query on indexed `sku`.
print(db.collection("items").where("sku", "abc").all())
PY
)
[[ "$ACTUAL_PY_PKG_INDEXED" == "$EXPECT_PY_PKG_INDEXED" ]] || {
  echo "Python (python/typra/README indexed sketch) output mismatch." >&2
  diff -u <(printf '%s' "$EXPECT_PY_PKG_INDEXED") <(printf '%s' "$ACTUAL_PY_PKG_INDEXED") >&2 || true
  exit 1
}

# --- Python: python/typra/README.md "Example (nested)" ---
read -r -d '' EXPECT_PY_PKG_FIELDS_NESTED <<'EOF' || true
nested: ['items']

EOF
ACTUAL_PY_PKG_FIELDS_NESTED=$("$PYTHON" <<'PY' | strip_cr
# Setup: in-memory DB and a collection whose PK uses an optional int field.
import typra

db = typra.Database.open_in_memory()
db.register_collection(
    "items",
    '[{"path": ["x"], "type": {"optional": "int64"}}]',
    "x",
)
# Example: confirm registration.
print("nested:", db.collection_names())
PY
)
[[ "$ACTUAL_PY_PKG_FIELDS_NESTED" == "$EXPECT_PY_PKG_FIELDS_NESTED" ]] || {
  echo "Python (python/typra/README fields nested) output mismatch." >&2
  diff -u <(printf '%s' "$EXPECT_PY_PKG_FIELDS_NESTED") <(printf '%s' "$ACTUAL_PY_PKG_FIELDS_NESTED") >&2 || true
  exit 1
}

# --- Python: python/typra/README.md "Example (multiple fields)" ---
read -r -d '' EXPECT_PY_PKG_FIELDS_MULTI <<'EOF' || true
multi: ['books']

EOF
ACTUAL_PY_PKG_FIELDS_MULTI=$("$PYTHON" <<'PY' | strip_cr
# Setup: in-memory DB and a multi-field `books` schema (PK `title`).
import typra

db = typra.Database.open_in_memory()
schema = """[
  {"path": ["title"], "type": "string"},
  {"path": ["year"], "type": "int64"},
  {"path": ["tags"], "type": {"list": "string"}}
]"""
db.register_collection("books", schema, "title")
# Example: confirm registration.
print("multi:", db.collection_names())
PY
)
[[ "$ACTUAL_PY_PKG_FIELDS_MULTI" == "$EXPECT_PY_PKG_FIELDS_MULTI" ]] || {
  echo "Python (python/typra/README fields multi) output mismatch." >&2
  diff -u <(printf '%s' "$EXPECT_PY_PKG_FIELDS_MULTI") <(printf '%s' "$ACTUAL_PY_PKG_FIELDS_MULTI") >&2 || true
  exit 1
}

# --- Python: docs/guide_python.md "Example: multiple top-level fields" ---
read -r -d '' EXPECT_PY_GUIDE_FIELDS <<'EOF' || true
collection_id: 1 schema_version: 1

EOF
ACTUAL_PY_GUIDE_FIELDS=$("$PYTHON" <<'PY' | strip_cr
# Setup: in-memory DB and a multi-field `books` schema (PK `title`).
import typra

db = typra.Database.open_in_memory()
fields = """[
  {"path": ["title"], "type": "string"},
  {"path": ["year"], "type": "int64"},
  {"path": ["tags"], "type": {"list": "string"}}
]"""
cid, ver = db.register_collection("books", fields, "title")
# Example: show assigned collection and schema version ids.
print("collection_id:", cid, "schema_version:", ver)
PY
)
[[ "$ACTUAL_PY_GUIDE_FIELDS" == "$EXPECT_PY_GUIDE_FIELDS" ]] || {
  echo "Python (guide_python fields example) output mismatch." >&2
  diff -u <(printf '%s' "$EXPECT_PY_GUIDE_FIELDS") <(printf '%s' "$ACTUAL_PY_GUIDE_FIELDS") >&2 || true
  exit 1
}

echo "verify-doc-examples: OK (Rust open + 10 Python snippets)"
