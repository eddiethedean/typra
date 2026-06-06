#!/usr/bin/env bash
# Verifies stdout from the minimal Rust and Python snippets shown in README / guides.
# Covered: root README (Rust + Pydantic Python), docs/guides/quickstart.md (Rust cmd + Python),
# docs/guides/python.md (quick start + query + realistic workflow + fields example),
# python/modelvault/README.md (Pydantic quick start).
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

# --- Rust: crates/modelvault/examples/open.rs (also embedded in README + guide_getting_started) ---
read -r -d '' EXPECT_RUST <<'EOF' || true
opened: :memory:
registered collection id=1 version=1

EOF
ACTUAL_RUST=$(cargo run -q -p modelvault --example open | strip_cr)
[[ "$ACTUAL_RUST" == "$EXPECT_RUST" ]] || {
  echo "Rust example output mismatch. Update scripts/verify-doc-examples.sh and docs (guide_getting_started, root README, crates/modelvault/README, guide_python)." >&2
  diff -u <(printf '%s' "$EXPECT_RUST") <(printf '%s' "$ACTUAL_RUST") >&2 || true
  exit 1
}

# --- Python: docs/guides/quickstart.md "Run it (from this repo)" ---
read -r -d '' EXPECT_PY_GUIDE <<'EOF' || true
get: Book(title='Hello', year=2020, rating=4.5)
modelvault 0.15.2

EOF
ACTUAL_PY_GUIDE=$("$PYTHON" <<'PY' | strip_cr
# Setup: class-defined schema + in-memory DB.
from __future__ import annotations

from dataclasses import dataclass
from typing import Annotated, Optional

import modelvault


@dataclass
class Book:
    __modelvault_primary_key__ = "title"
    __modelvault_indexes__ = [
        modelvault.models.index("year"),
        modelvault.models.unique("title"),
    ]

    title: str
    year: Annotated[int, modelvault.models.constrained(min_i64=0)]
    rating: Optional[float] = None


db = modelvault.Database.open_in_memory()
books = modelvault.models.collection(db, Book)
books.insert(Book(title="Hello", year=2020, rating=4.5))
print("get:", books.get("Hello"))
print("modelvault", modelvault.__version__)
PY
)
[[ "$ACTUAL_PY_GUIDE" == "$EXPECT_PY_GUIDE" ]] || {
  echo "Python (docs/guides/quickstart) output mismatch." >&2
  diff -u <(printf '%s' "$EXPECT_PY_GUIDE") <(printf '%s' "$ACTUAL_PY_GUIDE") >&2 || true
  exit 1
}

# --- Python: root README.md (Pydantic 60-second example) ---
read -r -d '' EXPECT_PY_ROOT <<'EOF' || true
title='Hello' year=2020
0.15.2

EOF
ACTUAL_PY_ROOT=$("$PYTHON" <<'PY' | strip_cr
from pydantic import BaseModel
import modelvault

class Book(BaseModel):
    __modelvault_primary_key__ = "title"
    title: str
    year: int

db = modelvault.Database.open_in_memory()
books = modelvault.models.collection(db, Book)
books.insert(Book(title="Hello", year=2020))
print(books.get("Hello"))
print(modelvault.__version__)
PY
)
[[ "$ACTUAL_PY_ROOT" == "$EXPECT_PY_ROOT" ]] || {
  echo "Python (root README) output mismatch." >&2
  diff -u <(printf '%s' "$EXPECT_PY_ROOT") <(printf '%s' "$ACTUAL_PY_ROOT") >&2 || true
  exit 1
}

# --- Python: python/modelvault/README.md quick start (Pydantic) ---
read -r -d '' EXPECT_PY_PKG <<'EOF' || true
title='ModelVault' year=2020
0.15.2

EOF
ACTUAL_PY_PKG=$("$PYTHON" <<'PY' | strip_cr
from pydantic import BaseModel
import modelvault

class Book(BaseModel):
    __modelvault_primary_key__ = "title"
    title: str
    year: int

db = modelvault.Database.open_in_memory()
books = modelvault.models.collection(db, Book)
books.insert(Book(title="ModelVault", year=2020))
print(books.get("ModelVault"))
print(modelvault.__version__)
PY
)
[[ "$ACTUAL_PY_PKG" == "$EXPECT_PY_PKG" ]] || {
  echo "Python (python/modelvault/README) output mismatch." >&2
  diff -u <(printf '%s' "$EXPECT_PY_PKG") <(printf '%s' "$ACTUAL_PY_PKG") >&2 || true
  exit 1
}

# --- Python: docs/guides/python.md Quick start ---
read -r -d '' EXPECT_PY_GUIDE_PYTHON <<'EOF' || true
path: :memory:
collection_id: 1 schema_version: 1
collection_names: ['books']

EOF
ACTUAL_PY_GUIDE_PYTHON=$("$PYTHON" <<'PY' | strip_cr
# Setup: module, in-memory DB, and one collection.
import modelvault

db = modelvault.Database.open_in_memory()
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
  echo "Python (docs/guides/python quick start) output mismatch." >&2
  diff -u <(printf '%s' "$EXPECT_PY_GUIDE_PYTHON") <(printf '%s' "$ACTUAL_PY_GUIDE_PYTHON") >&2 || true
  exit 1
}

# --- Python: docs/guides/python.md "Query example" ---
read -r -d '' EXPECT_PY_GUIDE_QUERY <<'EOF' || true
index_lookup: True
rows: [{'title': 'Hello'}]

EOF
ACTUAL_PY_GUIDE_QUERY=$("$PYTHON" <<'PY' | strip_cr
# Setup: in-memory DB, schema, index, and one row.
import modelvault

db = modelvault.Database.open_in_memory()
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
  echo "Python (docs/guides/python query example) output mismatch." >&2
  diff -u <(printf '%s' "$EXPECT_PY_GUIDE_QUERY") <(printf '%s' "$ACTUAL_PY_GUIDE_QUERY") >&2 || true
  exit 1
}

# --- Python: docs/guides/python.md "Realistic workflow: indexed queries on disk" ---
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

import modelvault

with tempfile.TemporaryDirectory() as d:
    path = Path(d) / "app.modelvault"
    db = modelvault.Database.open(str(path))
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
    del q
    del db
    db2 = modelvault.Database.open(str(path))
    row = db2.get("order_lines", 1)
    print("reopen_qty:", row["qty"] if row else None)
PY
)
[[ "$ACTUAL_PY_GUIDE_WORKFLOW" == "$EXPECT_PY_GUIDE_WORKFLOW" ]] || {
  echo "Python (docs/guides/python realistic workflow) output mismatch." >&2
  diff -u <(printf '%s' "$EXPECT_PY_GUIDE_WORKFLOW") <(printf '%s' "$ACTUAL_PY_GUIDE_WORKFLOW") >&2 || true
  exit 1
}

# --- Python: docs/guides/python.md "Example: multiple top-level fields" ---
read -r -d '' EXPECT_PY_GUIDE_FIELDS <<'EOF' || true
collection_id: 1 schema_version: 1

EOF
ACTUAL_PY_GUIDE_FIELDS=$("$PYTHON" <<'PY' | strip_cr
# Setup: in-memory DB and a multi-field `books` schema (PK `title`).
import modelvault

db = modelvault.Database.open_in_memory()
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
  echo "Python (docs/guides/python fields example) output mismatch." >&2
  diff -u <(printf '%s' "$EXPECT_PY_GUIDE_FIELDS") <(printf '%s' "$ACTUAL_PY_GUIDE_FIELDS") >&2 || true
  exit 1
}

# --- Python: docs/ops/operations_and_failure_modes.md "Operational smoke test (Python)" ---
read -r -d '' EXPECT_PY_OPS <<'EOF' || true
opened: :memory:
names: ['books']
get: {'title': 'Hello'}

EOF
ACTUAL_PY_OPS=$("$PYTHON" <<'PY' | strip_cr
import modelvault

db = modelvault.Database.open_in_memory()
db.register_collection("books", '[{"path": ["title"], "type": "string"}]', "title")
db.insert("books", {"title": "Hello"})

print("opened:", db.path())
print("names:", db.collection_names())
print("get:", db.get("books", "Hello"))
PY
)
[[ "$ACTUAL_PY_OPS" == "$EXPECT_PY_OPS" ]] || {
  echo "Python (docs/ops/operations_and_failure_modes) output mismatch." >&2
  diff -u <(printf '%s' "$EXPECT_PY_OPS") <(printf '%s' "$ACTUAL_PY_OPS") >&2 || true
  exit 1
}

echo "verify-doc-examples: OK (Rust open + 8 Python snippets)"
