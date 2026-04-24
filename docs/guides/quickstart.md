# Quickstart

Typra is a typed, embedded database with a Rust-first core and optional Python bindings.

## Current status (important)

As of **v0.13.x+**, Typra ships a **persisted schema catalog** (per-field **constraints** on catalog **v3**; **index definitions** on catalog **v4**), **secondary indexes**, richer **queries** (including `OR`, ranges, and `order_by`), **schema compatibility checks** + migration helpers, **record insert/get/delete** with **nested row values** (new writes use **record payload v2**; **v1** segments still replay), **engine validation** before append, **transactions** (`with db.transaction()` in Python / `Database::transaction` in Rust), **compaction**, **checkpoints** (faster reopen), in-memory databases + snapshots, and the first pieces of **bounded-memory query** scaffolding (ephemeral `Temp` spill segments; external sort plumbing behind `order_by`; spillable agg/join foundations).

- **Rust**: `Database::open`, **`open_with_options`**, **`register_collection(..., primary_field)`** / **`register_schema_version`**, **`insert` / `get` / `delete`** with **`RowValue`**, **`Database::open_in_memory`**, `Database::transaction`, typed **queries**, **secondary indexes**, migration helpers, and compaction (see [`ROADMAP.md`](https://github.com/eddiethedean/typra/blob/main/ROADMAP.md)).
- **Python**: `typra.Database.open`, **`open_in_memory`**, **`open_snapshot_bytes`**, **`register_collection(..., indexes_json=...)`** (optional **`constraints`** in `fields_json`), **`insert` / `get` / `delete`**, schema-version planning/registration helpers, **`db.collection(name).where(...).all()`** (plus **`and_where`**, **`limit`**, **`explain`**, subset **`all(fields=[...])`**), **`snapshot_bytes`**, compaction helpers, **`collection_names()`**, and `__version__`. A **disk + indexes** walkthrough is in **[Python guide — Realistic workflow](python.md#realistic-workflow-indexed-queries-on-disk)**.
- **Not yet**: arbitrary **SQL** text and SQLAlchemy integration. Typra ships a read-only **DB-API 2.0** adapter (`typra.dbapi`) with a minimal `SELECT` subset—see [Python guide](python.md#db-api-20-pep-249-and-sqlalchemy) and [`ROADMAP.md`](https://github.com/eddiethedean/typra/blob/main/ROADMAP.md).

Contributor-oriented layout (Rust crates and `typra-core` modules): [Rust crate/module layout](../specs/rust_crate_layout.md).

Compatibility and stability expectations (file-format minors + API policy): [Compatibility matrix](../reference/compatibility.md).

Supported types, constraints, indexes, and query operators: [Types matrix](../reference/types.md).

## Install (Rust)

In your application `Cargo.toml`:

    [dependencies]
    typra = "1.0"

## Minimal Rust example

This uses an **in-memory** database (no file; safe to run repeatedly). For an on-disk file, use `Database::open("my.typra")?` instead of `open_in_memory()`.

    use std::borrow::Cow;

    use typra::prelude::*;
    use typra::schema::FieldPath;
    use typra::FieldDef;
    use typra::Type;

    fn main() -> Result<(), DbError> {
        // Setup: in-memory database (no file on disk).
        let mut db = Database::open_in_memory()?;
        println!("opened: {}", db.path().display());
        // Example: register a `books` collection with a string primary key `title`.
        let (id, ver) = db.register_collection(
            "books",
            vec![FieldDef {
                path: FieldPath::new([Cow::Borrowed("title")])?,
                ty: Type::String,
                constraints: vec![],
            }],
            "title",
        )?;
        println!("registered collection id={} version={}", id.0, ver.0);
        Ok(())
    }

### Run it (from this repo)

The workspace includes the same program as **`crates/typra/examples/open.rs`**:

    cargo run -q -p typra --example open

Output:

    opened: :memory:
    registered collection id=1 version=1

## Install (Python)

    pip install "typra>=1.0.0,<2"

## Minimal Python example

In-memory (repeatable; same idea as the Rust example above):

    # Setup: class-defined schema + in-memory DB.
    from __future__ import annotations

    from dataclasses import dataclass
    from typing import Annotated, Optional

    import typra


    @dataclass
    class Book:
        __typra_primary_key__ = "title"
        __typra_indexes__ = [
            typra.models.index("year"),
            typra.models.unique("title"),
        ]

        title: str
        year: Annotated[int, typra.models.constrained(min_i64=0)]
        rating: Optional[float] = None


    db = typra.Database.open_in_memory()
    books = typra.models.collection(db, Book)

    books.insert(Book(title="Hello", year=2020, rating=4.5))
    print("get:", books.get("Hello"))
    print("typra", typra.__version__)

### Run it (from this repo)

Requires **Python 3.9+**. From the repository root, build the extension then run the snippet (bash):

    make python-develop
    .venv/bin/python <<'PY'
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

Output (the **`typra`** version line tracks the workspace / PyPI release):

    get: Book(title='Hello', year=2020, rating=4.5)
    typra 1.0.0

## Development quickstart (repo contributors)

From the repo root:

    python3 -m venv .venv
    .venv/bin/python -m pip install -U pip
    make check-full

This runs:

- Rust format/clippy/tests
- Python ruff/ty checks
- Python tests (via `maturin develop --release` + `pytest`)
- **`make verify-doc-examples`**: asserts stdout from `cargo run -p typra --example open` and the embedded Python snippets match each documented **text** output block on this page, the root **`README.md`**, [Python guide](python.md) (quick start, query, workflow, fields example), and **`python/typra/README.md`** (quick start, indexed sketch, **`fields_json`** examples). See **`scripts/verify-doc-examples.sh`** for the exact snippets.

## Where to go next

- **Concepts**: [Concepts](concepts.md)
- **Python** (`Database`, `fields_json`, errors): [Python guide](python.md)
- **Models & collections** (naming + subset models): [Models & collections](models_and_collections.md)
- **Storage modes** (disk vs in-memory vs hybrid/streaming): [Storage modes](storage_modes.md)

If you want deeper design specs (not a user guide), start in [Full architecture spec](../specs/full_architecture.md).

