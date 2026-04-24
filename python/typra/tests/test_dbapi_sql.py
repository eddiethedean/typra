import typra


def test_dbapi_connect_execute_fetch_and_description(tmp_path):
    db_path = tmp_path / "app.typra"
    db = typra.Database.open(str(db_path))
    db.register_collection(
        "books",
        '[{"path": ["id"], "type": "int64"}, {"path": ["title"], "type": "string"}, {"path": ["year"], "type": "int64"}]',
        "id",
    )
    db.insert("books", {"id": 1, "title": "A", "year": 2020})
    db.insert("books", {"id": 2, "title": "B", "year": 2021})

    conn = typra.dbapi.connect(str(db_path))
    cur = conn.cursor()
    cur.execute(
        "SELECT id,title FROM books WHERE year >= ? ORDER BY id DESC LIMIT 1", (2020,)
    )

    assert cur.description is not None
    row = cur.fetchone()
    assert row == (2, "B")
    assert cur.fetchone() is None


def test_dbapi_rejects_wrong_param_count(tmp_path):
    db_path = tmp_path / "app.typra"
    db = typra.Database.open(str(db_path))
    db.register_collection(
        "books",
        '[{"path": ["id"], "type": "int64"}, {"path": ["year"], "type": "int64"}]',
        "id",
    )
    db.insert("books", {"id": 1, "year": 2020})

    conn = typra.dbapi.connect(str(db_path))
    cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM books WHERE year >= ?", ())
        assert False, "expected ValueError"
    except ValueError:
        pass
