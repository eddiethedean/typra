import modelvault


def test_dbapi_connect_uses_read_only_handle(tmp_path):
    db_path = tmp_path / "app.modelvault"
    db = modelvault.Database.open(str(db_path))
    db.register_collection(
        "books",
        '[{"path": ["id"], "type": "int64"}]',
        "id",
    )
    del db
    import gc

    gc.collect()

    conn = modelvault.dbapi.connect(str(db_path))
    cur = conn.cursor()
    cur.execute("SELECT id FROM books")
    assert cur.fetchall() == []
    cur.close()
    conn.close()

    db2 = modelvault.Database.open(str(db_path))
    assert db2.path().endswith("app.modelvault")


def test_dbapi_connect_execute_fetch_and_description(tmp_path):
    db_path = tmp_path / "app.modelvault"
    db = modelvault.Database.open(str(db_path))
    db.register_collection(
        "books",
        '[{"path": ["id"], "type": "int64"}, {"path": ["title"], "type": "string"}, {"path": ["year"], "type": "int64"}]',
        "id",
    )
    db.insert("books", {"id": 1, "title": "A", "year": 2020})
    db.insert("books", {"id": 2, "title": "B", "year": 2021})

    conn = modelvault.dbapi.connect(str(db_path))
    cur = conn.cursor()
    cur.execute(
        "SELECT id,title FROM books WHERE year >= ? ORDER BY id DESC LIMIT 1", (2020,)
    )

    assert cur.description is not None
    row = cur.fetchone()
    assert row == (2, "B")
    assert cur.fetchone() is None


def test_dbapi_rejects_wrong_param_count(tmp_path):
    db_path = tmp_path / "app.modelvault"
    db = modelvault.Database.open(str(db_path))
    db.register_collection(
        "books",
        '[{"path": ["id"], "type": "int64"}, {"path": ["year"], "type": "int64"}]',
        "id",
    )
    db.insert("books", {"id": 1, "year": 2020})

    conn = modelvault.dbapi.connect(str(db_path))
    cur = conn.cursor()
    try:
        cur.execute("SELECT * FROM books WHERE year >= ?", ())
        assert False, "expected ValueError"
    except ValueError:
        pass


def test_dbapi_fetchmany_is_incremental_for_large_results(tmp_path):
    db_path = tmp_path / "app.modelvault"
    db = modelvault.Database.open(str(db_path))
    db.register_collection(
        "books",
        '[{"path": ["id"], "type": "int64"}, {"path": ["year"], "type": "int64"}]',
        "id",
    )
    for i in range(2000):
        db.insert("books", {"id": i, "year": i % 5})

    conn = modelvault.dbapi.connect(str(db_path))
    cur = conn.cursor()
    cur.execute("SELECT id,year FROM books ORDER BY id ASC", None)

    a = cur.fetchmany(7)
    b = cur.fetchmany(7)
    assert len(a) == 7
    assert len(b) == 7
    assert a[0] == (0, 0)
    assert a[-1] == (6, 1)
    assert b[0] == (7, 2)


def test_dbapi_commit_and_rollback_are_callable(tmp_path):
    db_path = tmp_path / "app.modelvault"
    db = modelvault.Database.open(str(db_path))
    db.register_collection(
        "books",
        '[{"path": ["id"], "type": "int64"}]',
        "id",
    )
    db.insert("books", {"id": 1})

    conn = modelvault.dbapi.connect(str(db_path))
    cur = conn.cursor()
    cur.execute("SELECT * FROM books", None)
    assert cur.fetchone() == (1,)

    conn.commit()
    conn.rollback()


def test_dbapi_rejects_non_select_sql(tmp_path):
    db_path = tmp_path / "app.modelvault"
    db = modelvault.Database.open(str(db_path))
    db.register_collection(
        "books",
        '[{"path": ["id"], "type": "int64"}]',
        "id",
    )
    db.insert("books", {"id": 1})

    conn = modelvault.dbapi.connect(str(db_path))
    cur = conn.cursor()
    try:
        cur.execute("DELETE FROM books", None)
        assert False, "expected ValueError"
    except ValueError:
        pass
