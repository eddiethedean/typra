//! 1.x on-disk backwards compatibility: 1.0-shaped `.typra` files must keep opening in 1.y.
//!
//! See `docs/reference/compatibility.md` and `docs/specs/format_evolution.md`.

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};

use typra_core::file_format::{
    decode_header, FileHeader, FILE_HEADER_SIZE, FORMAT_MAJOR, FORMAT_MINOR_V6,
};
use typra_core::record::RowValue;
use typra_core::schema::{FieldDef, FieldPath, IndexDef, IndexKind, Type};
use typra_core::superblock::SUPERBLOCK_SIZE;
use typra_core::{Database, ScalarValue};

fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/format")
}

fn field_path(parts: &[&str]) -> FieldPath {
    FieldPath(parts.iter().map(|s| Cow::Owned(s.to_string())).collect())
}

fn def(parts: &[&str], ty: Type) -> FieldDef {
    FieldDef {
        path: field_path(parts),
        ty,
        constraints: vec![],
    }
}

/// Build a file representative of Typra 1.0: minor 6, catalog v4, record v2 + v3, indexes, txn framing.
fn write_1_0_representative(path: &Path) {
    let mut db = Database::open(path).expect("open new db");

    // Collection A: flat schema → record payload v2 on insert.
    let flat_fields = vec![def(&["id"], Type::String), def(&["year"], Type::Int64)];
    let (cid_flat, _) = db
        .register_collection_with_indexes("books", flat_fields, vec![], "id")
        .expect("register books");

    let mut row = BTreeMap::new();
    row.insert("id".into(), RowValue::String("alpha".into()));
    row.insert("year".into(), RowValue::Int64(2020));
    db.insert(cid_flat, row).expect("insert book");

    // Collection B: multi-segment paths → record payload v3.
    let nested_fields = vec![
        def(&["id"], Type::String),
        def(&["profile", "timezone"], Type::String),
    ];
    let indexes = vec![IndexDef {
        name: "tz_idx".into(),
        path: field_path(&["profile", "timezone"]),
        kind: IndexKind::NonUnique,
    }];
    let (cid_nested, _) = db
        .register_collection_with_indexes("users", nested_fields, indexes, "id")
        .expect("register users");

    let mut user = BTreeMap::new();
    user.insert("id".into(), RowValue::String("u1".into()));
    user.insert(
        "profile".into(),
        RowValue::Object(BTreeMap::from([(
            "timezone".into(),
            RowValue::String("UTC".into()),
        )])),
    );
    db.insert(cid_nested, user).expect("insert user");

    db.checkpoint().expect("checkpoint");
    drop(db);

    let h = decode_header(&fs::read(path).expect("read file")[..32]).expect("header");
    assert_eq!(h.format_major, FORMAT_MAJOR);
    assert_eq!(h.format_minor, FORMAT_MINOR_V6);
}

#[cfg(coverage)]
fn assert_semantic_fixture_matches_fresh(fixture: &Path, fresh: &Path) {
    let fixture_db = Database::open(fixture).expect("open fixture");
    let fresh_db = Database::open(fresh).expect("open fresh encoder output");
    for name in ["books", "users"] {
        assert_eq!(
            fixture_db.collection_id_named(name).expect("fixture collection"),
            fresh_db.collection_id_named(name).expect("fresh collection"),
            "collection {name}"
        );
    }
    let books = fixture_db.collection_id_named("books").unwrap();
    let fresh_books = fresh_db.collection_id_named("books").unwrap();
    let alpha = fixture_db
        .get(books, &ScalarValue::String("alpha".into()))
        .unwrap()
        .expect("fixture alpha");
    let fresh_alpha = fresh_db
        .get(fresh_books, &ScalarValue::String("alpha".into()))
        .unwrap()
        .expect("fresh alpha");
    assert_eq!(alpha.get("year"), fresh_alpha.get("year"));

    let users = fixture_db.collection_id_named("users").unwrap();
    let fresh_users = fresh_db.collection_id_named("users").unwrap();
    let u1 = fixture_db
        .get(users, &ScalarValue::String("u1".into()))
        .unwrap()
        .expect("fixture u1");
    let fresh_u1 = fresh_db
        .get(fresh_users, &ScalarValue::String("u1".into()))
        .unwrap()
        .expect("fresh u1");
    assert_eq!(
        u1.get("profile").and_then(|v| match v {
            RowValue::Object(m) => m.get("timezone"),
            _ => None,
        }),
        fresh_u1.get("profile").and_then(|v| match v {
            RowValue::Object(m) => m.get("timezone"),
            _ => None,
        })
    );
}

#[test]
fn one_x_reads_1_0_representative_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("rep.typra");
    write_1_0_representative(&path);

    let db = Database::open(&path).expect("reopen 1.0-shaped file");
    let books = db.collection_id_named("books").unwrap();
    let got = db
        .get(books, &ScalarValue::String("alpha".into()))
        .unwrap()
        .expect("book row");
    assert_eq!(got.get("year"), Some(&RowValue::Int64(2020)));

    let users = db.collection_id_named("users").unwrap();
    let u = db
        .get(users, &ScalarValue::String("u1".into()))
        .unwrap()
        .expect("user row");
    let profile = u.get("profile").expect("profile object");
    let RowValue::Object(map) = profile else {
        panic!("expected nested profile object");
    };
    assert_eq!(map.get("timezone"), Some(&RowValue::String("UTC".into())));
}

#[test]
fn one_x_append_to_1_0_file_preserves_existing_rows() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("append.typra");
    write_1_0_representative(&path);

    {
        let mut db = Database::open(&path).unwrap();
        let books = db.collection_id_named("books").unwrap();
        let mut row = BTreeMap::new();
        row.insert("id".into(), RowValue::String("beta".into()));
        row.insert("year".into(), RowValue::Int64(2024));
        db.insert(books, row).unwrap();
    }

    let db = Database::open(&path).unwrap();
    let books = db.collection_id_named("books").unwrap();
    assert!(db
        .get(books, &ScalarValue::String("alpha".into()))
        .unwrap()
        .is_some());
    assert!(db
        .get(books, &ScalarValue::String("beta".into()))
        .unwrap()
        .is_some());
}

#[test]
fn committed_1_0_fixture_opens_and_matches_live_encoder() {
    let fixture_path = fixture_dir().join("typra_1_0_minor6.typra");
    assert!(
        fixture_path.is_file(),
        "missing {}; run scripts/generate-format-fixtures.sh from repo root",
        fixture_path.display()
    );

    let dir = tempfile::tempdir().unwrap();
    let fresh = dir.path().join("fresh.typra");
    write_1_0_representative(&fresh);
    let fresh_bytes = fs::read(&fresh).expect("read fresh encoder output");
    let committed = fs::read(&fixture_path).expect("read committed fixture");

    // Open a copy so this test never mutates the committed golden on disk.
    let copy = dir.path().join("fixture_copy.typra");
    fs::copy(&fixture_path, &copy).expect("copy fixture for open");

    // File header and append-only segment bytes must match exactly. Superblock slots may differ
    // only in generation/checksum (publish count), which is not part of the on-disk format spec.
    assert_eq!(
        &committed[..FILE_HEADER_SIZE],
        &fresh_bytes[..FILE_HEADER_SIZE],
        "file header drift — run scripts/generate-format-fixtures.sh if intentional"
    );
    #[cfg(not(coverage))]
    {
        let segment_start = FILE_HEADER_SIZE + 2 * SUPERBLOCK_SIZE;
        assert_eq!(
            committed.len(),
            fresh_bytes.len(),
            "fixture size drift — run scripts/generate-format-fixtures.sh if the format change was intentional"
        );
        assert_eq!(
            &committed[segment_start..],
            &fresh_bytes[segment_start..],
            "segment log drift — run scripts/generate-format-fixtures.sh if intentional"
        );
    }
    #[cfg(coverage)]
    assert_semantic_fixture_matches_fresh(&copy, &fresh);

    let committed_h = decode_header(&committed[..FILE_HEADER_SIZE]).expect("fixture header");
    let fresh_h = decode_header(&fresh_bytes[..FILE_HEADER_SIZE]).expect("fresh header");
    assert_eq!(committed_h.format_major, fresh_h.format_major);
    assert_eq!(committed_h.format_minor, fresh_h.format_minor);
    assert_eq!(committed_h, FileHeader::new_v0_8()); // representative uses current minor 6
    let db = Database::open(&copy).expect("open committed fixture");
    let books = db.collection_id_named("books").unwrap();
    let got = db
        .get(books, &ScalarValue::String("alpha".into()))
        .unwrap()
        .expect("fixture book");
    assert_eq!(got.get("year"), Some(&RowValue::Int64(2020)));
}

/// Regenerate golden fixtures after an intentional on-disk format change.
#[test]
#[ignore = "run via scripts/generate-format-fixtures.sh"]
fn export_format_fixtures() {
    fs::create_dir_all(fixture_dir()).expect("mkdir fixtures");
    let path = fixture_dir().join("typra_1_0_minor6.typra");
    let _ = fs::remove_file(&path);
    let lock = path.with_extension("typra.writer.lock");
    let _ = fs::remove_file(&lock);
    write_1_0_representative(&path);
}
