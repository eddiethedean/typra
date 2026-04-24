use assert_cmd::Command;
use predicates::prelude::*;

#[test]
fn inspect_and_verify_and_dump_catalog_work_on_new_db() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.typra");

    // Create a new DB using the library (so schema segments exist).
    {
        let mut db = typra_core::Database::open(&path).unwrap();
        let _ = db
            .register_collection(
                "books",
                vec![typra_core::FieldDef {
                    path: typra_core::schema::FieldPath(vec![std::borrow::Cow::Borrowed("title")]),
                    ty: typra_core::Type::String,
                    constraints: vec![],
                }],
                "title",
            )
            .unwrap();
        db.checkpoint().unwrap();
    }

    Command::cargo_bin("typra")
        .unwrap()
        .args(["inspect", path.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("format: 0."));

    Command::cargo_bin("typra")
        .unwrap()
        .args(["verify", path.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("schema_segments_ok=true"));

    Command::cargo_bin("typra")
        .unwrap()
        .args(["dump-catalog", path.to_str().unwrap(), "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"collections\""));
}
