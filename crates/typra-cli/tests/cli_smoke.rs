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

#[test]
fn migrate_plan_then_apply_force_backfill_works() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.typra");
    let schema_v1 = dir.path().join("schema_v1.json");
    let schema_v2 = dir.path().join("schema_v2.json");

    // Create a new DB using the library (so schema segments exist) and insert one row.
    {
        let mut db = typra_core::Database::open(&path).unwrap();
        let _cid = db
            .register_collection(
                "books",
                vec![typra_core::FieldDef {
                    path: typra_core::schema::FieldPath(vec![std::borrow::Cow::Borrowed("id")]),
                    ty: typra_core::Type::Int64,
                    constraints: vec![],
                }],
                "id",
            )
            .unwrap();
        let cid = db.collection_id_named("books").unwrap();
        let mut row = std::collections::BTreeMap::new();
        row.insert("id".to_string(), typra_core::RowValue::Int64(1));
        db.insert(cid, row).unwrap();
        db.checkpoint().unwrap();
    }

    std::fs::write(&schema_v1, r#"[{"path":["id"],"type":"int64"}]"#).unwrap();
    std::fs::write(
        &schema_v2,
        r#"[{"path":["id"],"type":"int64"},{"path":["genre"],"type":"string"}]"#,
    )
    .unwrap();

    Command::cargo_bin("typra")
        .unwrap()
        .args([
            "migrate",
            "plan",
            path.to_str().unwrap(),
            "--collection",
            "books",
            "--schema-json",
            schema_v2.to_str().unwrap(),
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"steps\""));

    // Apply a backfill for the new field and force-register the schema version.
    Command::cargo_bin("typra")
        .unwrap()
        .args([
            "migrate",
            "apply",
            path.to_str().unwrap(),
            "--collection",
            "books",
            "--schema-json",
            schema_v2.to_str().unwrap(),
            "--backfill-field",
            "genre",
            "--backfill-value",
            "\"unknown\"",
            "--force",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("ok: schema_version="));

    // Validate row contains the backfilled field.
    let db = typra_core::Database::open(&path).unwrap();
    let cid = db.collection_id_named("books").unwrap();
    let got = db.get(cid, &typra_core::ScalarValue::Int64(1)).unwrap();
    let obj = got.unwrap();
    assert_eq!(
        obj.get("genre"),
        Some(&typra_core::RowValue::String("unknown".to_string()))
    );
}
