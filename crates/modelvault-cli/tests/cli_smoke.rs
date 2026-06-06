use assert_cmd::Command;
use predicates::prelude::*;

#[test]
fn inspect_and_verify_and_dump_catalog_work_on_new_db() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.modelvault");

    // Create a new DB using the library (so schema segments exist).
    {
        let mut db = modelvault_core::Database::open(&path).unwrap();
        let _ = db
            .register_collection(
                "books",
                vec![modelvault_core::FieldDef {
                    path: modelvault_core::schema::FieldPath(vec![std::borrow::Cow::Borrowed(
                        "title",
                    )]),
                    ty: modelvault_core::Type::String,
                    constraints: vec![],
                }],
                "title",
            )
            .unwrap();
        db.checkpoint().unwrap();
    }

    Command::cargo_bin("modelvault")
        .unwrap()
        .args(["inspect", path.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("format: 0."));

    Command::cargo_bin("modelvault")
        .unwrap()
        .args(["verify", path.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("schema_segments_ok=true"));

    Command::cargo_bin("modelvault")
        .unwrap()
        .args(["dump-catalog", path.to_str().unwrap(), "--json"])
        .assert()
        .success()
        .stdout(predicate::str::contains("\"collections\""));
}

#[test]
fn migrate_plan_then_apply_force_backfill_works() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.modelvault");
    let schema_v1 = dir.path().join("schema_v1.json");
    let schema_v2 = dir.path().join("schema_v2.json");

    // Create a new DB using the library (so schema segments exist) and insert one row.
    {
        let mut db = modelvault_core::Database::open(&path).unwrap();
        let _cid = db
            .register_collection(
                "books",
                vec![modelvault_core::FieldDef {
                    path: modelvault_core::schema::FieldPath(vec![std::borrow::Cow::Borrowed(
                        "id",
                    )]),
                    ty: modelvault_core::Type::Int64,
                    constraints: vec![],
                }],
                "id",
            )
            .unwrap();
        let cid = db.collection_id_named("books").unwrap();
        let mut row = std::collections::BTreeMap::new();
        row.insert("id".to_string(), modelvault_core::RowValue::Int64(1));
        db.insert(cid, row).unwrap();
        db.checkpoint().unwrap();
    }

    std::fs::write(&schema_v1, r#"[{"path":["id"],"type":"int64"}]"#).unwrap();
    std::fs::write(
        &schema_v2,
        r#"[{"path":["id"],"type":"int64"},{"path":["genre"],"type":"string"}]"#,
    )
    .unwrap();

    Command::cargo_bin("modelvault")
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
    Command::cargo_bin("modelvault")
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
    let db = modelvault_core::Database::open(&path).unwrap();
    let cid = db.collection_id_named("books").unwrap();
    let got = db
        .get(cid, &modelvault_core::ScalarValue::Int64(1))
        .unwrap();
    let obj = got.unwrap();
    assert_eq!(
        obj.get("genre"),
        Some(&modelvault_core::RowValue::String("unknown".to_string()))
    );
}

fn seed_db_with_row(path: &std::path::Path) -> modelvault_core::CollectionId {
    let mut db = modelvault_core::Database::open(path).unwrap();
    let (cid, _) = db
        .register_collection(
            "books",
            vec![
                modelvault_core::FieldDef {
                    path: modelvault_core::schema::FieldPath(vec![std::borrow::Cow::Borrowed(
                        "id",
                    )]),
                    ty: modelvault_core::Type::Int64,
                    constraints: vec![],
                },
                modelvault_core::FieldDef {
                    path: modelvault_core::schema::FieldPath(vec![std::borrow::Cow::Borrowed(
                        "title",
                    )]),
                    ty: modelvault_core::Type::String,
                    constraints: vec![],
                },
            ],
            "id",
        )
        .unwrap();
    let mut row = std::collections::BTreeMap::new();
    row.insert("id".to_string(), modelvault_core::RowValue::Int64(1));
    row.insert(
        "title".to_string(),
        modelvault_core::RowValue::String("A".to_string()),
    );
    db.insert(cid, row).unwrap();
    cid
}

#[test]
fn checkpoint_command_writes_durable_state() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.modelvault");
    seed_db_with_row(&path);

    Command::cargo_bin("modelvault")
        .unwrap()
        .args(["checkpoint", path.to_str().unwrap()])
        .assert()
        .success()
        .stdout(predicate::str::contains("ok: checkpoint written"));

    let db = modelvault_core::Database::open(&path).unwrap();
    let cid = db.collection_id_named("books").unwrap();
    let got = db
        .get(cid, &modelvault_core::ScalarValue::Int64(1))
        .unwrap()
        .expect("row survives checkpoint");
    assert_eq!(
        got.get("title"),
        Some(&modelvault_core::RowValue::String("A".to_string()))
    );
}

#[test]
fn compact_to_and_backup_with_verify_work() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.modelvault");
    let compact_dst = dir.path().join("compact.modelvault");
    let backup_dst = dir.path().join("backup.modelvault");
    seed_db_with_row(&path);

    Command::cargo_bin("modelvault")
        .unwrap()
        .args([
            "compact",
            path.to_str().unwrap(),
            "--to",
            compact_dst.to_str().unwrap(),
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("ok: compacted_to"));

    assert!(compact_dst.exists());
    let db = modelvault_core::Database::open(&compact_dst).unwrap();
    let cid = db.collection_id_named("books").unwrap();
    assert!(db
        .get(cid, &modelvault_core::ScalarValue::Int64(1))
        .unwrap()
        .is_some());

    Command::cargo_bin("modelvault")
        .unwrap()
        .args([
            "backup",
            path.to_str().unwrap(),
            "--to",
            backup_dst.to_str().unwrap(),
            "--verify",
        ])
        .assert()
        .success()
        .stdout(predicate::str::contains("ok: backup_written"));

    assert!(backup_dst.exists());
    Command::cargo_bin("modelvault")
        .unwrap()
        .args(["verify", backup_dst.to_str().unwrap()])
        .assert()
        .success();
}

#[test]
fn compact_in_place_rewrites_file() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.modelvault");
    seed_db_with_row(&path);

    Command::cargo_bin("modelvault")
        .unwrap()
        .args(["compact", path.to_str().unwrap(), "--in-place"])
        .assert()
        .success()
        .stdout(predicate::str::contains("ok: compacted_in_place"));

    let db = modelvault_core::Database::open(&path).unwrap();
    let cid = db.collection_id_named("books").unwrap();
    assert!(db
        .get(cid, &modelvault_core::ScalarValue::Int64(1))
        .unwrap()
        .is_some());
}
