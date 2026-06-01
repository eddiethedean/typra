use std::borrow::Cow;
use std::collections::BTreeMap;

use modelvault_core::query::{Predicate, Query};
use modelvault_core::schema::{Constraint, FieldDef, FieldPath, IndexDef, IndexKind, Type};
use modelvault_core::{Database, DbError, RowValue, ScalarValue};
use tempfile::tempdir;

fn path(parts: &[&str]) -> FieldPath {
    FieldPath(parts.iter().map(|s| Cow::Owned(s.to_string())).collect())
}

fn def(parts: &[&str], ty: Type) -> FieldDef {
    FieldDef {
        path: path(parts),
        ty,
        constraints: vec![],
    }
}

#[test]
fn multi_segment_schema_paths_roundtrip_insert_get_reopen_and_index_query() {
    let dir = tempdir().unwrap();
    let p = dir.path().join("m.modelvault");

    {
        let mut db = Database::open(&p).unwrap();
        let fields = vec![
            def(&["id"], Type::String),
            def(&["profile", "timezone"], Type::String),
            def(&["profile", "age"], Type::Int64),
        ];
        let indexes = vec![IndexDef {
            name: "tz_idx".to_string(),
            path: path(&["profile", "timezone"]),
            kind: IndexKind::NonUnique,
        }];
        let (cid, _) = db
            .register_collection_with_indexes("users", fields, indexes, "id")
            .unwrap();

        let mut row = BTreeMap::new();
        row.insert("id".into(), RowValue::String("u1".into()));
        row.insert(
            "profile".into(),
            RowValue::Object(BTreeMap::from([
                ("timezone".into(), RowValue::String("UTC".into())),
                ("age".into(), RowValue::Int64(30)),
            ])),
        );
        db.insert(cid, row).unwrap();

        // Indexed query on nested path (planner uses Eq -> IndexLookup when index exists).
        let q = Query {
            collection: cid,
            predicate: Some(Predicate::Eq {
                path: path(&["profile", "timezone"]),
                value: ScalarValue::String("UTC".into()),
            }),
            limit: None,
            order_by: None,
        };
        let rows = db.query(&q).unwrap();
        assert_eq!(rows.len(), 1);
    }

    // Reopen and `get` should return nested structure.
    let db = Database::open(&p).unwrap();
    let cid = db.collection_id_named("users").unwrap();
    let got = db
        .get(cid, &ScalarValue::String("u1".into()))
        .unwrap()
        .unwrap();
    let RowValue::Object(profile) = got.get("profile").unwrap() else {
        panic!("expected profile object");
    };
    assert_eq!(
        profile.get("timezone"),
        Some(&RowValue::String("UTC".into()))
    );
    assert_eq!(profile.get("age"), Some(&RowValue::Int64(30)));
}

#[test]
fn multi_segment_schema_rejects_wrong_type_and_constraint_violations() {
    let mut db = Database::open_in_memory().unwrap();
    let fields = vec![
        def(&["id"], Type::String),
        FieldDef {
            path: path(&["profile", "timezone"]),
            ty: Type::String,
            constraints: vec![Constraint::MinLength(3)],
        },
        def(&["profile", "age"], Type::Int64),
    ];
    let (cid, _) = db.register_collection("users", fields, "id").unwrap();

    let mut bad_type = BTreeMap::new();
    bad_type.insert("id".into(), RowValue::String("u1".into()));
    bad_type.insert(
        "profile".into(),
        RowValue::Object(BTreeMap::from([
            ("timezone".into(), RowValue::Int64(99)),
            ("age".into(), RowValue::Int64(30)),
        ])),
    );
    assert!(matches!(
        db.insert(cid, bad_type),
        Err(DbError::Validation(_))
    ));

    let mut bad_constraint = BTreeMap::new();
    bad_constraint.insert("id".into(), RowValue::String("u2".into()));
    bad_constraint.insert(
        "profile".into(),
        RowValue::Object(BTreeMap::from([
            ("timezone".into(), RowValue::String("UT".into())),
            ("age".into(), RowValue::Int64(30)),
        ])),
    );
    assert!(matches!(
        db.insert(cid, bad_constraint),
        Err(DbError::Validation(_))
    ));
}

#[test]
fn multi_segment_schema_checkpoint_preserves_nested_fields() {
    let dir = tempdir().unwrap();
    let p = dir.path().join("checkpoint.modelvault");

    {
        let mut db = Database::open(&p).unwrap();
        let fields = vec![
            def(&["id"], Type::String),
            def(&["profile", "timezone"], Type::String),
            def(&["profile", "age"], Type::Int64),
        ];
        let (cid, _) = db.register_collection("users", fields, "id").unwrap();
        let mut row = BTreeMap::new();
        row.insert("id".into(), RowValue::String("u1".into()));
        row.insert(
            "profile".into(),
            RowValue::Object(BTreeMap::from([
                ("timezone".into(), RowValue::String("UTC".into())),
                ("age".into(), RowValue::Int64(30)),
            ])),
        );
        db.insert(cid, row).unwrap();
        db.checkpoint().unwrap();
    }

    let db = Database::open(&p).unwrap();
    let cid = db.collection_id_named("users").unwrap();
    let got = db
        .get(cid, &ScalarValue::String("u1".into()))
        .unwrap()
        .unwrap();
    let RowValue::Object(profile) = got.get("profile").unwrap() else {
        panic!("expected profile object after checkpoint");
    };
    assert_eq!(
        profile.get("timezone"),
        Some(&RowValue::String("UTC".into()))
    );
    assert_eq!(profile.get("age"), Some(&RowValue::Int64(30)));
}

#[test]
fn nested_required_field_migration_plan_and_backfill_at_path() {
    use modelvault_core::schema::SchemaChange;
    use modelvault_core::MigrationStep;

    let mut db = Database::open_in_memory().unwrap();
    let v1 = vec![
        def(&["id"], Type::String),
        def(&["profile", "timezone"], Type::String),
    ];
    let (cid, _) = db.register_collection("users", v1, "id").unwrap();

    let mut row = BTreeMap::new();
    row.insert("id".into(), RowValue::String("u1".into()));
    row.insert(
        "profile".into(),
        RowValue::Object(BTreeMap::from([(
            "timezone".into(),
            RowValue::String("UTC".into()),
        )])),
    );
    db.insert(cid, row).unwrap();

    let v2 = vec![
        def(&["id"], Type::String),
        def(&["profile", "timezone"], Type::String),
        def(&["profile", "age"], Type::Int64),
    ];
    let plan = db
        .plan_schema_version_with_indexes(cid, v2.clone(), vec![])
        .unwrap();
    assert!(matches!(
        plan.change,
        SchemaChange::NeedsMigration {
            backfill_top_level_field: None,
            backfill_field_path: Some(_),
            ..
        }
    ));
    assert!(plan.steps.iter().any(|s| matches!(
        s,
        MigrationStep::BackfillFieldAtPath { path } if path.0.len() == 2
    )));

    db.register_schema_version_with_indexes_force(cid, v2, vec![])
        .unwrap();
    db.backfill_field_at_path_with_value(cid, &path(&["profile", "age"]), RowValue::Int64(42))
        .unwrap();

    let got = db
        .get(cid, &ScalarValue::String("u1".into()))
        .unwrap()
        .unwrap();
    let RowValue::Object(profile) = got.get("profile").unwrap() else {
        panic!("expected profile object");
    };
    assert_eq!(profile.get("age"), Some(&RowValue::Int64(42)));
}
