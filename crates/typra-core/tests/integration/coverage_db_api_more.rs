use std::borrow::Cow;
use std::collections::BTreeMap;

use typra_core::error::{FormatError, TransactionError};
use typra_core::record::RowValue;
use typra_core::schema::{FieldDef, FieldPath, IndexDef, IndexKind, Type};
use typra_core::schema::SchemaChange;
use typra_core::{CollectionId, Database, DbError, MigrationStep, ScalarValue};

fn field(name: &str, ty: Type) -> FieldDef {
    FieldDef {
        path: FieldPath(vec![Cow::Owned(name.to_string())]),
        ty,
        constraints: vec![],
    }
}

#[test]
fn commit_empty_transaction_updates_shadow_without_segments() {
    let mut db = Database::open_in_memory().unwrap();
    db.register_collection("books", vec![field("title", Type::String)], "title")
        .unwrap();

    db.begin_transaction().unwrap();
    db.commit_transaction().unwrap();
}

#[test]
fn transaction_rollback_discards_staged_writes() {
    let mut db = Database::open_in_memory().unwrap();
    db.register_collection("books", vec![field("title", Type::String)], "title")
        .unwrap();
    let cid = CollectionId(1);

    db.begin_transaction().unwrap();
    let mut r = BTreeMap::new();
    r.insert("title".into(), RowValue::String("a".into()));
    db.insert(cid, r).unwrap();

    // Read-your-writes inside the txn.
    assert!(db
        .get(cid, &ScalarValue::String("a".into()))
        .unwrap()
        .is_some());

    db.rollback_transaction();

    // Rolled back: no data.
    assert!(db
        .get(cid, &ScalarValue::String("a".into()))
        .unwrap()
        .is_none());
}

#[test]
fn collection_names_and_unknown_collection_id_named_error() {
    let mut db = Database::open_in_memory().unwrap();
    db.register_collection("books", vec![field("title", Type::String)], "title")
        .unwrap();

    assert_eq!(db.collection_names(), vec!["books".to_string()]);
    assert_eq!(db.collection_id_named("books").unwrap(), CollectionId(1));

    let e = db.collection_id_named("nope").unwrap_err();
    assert!(matches!(e, DbError::Schema(_)));
}

#[test]
fn unique_index_violation_returns_error_and_does_not_mutate() {
    let mut db = Database::open_in_memory().unwrap();
    let fields = vec![field("id", Type::Int64), field("email", Type::String)];
    let indexes = vec![IndexDef {
        name: "email_u".to_string(),
        path: FieldPath(vec![Cow::Borrowed("email")]),
        kind: IndexKind::Unique,
    }];
    let (cid, _) = db
        .register_collection_with_indexes("accounts", fields, indexes, "id")
        .unwrap();

    let mut r1 = BTreeMap::new();
    r1.insert("id".into(), RowValue::Int64(1));
    r1.insert("email".into(), RowValue::String("a@example.test".into()));
    db.insert(cid, r1).unwrap();

    let mut r2 = BTreeMap::new();
    r2.insert("id".into(), RowValue::Int64(2));
    r2.insert("email".into(), RowValue::String("a@example.test".into()));
    let e = db.insert(cid, r2).unwrap_err();
    assert!(matches!(e, DbError::Schema(_)));

    // Second insert should not be visible.
    assert!(db.get(cid, &ScalarValue::Int64(2)).unwrap().is_none());
}

#[test]
fn from_snapshot_bytes_rejects_empty_buffer() {
    // An empty snapshot is treated as a new empty DB; a *truncated* header must fail.
    let e = match Database::from_snapshot_bytes(vec![0u8; 1]) {
        Ok(_) => panic!("expected open from truncated snapshot to fail"),
        Err(e) => e,
    };
    assert!(matches!(e, DbError::Format(_)));
}

#[test]
fn plan_schema_version_reports_backfill_when_adding_required_field() {
    let mut db = Database::open_in_memory().unwrap();
    let fields = vec![field("title", Type::String)];
    let (cid, _) = db.register_collection("books", fields, "title").unwrap();
    let proposed = vec![
        field("title", Type::String),
        field("year", Type::Int64),
    ];
    let plan = db.plan_schema_version_with_indexes(cid, proposed, vec![]).unwrap();
    assert!(matches!(
        plan.change,
        SchemaChange::NeedsMigration { ref reason } if reason.contains("new required field")
    ));
    assert!(
        plan.steps.iter().any(|s| matches!(
            s,
            MigrationStep::BackfillTopLevelField { .. }
        )),
        "{plan:?}"
    );
}

#[test]
fn plan_schema_version_reports_rebuild_when_adding_unique_index() {
    let mut db = Database::open_in_memory().unwrap();
    let fields = vec![field("title", Type::String)];
    let (cid, _) = db.register_collection("books", fields, "title").unwrap();
    let new_indexes = vec![IndexDef {
        name: "title_u".to_string(),
        path: FieldPath(vec![Cow::Borrowed("title")]),
        kind: IndexKind::Unique,
    }];
    let plan = db
        .plan_schema_version_with_indexes(cid, vec![field("title", Type::String)], new_indexes)
        .unwrap();
    assert!(
        plan.steps.contains(&MigrationStep::RebuildIndexes),
        "{plan:?}"
    );
}

#[test]
fn backfill_top_level_field_runs_inside_transaction_over_snapshot_rows() {
    let mut db = Database::open_in_memory().unwrap();
    let fields = vec![
        field("title", Type::String),
        field("sku", Type::String),
    ];
    let (cid, _) = db.register_collection("books", fields, "title").unwrap();
    db.insert(
        cid,
        BTreeMap::from([
            ("title".into(), RowValue::String("a".into())),
            ("sku".into(), RowValue::String("s".into())),
        ]),
    )
    .unwrap();

    // Force a second top-level column via escape hatch; existing rows won't include `extra`.
    db.register_schema_version_with_indexes_force(
        cid,
        vec![
            field("title", Type::String),
            field("sku", Type::String),
            field("extra", Type::Int64),
        ],
        vec![],
    )
    .unwrap();

    db.backfill_top_level_field_with_value(cid, "extra", RowValue::Int64(7))
        .unwrap();
    let row = db
        .get(cid, &ScalarValue::String("a".into()))
        .unwrap()
        .unwrap();
    assert_eq!(row.get("extra"), Some(&RowValue::Int64(7)));
}

#[test]
fn rebuild_indexes_for_collection_applies_pending_index_segment() {
    let mut db = Database::open_in_memory().unwrap();
    let fields = vec![
        field("title", Type::String),
        field("tag", Type::String),
    ];
    let indexes = vec![IndexDef {
        name: "tag_idx".to_string(),
        path: FieldPath(vec![Cow::Borrowed("tag")]),
        kind: IndexKind::NonUnique,
    }];
    let (cid, _) = db
        .register_collection_with_indexes("books", fields, indexes, "title")
        .unwrap();
    db.insert(
        cid,
        BTreeMap::from([
            ("title".into(), RowValue::String("a".into())),
            ("tag".into(), RowValue::String("x".into())),
        ]),
    )
    .unwrap();

    db.rebuild_indexes_for_collection(cid).unwrap();
}

#[test]
fn insert_nested_multi_segment_field_paths_round_trips() {
    let mut db = Database::open_in_memory().unwrap();
    let fields = vec![
        field("id", Type::String),
        FieldDef {
            path: FieldPath(vec![Cow::Borrowed("meta"), Cow::Borrowed("tag")]),
            ty: Type::String,
            constraints: vec![],
        },
    ];
    let (cid, _) = db
        .register_collection_with_indexes("items", fields, vec![], "id")
        .unwrap();

    let mut row = BTreeMap::new();
    row.insert("id".into(), RowValue::String("k".into()));
    row.insert(
        "meta".into(),
        RowValue::Object(BTreeMap::from([(
            "tag".into(),
            RowValue::String("hello".into()),
        )])),
    );
    db.insert(cid, row).unwrap();

    let got = db
        .get(cid, &ScalarValue::String("k".into()))
        .unwrap()
        .unwrap();
    match got.get("meta") {
        Some(RowValue::Object(m)) => assert_eq!(
            m.get("tag"),
            Some(&RowValue::String("hello".into()))
        ),
        o => panic!("expected nested object, got {o:?}"),
    }
}

#[test]
fn replace_multi_segment_row_rewrites_payload() {
    let mut db = Database::open_in_memory().unwrap();
    let fields = vec![
        field("id", Type::String),
        FieldDef {
            path: FieldPath(vec![Cow::Borrowed("meta"), Cow::Borrowed("n")]),
            ty: Type::Int64,
            constraints: vec![],
        },
    ];
    let (cid, _) = db
        .register_collection_with_indexes("items", fields, vec![], "id")
        .unwrap();

    let mut r1 = BTreeMap::new();
    r1.insert("id".into(), RowValue::String("k".into()));
    r1.insert(
        "meta".into(),
        RowValue::Object(BTreeMap::from([("n".into(), RowValue::Int64(1))])),
    );
    db.insert(cid, r1).unwrap();

    let mut r2 = BTreeMap::new();
    r2.insert("id".into(), RowValue::String("k".into()));
    r2.insert(
        "meta".into(),
        RowValue::Object(BTreeMap::from([("n".into(), RowValue::Int64(2))])),
    );
    db.insert(cid, r2).unwrap();

    let got = db
        .get(cid, &ScalarValue::String("k".into()))
        .unwrap()
        .unwrap();
    match got.get("meta") {
        Some(RowValue::Object(m)) => assert_eq!(m.get("n"), Some(&RowValue::Int64(2))),
        o => panic!("expected nested object, got {o:?}"),
    }
}

#[test]
fn register_schema_version_force_inside_transaction_commits() {
    let mut db = Database::open_in_memory().unwrap();
    let (cid, _) = db
        .register_collection(
            "books",
            vec![field("title", Type::String)],
            "title",
        )
        .unwrap();

    db.begin_transaction().unwrap();
    db.register_schema_version_with_indexes_force(
        cid,
        vec![
            field("title", Type::String),
            field("extra", Type::Int64),
        ],
        vec![],
    )
    .unwrap();
    db.commit_transaction().unwrap();

    let col = db.catalog().get(cid).unwrap();
    assert!(col.fields.iter().any(|f| f.path.0.len() == 1 && f.path.0[0].as_ref() == "extra"));
}

#[test]
fn checkpoint_rejects_nested_transaction() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.typra");
    let mut db = Database::open(&path).unwrap();
    db.register_collection("books", vec![field("title", Type::String)], "title")
        .unwrap();

    db.begin_transaction().unwrap();
    let e = db.checkpoint().unwrap_err();
    assert!(matches!(
        e,
        DbError::Transaction(TransactionError::NestedTransaction)
    ));
    db.rollback_transaction();
}

#[test]
fn get_rejects_primary_key_type_mismatch() {
    let mut db = Database::open_in_memory().unwrap();
    let (cid, _) = db
        .register_collection("books", vec![field("title", Type::String)], "title")
        .unwrap();
    db.insert(
        cid,
        BTreeMap::from([("title".into(), RowValue::String("a".into()))]),
    )
    .unwrap();

    let e = db.get(cid, &ScalarValue::Int64(1)).unwrap_err();
    assert!(matches!(
        e,
        DbError::Format(FormatError::RecordPayloadTypeMismatch)
    ));
}

#[test]
fn backfill_skips_rows_that_already_have_target_field() {
    let mut db = Database::open_in_memory().unwrap();
    let fields = vec![field("title", Type::String), field("sku", Type::String)];
    let (cid, _) = db.register_collection("books", fields, "title").unwrap();
    db.insert(
        cid,
        BTreeMap::from([
            ("title".into(), RowValue::String("a".into())),
            ("sku".into(), RowValue::String("s".into())),
        ]),
    )
    .unwrap();
    db.insert(
        cid,
        BTreeMap::from([
            ("title".into(), RowValue::String("b".into())),
            ("sku".into(), RowValue::String("t".into())),
        ]),
    )
    .unwrap();

    db.register_schema_version_with_indexes_force(
        cid,
        vec![
            field("title", Type::String),
            field("sku", Type::String),
            field("extra", Type::Int64),
        ],
        vec![],
    )
    .unwrap();

    db.insert(
        cid,
        BTreeMap::from([
            ("title".into(), RowValue::String("a".into())),
            ("sku".into(), RowValue::String("s".into())),
            ("extra".into(), RowValue::Int64(1)),
        ]),
    )
    .unwrap();

    db.backfill_top_level_field_with_value(cid, "extra", RowValue::Int64(99))
        .unwrap();

    let ra = db
        .get(cid, &ScalarValue::String("a".into()))
        .unwrap()
        .unwrap();
    assert_eq!(ra.get("extra"), Some(&RowValue::Int64(1)));

    let rb = db
        .get(cid, &ScalarValue::String("b".into()))
        .unwrap()
        .unwrap();
    assert_eq!(rb.get("extra"), Some(&RowValue::Int64(99)));
}

#[test]
fn rebuild_indexes_no_rows_is_ok() {
    let mut db = Database::open_in_memory().unwrap();
    let fields = vec![field("title", Type::String), field("tag", Type::String)];
    let indexes = vec![IndexDef {
        name: "tag_idx".to_string(),
        path: FieldPath(vec![Cow::Borrowed("tag")]),
        kind: IndexKind::NonUnique,
    }];
    let (cid, _) = db
        .register_collection_with_indexes("books", fields, indexes, "title")
        .unwrap();

    db.rebuild_indexes_for_collection(cid).unwrap();
}
