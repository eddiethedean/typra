use std::borrow::Cow;

use typra_core::{Constraint, Database, FieldDef, IndexDef, IndexKind, SchemaError, Type};
use typra_core::schema::FieldPath;

fn def(path: &[&'static str], ty: Type, constraints: Vec<Constraint>) -> FieldDef {
    FieldDef {
        path: FieldPath(path.iter().map(|s| Cow::Borrowed(*s)).collect()),
        ty,
        constraints,
    }
}

#[test]
fn register_collection_rejects_empty_field_path() {
    let mut db = Database::open_in_memory().unwrap();

    let fields = vec![
        def(&["id"], Type::Int64, vec![]),
        FieldDef {
            path: FieldPath(Vec::new()),
            ty: Type::Int64,
            constraints: Vec::new(),
        },
    ];

    let err = db.register_collection("t", fields, "id").unwrap_err();
    assert!(matches!(err, typra_core::DbError::Schema(SchemaError::InvalidFieldPath)));
}

#[test]
fn register_collection_rejects_empty_path_segment() {
    let mut db = Database::open_in_memory().unwrap();
    let fields = vec![def(&["id"], Type::Int64, vec![]), def(&[""], Type::Int64, vec![])];
    let err = db.register_collection("t", fields, "id").unwrap_err();
    assert!(matches!(err, typra_core::DbError::Schema(SchemaError::InvalidFieldPath)));
}

#[test]
fn register_collection_rejects_duplicate_field_paths() {
    let mut db = Database::open_in_memory().unwrap();
    let fields = vec![
        def(&["id"], Type::Int64, vec![]),
        def(&["x"], Type::Int64, vec![]),
        def(&["x"], Type::Int64, vec![]),
    ];
    let err = db.register_collection("t", fields, "id").unwrap_err();
    assert!(matches!(err, typra_core::DbError::Schema(SchemaError::InvalidFieldPath)));
}

#[test]
fn register_collection_rejects_parent_child_conflict_field_paths() {
    let mut db = Database::open_in_memory().unwrap();
    let fields = vec![
        def(&["id"], Type::Int64, vec![]),
        def(&["a"], Type::Int64, vec![]),
        def(&["a", "b"], Type::Int64, vec![]),
    ];
    let err = db.register_collection("t", fields, "id").unwrap_err();
    assert!(matches!(err, typra_core::DbError::Schema(SchemaError::InvalidFieldPath)));
}

#[test]
fn register_schema_version_rejects_constraint_change() {
    let mut db = Database::open_in_memory().unwrap();
    let (cid, _) = db
        .register_collection(
            "t",
            vec![def(&["id"], Type::Int64, vec![Constraint::NonEmpty])],
            "id",
        )
        .unwrap();

    let err = db
        .register_schema_version(cid, vec![def(&["id"], Type::Int64, vec![])])
        .unwrap_err();
    assert!(matches!(
        err,
        typra_core::DbError::Schema(SchemaError::IncompatibleSchemaChange { .. })
    ));
}

#[test]
fn register_schema_version_rejects_index_kind_and_path_changes() {
    let mut db = Database::open_in_memory().unwrap();
    let (cid, _) = db
        .register_collection_with_indexes(
            "t",
            vec![def(&["id"], Type::Int64, vec![]), def(&["x"], Type::Int64, vec![])],
            vec![IndexDef {
                name: "i".to_string(),
                path: FieldPath(vec![Cow::Borrowed("x")]),
                kind: IndexKind::NonUnique,
            }],
            "id",
        )
        .unwrap();

    // Kind change should be breaking.
    let err = db
        .register_schema_version_with_indexes(
            cid,
            vec![def(&["id"], Type::Int64, vec![]), def(&["x"], Type::Int64, vec![])],
            vec![IndexDef {
                name: "i".to_string(),
                path: FieldPath(vec![Cow::Borrowed("x")]),
                kind: IndexKind::Unique,
            }],
        )
        .unwrap_err();
    assert!(matches!(
        err,
        typra_core::DbError::Schema(SchemaError::IncompatibleSchemaChange { .. })
    ));

    // Path change should be breaking.
    let err = db
        .register_schema_version_with_indexes(
            cid,
            vec![def(&["id"], Type::Int64, vec![]), def(&["x"], Type::Int64, vec![])],
            vec![IndexDef {
                name: "i".to_string(),
                path: FieldPath(vec![Cow::Borrowed("id")]),
                kind: IndexKind::NonUnique,
            }],
        )
        .unwrap_err();
    assert!(matches!(
        err,
        typra_core::DbError::Schema(SchemaError::IncompatibleSchemaChange { .. })
    ));
}

#[test]
fn register_schema_version_unique_index_add_requires_migration_but_non_unique_add_is_safe() {
    let mut db = Database::open_in_memory().unwrap();
    let (cid, _) = db
        .register_collection("t", vec![def(&["id"], Type::Int64, vec![]), def(&["x"], Type::Int64, vec![])], "id")
        .unwrap();

    // Adding non-unique index is safe.
    db.register_schema_version_with_indexes(
        cid,
        vec![def(&["id"], Type::Int64, vec![]), def(&["x"], Type::Int64, vec![])],
        vec![IndexDef {
            name: "i".to_string(),
            path: FieldPath(vec![Cow::Borrowed("x")]),
            kind: IndexKind::NonUnique,
        }],
    )
    .unwrap();

    // Adding unique index should require migration.
    let err = db
        .register_schema_version_with_indexes(
            cid,
            vec![def(&["id"], Type::Int64, vec![]), def(&["x"], Type::Int64, vec![])],
            vec![
                IndexDef {
                    name: "i".to_string(),
                    path: FieldPath(vec![Cow::Borrowed("x")]),
                    kind: IndexKind::NonUnique,
                },
                IndexDef {
                    name: "u".to_string(),
                    path: FieldPath(vec![Cow::Borrowed("x")]),
                    kind: IndexKind::Unique,
                },
            ],
        )
        .unwrap_err();
    assert!(matches!(
        err,
        typra_core::DbError::Schema(SchemaError::MigrationRequired { .. })
    ));
}

