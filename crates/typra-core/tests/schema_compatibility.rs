use std::borrow::Cow;

use typra_core::error::SchemaError;
use typra_core::schema::{FieldDef, FieldPath, IndexDef, IndexKind, Type};
use typra_core::{CollectionId, Database, DbError};

fn field(name: &str, ty: Type) -> FieldDef {
    FieldDef {
        path: FieldPath(vec![Cow::Owned(name.to_string())]),
        ty,
        constraints: vec![],
    }
}

#[test]
fn schema_update_allows_add_optional_field() {
    let mut db = Database::open_in_memory().unwrap();
    db.register_collection("t", vec![field("id", Type::Int64)], "id")
        .unwrap();
    let cid = CollectionId(1);
    let next = db
        .register_schema_version(
            cid,
            vec![
                field("id", Type::Int64),
                field("note", Type::Optional(Box::new(Type::String))),
            ],
        )
        .unwrap();
    assert_eq!(next.0, 2);
}

#[test]
fn schema_update_rejects_add_required_field_without_migration() {
    let mut db = Database::open_in_memory().unwrap();
    db.register_collection("t", vec![field("id", Type::Int64)], "id")
        .unwrap();
    let cid = CollectionId(1);
    let err =
        db.register_schema_version(cid, vec![field("id", Type::Int64), field("x", Type::Int64)]);
    assert!(matches!(
        err,
        Err(DbError::Schema(SchemaError::MigrationRequired { .. }))
    ));
}

#[test]
fn schema_update_rejects_type_change_and_field_removal() {
    let mut db = Database::open_in_memory().unwrap();
    db.register_collection(
        "t",
        vec![field("id", Type::Int64), field("x", Type::Int64)],
        "id",
    )
    .unwrap();
    let cid = CollectionId(1);

    let err = db.register_schema_version(cid, vec![field("id", Type::Int64)]);
    assert!(matches!(
        err,
        Err(DbError::Schema(
            SchemaError::IncompatibleSchemaChange { .. }
        ))
    ));

    let err2 = db.register_schema_version(
        cid,
        vec![field("id", Type::Int64), field("x", Type::String)],
    );
    assert!(matches!(
        err2,
        Err(DbError::Schema(
            SchemaError::IncompatibleSchemaChange { .. }
        ))
    ));
}

#[test]
fn schema_update_allows_add_enum_variant_but_rejects_removal() {
    let mut db = Database::open_in_memory().unwrap();
    db.register_collection(
        "t",
        vec![
            field("id", Type::Int64),
            field("status", Type::Enum(vec!["open".into()])),
        ],
        "id",
    )
    .unwrap();
    let cid = CollectionId(1);

    db.register_schema_version(
        cid,
        vec![
            field("id", Type::Int64),
            field("status", Type::Enum(vec!["open".into(), "closed".into()])),
        ],
    )
    .unwrap();

    let err = db.register_schema_version(
        cid,
        vec![
            field("id", Type::Int64),
            field("status", Type::Enum(vec!["open".into()])),
        ],
    );
    assert!(matches!(
        err,
        Err(DbError::Schema(
            SchemaError::IncompatibleSchemaChange { .. }
        ))
    ));
}

#[test]
fn schema_update_index_additions_are_classified() {
    let mut db = Database::open_in_memory().unwrap();
    db.register_collection(
        "t",
        vec![field("id", Type::Int64), field("tag", Type::String)],
        "id",
    )
    .unwrap();
    let cid = CollectionId(1);

    // Non-unique index add is allowed.
    db.register_schema_version_with_indexes(
        cid,
        vec![field("id", Type::Int64), field("tag", Type::String)],
        vec![IndexDef {
            name: "tag_idx".to_string(),
            path: FieldPath(vec![Cow::Owned("tag".to_string())]),
            kind: IndexKind::NonUnique,
        }],
    )
    .unwrap();

    // Unique index add requires migration.
    let err = db.register_schema_version_with_indexes(
        cid,
        vec![field("id", Type::Int64), field("tag", Type::String)],
        vec![IndexDef {
            name: "tag_u".to_string(),
            path: FieldPath(vec![Cow::Owned("tag".to_string())]),
            kind: IndexKind::Unique,
        }],
    );
    assert!(matches!(
        err,
        Err(DbError::Schema(SchemaError::MigrationRequired { .. }))
    ));
}
