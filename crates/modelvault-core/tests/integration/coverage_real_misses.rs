//! Integration tests targeting llvm-cov "real miss" lines (see `scripts/modelvault_core_coverage_real_misses.py`).

use std::borrow::Cow;
use std::collections::BTreeMap;

use modelvault_core::catalog::CollectionInfo;
use modelvault_core::checkpoint::{
    decode_checkpoint_payload, encode_checkpoint_payload_v0, CheckpointV0,
};
use modelvault_core::error::{DbError, SchemaError};
use modelvault_core::record::RowValue;
use modelvault_core::schema::{FieldDef, FieldPath, IndexDef, IndexKind, Type};
use modelvault_core::validation::validate_multiseg_row;
use modelvault_core::{validate_model_fields_against_catalog, CollectionId, Database, ScalarValue};

fn field(name: &str, ty: Type) -> FieldDef {
    FieldDef {
        path: FieldPath(vec![Cow::Owned(name.to_string())]),
        ty,
        constraints: vec![],
    }
}

fn path(segs: &[&str]) -> FieldPath {
    FieldPath(segs.iter().map(|s| Cow::Owned((*s).to_string())).collect())
}

fn sample_collection() -> CollectionInfo {
    CollectionInfo {
        id: CollectionId(1),
        name: "books".into(),
        current_version: modelvault_core::schema::SchemaVersion(1),
        fields: vec![field("id", Type::Int64), field("title", Type::String)],
        indexes: vec![IndexDef {
            name: "title_idx".into(),
            path: path(&["title"]),
            kind: IndexKind::NonUnique,
        }],
        primary_field: Some("id".into()),
    }
}

#[test]
fn backfill_rejects_unknown_field_path() {
    let mut db = Database::open_in_memory().unwrap();
    let (cid, _) = db
        .register_collection("books", vec![field("id", Type::Int64)], "id")
        .unwrap();
    let err = db
        .backfill_field_at_path_with_value(cid, &path(&["missing"]), RowValue::Int64(1))
        .unwrap_err();
    assert!(matches!(
        err,
        DbError::Schema(SchemaError::RowUnknownField { name }) if name == "missing"
    ));
}

#[test]
fn validate_model_fields_full_schema_index_mismatch() {
    let col = sample_collection();
    let model_fields = col.fields.clone();
    let bad_indexes = vec![IndexDef {
        name: "other".into(),
        path: path(&["title"]),
        kind: IndexKind::NonUnique,
    }];
    let err =
        validate_model_fields_against_catalog(&col, "id", &model_fields, &bad_indexes).unwrap_err();
    assert!(matches!(
        err,
        DbError::Schema(SchemaError::IncompatibleSchemaChange { message }) if message.contains("index definitions")
    ));
}

#[test]
fn validate_model_fields_subset_unknown_index() {
    let col = sample_collection();
    let subset = vec![field("id", Type::Int64)];
    let indexes = vec![IndexDef {
        name: "nope".into(),
        path: path(&["title"]),
        kind: IndexKind::NonUnique,
    }];
    let err = validate_model_fields_against_catalog(&col, "id", &subset, &indexes).unwrap_err();
    assert!(matches!(
        err,
        DbError::Schema(SchemaError::IncompatibleSchemaChange { message }) if message.contains("unknown index")
    ));
}

#[test]
fn validate_model_fields_subset_index_kind_mismatch() {
    let col = sample_collection();
    let subset = vec![field("id", Type::Int64)];
    let indexes = vec![IndexDef {
        name: "title_idx".into(),
        path: path(&["title"]),
        kind: IndexKind::Unique,
    }];
    let err = validate_model_fields_against_catalog(&col, "id", &subset, &indexes).unwrap_err();
    assert!(matches!(
        err,
        DbError::Schema(SchemaError::IncompatibleSchemaChange { message }) if message.contains("does not match catalog")
    ));
}

#[test]
fn validate_multiseg_row_rejects_explicit_null_on_required_nested_field() {
    let fields = vec![
        field("id", Type::Int64),
        FieldDef {
            path: path(&["profile", "name"]),
            ty: Type::String,
            constraints: vec![],
        },
    ];
    let row = BTreeMap::from([
        ("id".into(), RowValue::Int64(1)),
        (
            "profile".into(),
            RowValue::Object(BTreeMap::from([("name".into(), RowValue::None)])),
        ),
    ]);
    let err = validate_multiseg_row(&fields, "id", &row).unwrap_err();
    assert!(matches!(
        err,
        DbError::Validation(v) if v.path == ["profile", "name"] && v.message == "unexpected null for required field"
    ));
}

#[test]
fn checkpoint_payload_roundtrip_exercises_tracing_encode_decode_paths() {
    let cp = CheckpointV0 {
        replay_from_offset: 42,
        catalog_records: vec![],
        record_payloads: vec![],
        index_entries: vec![],
    };
    let bytes = encode_checkpoint_payload_v0(&cp);
    let decoded = decode_checkpoint_payload(&bytes).unwrap();
    assert_eq!(decoded.replay_from_offset, 42);
}

#[test]
fn checkpoint_from_state_on_multiseg_db_hits_tracing_ok_path() {
    let mut db = Database::open_in_memory().unwrap();
    let fields = vec![
        field("id", Type::Int64),
        FieldDef {
            path: path(&["meta", "tag"]),
            ty: Type::String,
            constraints: vec![],
        },
    ];
    let (cid, _) = db.register_collection("items", fields, "id").unwrap();
    let row = BTreeMap::from([
        ("id".into(), RowValue::Int64(1)),
        (
            "meta".into(),
            RowValue::Object(BTreeMap::from([(
                "tag".into(),
                RowValue::String("a".into()),
            )])),
        ),
    ]);
    db.insert(cid, row).unwrap();
    db.checkpoint().unwrap();
}

#[test]
fn compact_to_hits_tracing_span_on_disk_db() {
    let dir = tempfile::tempdir().unwrap();
    let src = dir.path().join("src.modelvault");
    let dst = dir.path().join("dst.modelvault");
    let mut db = Database::open(&src).unwrap();
    let (cid, _) = db
        .register_collection("t", vec![field("id", Type::Int64)], "id")
        .unwrap();
    db.insert(cid, BTreeMap::from([("id".into(), RowValue::Int64(1))]))
        .unwrap();
    db.compact_to(&dst).unwrap();
    let db2 = Database::open(&dst).unwrap();
    assert!(db2.get(cid, &ScalarValue::Int64(1)).unwrap().is_some());
}
