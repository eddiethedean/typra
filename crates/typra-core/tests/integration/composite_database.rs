//! Integration tests: nested `RowValue` rows, enums, optional fields, and constraint failures.

use std::borrow::Cow;
use std::collections::BTreeMap;

use typra_core::error::DbError;
use typra_core::record::{RowValue, ScalarValue};
use typra_core::schema::{Constraint, FieldDef, FieldPath, Type};
use typra_core::CollectionId;
use typra_core::Database;

fn seg(s: &'static str) -> FieldPath {
    FieldPath::new([Cow::Borrowed(s)]).unwrap()
}

fn composite_fields() -> Vec<FieldDef> {
    vec![
        FieldDef {
            path: seg("id"),
            ty: Type::String,
            constraints: vec![],
        },
        FieldDef {
            path: seg("meta"),
            ty: Type::Object(vec![FieldDef {
                path: seg("label"),
                ty: Type::String,
                constraints: vec![Constraint::MaxLength(100)],
            }]),
            constraints: vec![],
        },
        FieldDef {
            path: seg("tags"),
            ty: Type::List(Box::new(Type::String)),
            constraints: vec![Constraint::NonEmpty],
        },
        FieldDef {
            path: seg("mode"),
            ty: Type::Enum(vec!["read".into(), "write".into()]),
            constraints: vec![],
        },
        FieldDef {
            path: seg("note"),
            ty: Type::Optional(Box::new(Type::String)),
            constraints: vec![],
        },
    ]
}

#[test]
fn insert_get_nested_object_list_enum_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("comp.typra");
    let (id, _) = {
        let mut db = Database::open(&path).unwrap();
        let out = db
            .register_collection("c", composite_fields(), "id")
            .unwrap();
        let mut meta = BTreeMap::new();
        meta.insert("label".into(), RowValue::String("hi".into()));
        let mut row = BTreeMap::new();
        row.insert("id".into(), RowValue::String("pk1".into()));
        row.insert("meta".into(), RowValue::Object(meta));
        row.insert(
            "tags".into(),
            RowValue::List(vec![RowValue::String("a".into())]),
        );
        row.insert("mode".into(), RowValue::String("read".into()));
        db.insert(out.0, row).unwrap();
        out
    };

    let db = Database::open(&path).unwrap();
    let got = db
        .get(id, &ScalarValue::String("pk1".into()))
        .unwrap()
        .expect("row");
    match got.get("meta") {
        Some(RowValue::Object(m)) => {
            assert_eq!(m.get("label"), Some(&RowValue::String("hi".into())));
        }
        o => panic!("expected object meta, got {:?}", o),
    }
    assert_eq!(
        got.get("tags"),
        Some(&RowValue::List(vec![RowValue::String("a".into())]))
    );
    assert_eq!(got.get("mode"), Some(&RowValue::String("read".into())));
    assert_eq!(got.get("note"), Some(&RowValue::None));
}

#[test]
fn insert_optional_note_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("opt.typra");
    {
        let mut db = Database::open(&path).unwrap();
        let (cid, _) = db
            .register_collection("c", composite_fields(), "id")
            .unwrap();
        let mut meta = BTreeMap::new();
        meta.insert("label".into(), RowValue::String("x".into()));
        let mut row = BTreeMap::new();
        row.insert("id".into(), RowValue::String("k".into()));
        row.insert("meta".into(), RowValue::Object(meta));
        row.insert(
            "tags".into(),
            RowValue::List(vec![RowValue::String("t".into())]),
        );
        row.insert("mode".into(), RowValue::String("write".into()));
        row.insert("note".into(), RowValue::String("n".into()));
        db.insert(cid, row).unwrap();
    }
    let db = Database::open(&path).unwrap();
    let got = db
        .get(CollectionId(1), &ScalarValue::String("k".into()))
        .unwrap()
        .expect("row");
    assert_eq!(got.get("note"), Some(&RowValue::String("n".into())));
}

#[test]
fn insert_constraint_violation_year() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = Database::open(dir.path().join("c.typra")).unwrap();
    let fields = vec![
        FieldDef {
            path: seg("title"),
            ty: Type::String,
            constraints: vec![],
        },
        FieldDef {
            path: seg("year"),
            ty: Type::Int64,
            constraints: vec![Constraint::MinI64(2000), Constraint::MaxI64(2100)],
        },
    ];
    let (id, _) = db.register_collection("books", fields, "title").unwrap();
    let mut row = BTreeMap::new();
    row.insert("title".into(), RowValue::String("x".into()));
    row.insert("year".into(), RowValue::Int64(1999));
    let e = db.insert(id, row).unwrap_err();
    assert!(matches!(e, DbError::Validation(_)));
}

#[test]
fn insert_nonempty_list_violation() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = Database::open(dir.path().join("ne.typra")).unwrap();
    let (id, _) = db
        .register_collection("c", composite_fields(), "id")
        .unwrap();
    let mut meta = BTreeMap::new();
    meta.insert("label".into(), RowValue::String("x".into()));
    let mut row = BTreeMap::new();
    row.insert("id".into(), RowValue::String("a".into()));
    row.insert("meta".into(), RowValue::Object(meta));
    row.insert("tags".into(), RowValue::List(vec![]));
    row.insert("mode".into(), RowValue::String("read".into()));
    let e = db.insert(id, row).unwrap_err();
    assert!(matches!(e, DbError::Validation(_)));
}

#[test]
fn reopen_preserves_composite_row() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("re.typra");
    {
        let mut db = Database::open(&path).unwrap();
        let (cid, _) = db
            .register_collection("c", composite_fields(), "id")
            .unwrap();
        let mut meta = BTreeMap::new();
        meta.insert("label".into(), RowValue::String("L".into()));
        let mut row = BTreeMap::new();
        row.insert("id".into(), RowValue::String("one".into()));
        row.insert("meta".into(), RowValue::Object(meta));
        row.insert(
            "tags".into(),
            RowValue::List(vec![RowValue::String("z".into())]),
        );
        row.insert("mode".into(), RowValue::String("write".into()));
        db.insert(cid, row).unwrap();
    }
    let db2 = Database::open(&path).unwrap();
    let r = db2
        .get(CollectionId(1), &ScalarValue::String("one".into()))
        .unwrap()
        .unwrap();
    assert!(matches!(r.get("meta"), Some(RowValue::Object(_))));
}
