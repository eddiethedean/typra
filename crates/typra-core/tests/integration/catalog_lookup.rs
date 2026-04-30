//! `Catalog::lookup_name` and related helpers.

use std::borrow::Cow;

use typra_core::schema::{FieldDef, FieldPath, Type};
use typra_core::Database;

fn title_field() -> FieldDef {
    FieldDef {
        path: FieldPath(vec![Cow::Owned("title".to_string())]),
        ty: Type::String,
        constraints: vec![],
    }
}

#[test]
fn catalog_lookup_name_trims_and_resolves() {
    let dir = tempfile::tempdir().unwrap();
    let mut db = Database::open(dir.path().join("c.typra")).unwrap();
    db.register_collection("  books  ", vec![title_field()], "title")
        .unwrap();
    let id = db.catalog().lookup_name("books").expect("id");
    assert_eq!(id.0, 1);
    assert!(db.catalog().lookup_name("  books  ").is_some());
    assert!(db.catalog().lookup_name("nope").is_none());
}
