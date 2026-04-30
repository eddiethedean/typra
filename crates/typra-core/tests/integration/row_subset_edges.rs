use std::borrow::Cow;
use std::collections::BTreeMap;

use typra_core::schema::{FieldDef, FieldPath, Type};
use typra_core::{db, RowValue};

fn fp(parts: &[&'static str]) -> FieldPath {
    FieldPath(parts.iter().copied().map(Cow::Borrowed).collect())
}

#[test]
fn row_subset_handles_missing_paths_and_none_and_non_object_parents() {
    let mut row = BTreeMap::new();
    row.insert("a".into(), RowValue::None);
    row.insert("x".into(), RowValue::Int64(1));

    let wanted = vec![
        FieldDef::new(fp(&["a", "b"]), Type::String),
        FieldDef::new(fp(&["x", "y"]), Type::Int64),
        FieldDef::new(fp(&["missing"]), Type::Int64),
    ];

    let out = db::row_subset_by_field_defs(&row, &wanted);
    assert!(out.is_empty());
}

