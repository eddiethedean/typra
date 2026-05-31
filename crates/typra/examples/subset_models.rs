//! Subset models: typed read projections over the same collection.
//!
//! Run: `cargo run -p typra --example subset_models`

use std::collections::BTreeMap;

use typra::prelude::*;
use typra::DbModel;

#[derive(DbModel)]
#[allow(dead_code)]
struct Book {
    #[db(primary)]
    id: i64,
    title: String,
    year: i64,
}

/// Fewer fields than `Book`, same collection name via attribute override.
#[derive(DbModel)]
#[db(collection = "books")]
#[allow(dead_code)]
struct BookTitle {
    #[db(primary)]
    id: i64,
    title: String,
}

fn main() -> Result<(), DbError> {
    let mut db = Database::open_in_memory()?;
    db.register_model::<Book>()?;
    let cid = db.collection_id_named("books")?;

    let mut row = BTreeMap::new();
    row.insert("id".into(), RowValue::Int64(1));
    row.insert("title".into(), RowValue::String("Typra".into()));
    row.insert("year".into(), RowValue::Int64(2026));
    db.insert(cid, row)?;

    let summaries = db.collection::<BookTitle>()?.all()?;
    assert_eq!(summaries.len(), 1);
    match summaries[0].get("title") {
        Some(RowValue::String(s)) => assert_eq!(s, "Typra"),
        other => panic!("expected title string, got {other:?}"),
    }
    assert!(!summaries[0].contains_key("year"));

    println!("subset_models_ok: {} title-only rows", summaries.len());
    Ok(())
}
