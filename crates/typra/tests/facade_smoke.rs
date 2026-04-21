#![cfg(feature = "derive")]

use std::error::Error;

use typra::prelude::*;
use typra::DbModel;

#[derive(DbModel)]
struct Book {
    _title: String,
}

fn assert_db_model<T: typra_core::DbModel>() {}

#[test]
fn facade_smoke() -> Result<(), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    let _db = Database::open(dir.path().join("db.typra"))?;
    let _book = Book {
        _title: "Example".into(),
    };
    assert_db_model::<Book>();
    Ok(())
}
