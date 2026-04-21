use std::borrow::Cow;
use std::path::PathBuf;

use typra::prelude::*;
use typra::schema::FieldPath;
use typra::FieldDef;
use typra::Type;

fn main() -> Result<(), DbError> {
    let path = PathBuf::from("example.typra");
    let mut db = Database::open(&path)?;
    println!("opened: {}", db.path().display());
    let (id, ver) = db.register_collection(
        "books",
        vec![FieldDef {
            path: FieldPath::new([Cow::Borrowed("title")])?,
            ty: Type::String,
        }],
    )?;
    println!("registered collection id={} version={}", id.0, ver.0);
    Ok(())
}
