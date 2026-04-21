use std::path::PathBuf;

use typra::prelude::*;

fn main() -> Result<(), DbError> {
    let path = PathBuf::from("example.typra");
    let db = Database::open(&path)?;
    println!("opened: {}", db.path().display());
    Ok(())
}

