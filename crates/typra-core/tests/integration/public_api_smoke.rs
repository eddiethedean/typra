//! Compile-time guardrail for typra-core's public API.
//!
//! This is intentionally a lightweight "does it still compile" test that exercises the stable
//! re-exports from `typra_core` without relying on internal module structure.

use typra_core::prelude::*;

#[test]
fn public_api_smoke_compiles() {
    // Ensure key re-exports remain available.
    let _opts = OpenOptions::default();

    // Schema/value types.
    let _id = CollectionId(1);
    let _v = SchemaVersion(1);
    let _fd = FieldDef::new(FieldPath::new([std::borrow::Cow::Borrowed("x")]).unwrap(), Type::Int64);

    // Database handle exists and can be opened in memory.
    let _db = Database::open_in_memory().unwrap();
}

