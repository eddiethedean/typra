pub struct CollectionSchema;

/// Marker trait for Rust types that map to Typra collection records.
///
/// Implement via `#[derive(DbModel)]` from the `typra-derive` crate.
pub trait DbModel {}
