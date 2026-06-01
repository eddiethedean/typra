//! Persisted schema catalog: binary codec payloads and [`Catalog`] state.

mod codec;
mod state;

pub use codec::{
    decode_catalog_payload, encode_catalog_payload, CatalogDecodeError, CatalogRecordWire,
    CATALOG_PAYLOAD_VERSION, CATALOG_PAYLOAD_VERSION_V1, CATALOG_PAYLOAD_VERSION_V4,
    ENTRY_KIND_CREATE_COLLECTION, ENTRY_KIND_NEW_SCHEMA_VERSION, MAX_COLLECTION_NAME_BYTES,
    MAX_TYPE_NESTING_DEPTH,
};
pub use state::{Catalog, CollectionInfo};

/// Alias for encoded catalog records on the wire.
pub type CatalogRecord = CatalogRecordWire;
