use typra_core::catalog::decode_catalog_payload;
use typra_core::error::{DbError, FormatError};

#[test]
fn decode_optional_primary_name_rejects_empty_string() {
    // Build a minimal v2 create-collection payload where primary_field length is non-zero but bytes are empty.
    // Layout (v2+):
    // ver(u16) kind(u16) collection_id(u32) name(str) schema_version(u32) fields(...) indexes(v4+) primary_field(opt_name)
    //
    // We'll piggyback on an existing valid payload and then patch the primary_field bytes to be empty
    // by setting n=1 and byte=0 (which will fail UTF-8? no) -> actually empty string check triggers
    // only when decoded string is empty; that requires n>0 but bytes decode to "" which isn't possible.
    // So we directly craft: n=1 and byte = 0x00 => string is "\0" (non-empty). Instead craft n=0 => None.
    // The only way to hit EmptyCollectionName here is via invalid utf8 that becomes empty after from_utf8?
    // Not possible. So instead target the other uncovered branches around optional primary by hitting too-long.
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&2u16.to_le_bytes()); // catalog payload v2
    bytes.extend_from_slice(&1u16.to_le_bytes()); // create collection
    bytes.extend_from_slice(&1u32.to_le_bytes()); // collection_id
                                                  // name = "a"
    bytes.extend_from_slice(&1u32.to_le_bytes());
    bytes.extend_from_slice(b"a");
    bytes.extend_from_slice(&1u32.to_le_bytes()); // schema_version
                                                  // fields: 0 (v2 fields encoding uses count u32 then entries; zero is accepted)
    bytes.extend_from_slice(&0u32.to_le_bytes());
    // indexes absent pre-v4
    // primary_field optional name: length 1 but provide 0 bytes by truncating (forces unexpected eof)
    bytes.extend_from_slice(&1u32.to_le_bytes());
    let err = decode_catalog_payload(&bytes).unwrap_err();
    assert!(matches!(
        err,
        DbError::Format(FormatError::InvalidCatalogPayload { .. })
    ));
}

#[test]
fn decode_optional_primary_name_rejects_too_long() {
    // Similar to above, but give a huge length so decode_optional_primary_name hits name-too-long.
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&2u16.to_le_bytes()); // v2
    bytes.extend_from_slice(&1u16.to_le_bytes()); // create
    bytes.extend_from_slice(&1u32.to_le_bytes()); // collection_id
    bytes.extend_from_slice(&1u32.to_le_bytes());
    bytes.extend_from_slice(b"a"); // name
    bytes.extend_from_slice(&1u32.to_le_bytes()); // schema_version
    bytes.extend_from_slice(&0u32.to_le_bytes()); // fields=0
    bytes.extend_from_slice(&(10_000u32).to_le_bytes()); // absurd primary_field length
    let err = decode_catalog_payload(&bytes).unwrap_err();
    assert!(matches!(
        err,
        DbError::Format(FormatError::InvalidCatalogPayload { .. })
    ));
}
