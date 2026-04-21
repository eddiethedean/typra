use typra_core::error::FormatError;
use typra_core::file_format::{decode_header, FileHeader, FILE_HEADER_SIZE};
use typra_core::DbError;

#[test]
fn decode_header_rejects_wrong_magic() {
    let mut bytes = [0u8; FILE_HEADER_SIZE];
    bytes[0..4].copy_from_slice(b"NOPE");
    let res = decode_header(&bytes);
    assert!(matches!(res, Err(DbError::Format(FormatError::BadMagic { .. }))));
}

#[test]
fn decode_header_rejects_unsupported_version() {
    let mut bytes = FileHeader::new_v0_2().encode();
    // Mutate version fields.
    bytes[4] = 9;
    bytes[6] = 9;
    let res = decode_header(&bytes);
    assert!(matches!(
        res,
        Err(DbError::Format(FormatError::UnsupportedVersion { .. }))
    ));
}

#[test]
fn decode_header_rejects_truncated() {
    let bytes = [0u8; 8];
    let res = decode_header(&bytes);
    assert!(matches!(
        res,
        Err(DbError::Format(FormatError::TruncatedHeader { .. }))
    ));
}

