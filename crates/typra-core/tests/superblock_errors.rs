use typra_core::error::FormatError;
use typra_core::superblock::{decode_superblock, Superblock, SUPERBLOCK_SIZE};
use typra_core::DbError;

#[test]
fn decode_superblock_rejects_truncated() {
    let bytes = [0u8; 32];
    let res = decode_superblock(&bytes);
    assert!(matches!(
        res,
        Err(DbError::Format(FormatError::TruncatedSuperblock { .. }))
    ));
}

#[test]
fn decode_superblock_rejects_bad_magic() {
    let mut bytes = Superblock::empty().encode();
    bytes[0] ^= 0xff;
    let res = decode_superblock(&bytes);
    assert!(matches!(
        res,
        Err(DbError::Format(FormatError::BadSuperblockMagic { .. }))
    ));
}

#[test]
fn decode_superblock_rejects_bad_checksum() {
    let mut bytes = Superblock::empty().encode();
    bytes[32] ^= 0xff;
    let res = decode_superblock(&bytes);
    assert!(matches!(
        res,
        Err(DbError::Format(FormatError::BadSuperblockChecksum))
    ));
    assert_eq!(SUPERBLOCK_SIZE, 4096);
}

#[test]
fn decode_superblock_rejects_unsupported_version() {
    let mut bytes = Superblock::empty().encode();
    bytes[4] = 9;
    let crc = crc32c::crc32c(&bytes[0..32]);
    bytes[32..36].copy_from_slice(&crc.to_le_bytes());
    let res = decode_superblock(&bytes);
    assert!(matches!(
        res,
        Err(DbError::Format(FormatError::UnsupportedVersion { .. }))
    ));
}

#[test]
fn decode_superblock_rejects_checksum_kind_mismatch() {
    let mut bytes = Superblock::empty().encode();
    bytes[28] = 9;
    let crc = crc32c::crc32c(&bytes[0..32]);
    bytes[32..36].copy_from_slice(&crc.to_le_bytes());
    let res = decode_superblock(&bytes);
    assert!(matches!(
        res,
        Err(DbError::Format(FormatError::UnsupportedVersion { .. }))
    ));
}
