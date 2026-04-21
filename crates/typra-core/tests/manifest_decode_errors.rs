use typra_core::error::FormatError;
use typra_core::manifest::{decode_manifest_v0, ManifestV0, MANIFEST_V0_LEN};
use typra_core::DbError;

#[test]
fn decode_manifest_v0_rejects_truncated() {
    let bytes = [0u8; MANIFEST_V0_LEN - 1];
    let res = decode_manifest_v0(&bytes);
    assert!(matches!(
        res,
        Err(DbError::Format(FormatError::TruncatedHeader { .. }))
    ));
}

#[test]
fn decode_manifest_v0_rejects_wrong_version() {
    let m = ManifestV0 {
        last_segment_offset: 1,
        last_segment_len: 2,
    };
    let mut bytes = m.encode();
    bytes[0] = 9;
    let res = decode_manifest_v0(&bytes);
    assert!(matches!(
        res,
        Err(DbError::Format(FormatError::UnsupportedVersion { .. }))
    ));
}
