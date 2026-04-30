use typra_core::manifest::{decode_manifest_v0, ManifestV0, MANIFEST_V0_LEN};

#[test]
fn manifest_v0_roundtrip() {
    let m = ManifestV0 {
        last_segment_offset: 123,
        last_segment_len: 456,
    };
    let bytes = m.encode();
    assert_eq!(bytes.len(), MANIFEST_V0_LEN);
    let got = decode_manifest_v0(&bytes).unwrap();
    assert_eq!(got, m);
}
