use typra_core::error::{DbError, DbErrorKind};
use typra_core::txn::{decode_txn_payload_v0, encode_txn_payload_v0, TXN_PAYLOAD_V0_LEN};

#[test]
fn txn_payload_v0_roundtrips() {
    let b = encode_txn_payload_v0(42);
    let got = decode_txn_payload_v0(&b).unwrap();
    assert_eq!(got, 42);
}

#[test]
fn txn_payload_v0_rejects_wrong_length() {
    let err = decode_txn_payload_v0(&[]).unwrap_err();
    assert_eq!(err.kind(), DbErrorKind::Format);
}

#[test]
fn txn_payload_v0_rejects_unsupported_version() {
    let mut b = encode_txn_payload_v0(1);
    // version is first 2 bytes (little-endian u16)
    b[0] = 9;
    b[1] = 0;
    let err = decode_txn_payload_v0(&b).unwrap_err();
    assert_eq!(err.kind(), DbErrorKind::Format);
}

#[test]
fn txn_payload_v0_rejects_crc_mismatch() {
    let mut b = encode_txn_payload_v0(1);
    assert_eq!(b.len(), TXN_PAYLOAD_V0_LEN);
    // Flip a reserved byte so the crc check fails.
    b[10] ^= 0b0000_0001;
    let err = decode_txn_payload_v0(&b).unwrap_err();
    assert!(matches!(err, DbError::Format(_)));
}

