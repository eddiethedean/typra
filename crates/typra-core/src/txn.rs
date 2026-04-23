//! Transaction control segment payloads (`TxnBegin` / `TxnCommit` / `TxnAbort`).

use crate::checksum::crc32c;
use crate::error::{DbError, FormatError};

pub const TXN_PAYLOAD_VERSION_V0: u16 = 0;
/// Fixed size: version (2) + txn_id (8) + reserved (8) + crc32c of first 18 bytes (4) + pad (2).
pub const TXN_PAYLOAD_V0_LEN: usize = 24;

/// Encode [`TXN_PAYLOAD_VERSION_V0`] body for transaction marker segments.
pub fn encode_txn_payload_v0(txn_id: u64) -> [u8; TXN_PAYLOAD_V0_LEN] {
    let mut buf = [0u8; TXN_PAYLOAD_V0_LEN];
    buf[0..2].copy_from_slice(&TXN_PAYLOAD_VERSION_V0.to_le_bytes());
    buf[2..10].copy_from_slice(&txn_id.to_le_bytes());
    // [10..18] reserved zeros
    let c = crc32c(&buf[0..18]);
    buf[18..22].copy_from_slice(&c.to_le_bytes());
    buf
}

/// Decode and verify CRC; returns `txn_id`.
pub fn decode_txn_payload_v0(payload: &[u8]) -> Result<u64, DbError> {
    if payload.len() != TXN_PAYLOAD_V0_LEN {
        return Err(DbError::Format(FormatError::InvalidTxnPayload {
            message: format!(
                "expected {} bytes, got {}",
                TXN_PAYLOAD_V0_LEN,
                payload.len()
            ),
        }));
    }
    let ver = u16::from_le_bytes([payload[0], payload[1]]);
    if ver != TXN_PAYLOAD_VERSION_V0 {
        return Err(DbError::Format(FormatError::InvalidTxnPayload {
            message: format!("unsupported txn payload version {ver}"),
        }));
    }
    let got_crc = u32::from_le_bytes([payload[18], payload[19], payload[20], payload[21]]);
    let want_crc = crc32c(&payload[0..18]);
    if got_crc != want_crc {
        return Err(DbError::Format(FormatError::InvalidTxnPayload {
            message: "txn payload crc mismatch".into(),
        }));
    }
    Ok(u64::from_le_bytes(payload[2..10].try_into().unwrap()))
}
