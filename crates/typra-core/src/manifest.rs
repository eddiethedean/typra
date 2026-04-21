use crate::error::{DbError, FormatError};

pub const MANIFEST_VERSION_V0: u16 = 0;
pub const MANIFEST_V0_LEN: usize = 2 + 8 + 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ManifestV0 {
    pub last_segment_offset: u64,
    pub last_segment_len: u64,
}

impl ManifestV0 {
    pub fn encode(self) -> [u8; MANIFEST_V0_LEN] {
        let mut buf = [0u8; MANIFEST_V0_LEN];
        buf[0..2].copy_from_slice(&MANIFEST_VERSION_V0.to_le_bytes());
        buf[2..10].copy_from_slice(&self.last_segment_offset.to_le_bytes());
        buf[10..18].copy_from_slice(&self.last_segment_len.to_le_bytes());
        buf
    }
}

pub fn decode_manifest_v0(bytes: &[u8]) -> Result<ManifestV0, DbError> {
    if bytes.len() < MANIFEST_V0_LEN {
        return Err(DbError::Format(FormatError::TruncatedHeader {
            got: bytes.len(),
            expected: MANIFEST_V0_LEN,
        }));
    }

    let version = u16::from_le_bytes([bytes[0], bytes[1]]);
    if version != MANIFEST_VERSION_V0 {
        return Err(DbError::Format(FormatError::UnsupportedVersion {
            major: 0,
            minor: version,
        }));
    }

    let last_segment_offset = u64::from_le_bytes(bytes[2..10].try_into().unwrap());
    let last_segment_len = u64::from_le_bytes(bytes[10..18].try_into().unwrap());
    Ok(ManifestV0 {
        last_segment_offset,
        last_segment_len,
    })
}
