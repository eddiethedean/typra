use crate::checksum::{crc32c, CHECKSUM_KIND_CRC32C};
use crate::error::{DbError, FormatError};

pub const SEGMENT_MAGIC: [u8; 4] = *b"TSG0";
pub const SEGMENT_VERSION: u16 = 0;
pub const SEGMENT_HEADER_LEN: usize = 32;

#[repr(u16)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SegmentType {
    Schema = 1,
    Record = 2,
    Manifest = 3,
    Checkpoint = 4,
}

impl SegmentType {
    fn from_u16(v: u16) -> Option<Self> {
        Some(match v {
            1 => SegmentType::Schema,
            2 => SegmentType::Record,
            3 => SegmentType::Manifest,
            4 => SegmentType::Checkpoint,
            _ => return None,
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SegmentHeader {
    pub segment_type: SegmentType,
    pub payload_len: u64,
    pub payload_crc32c: u32,
}

impl SegmentHeader {
    pub fn encode(self) -> [u8; SEGMENT_HEADER_LEN] {
        let mut buf = [0u8; SEGMENT_HEADER_LEN];
        buf[0..4].copy_from_slice(&SEGMENT_MAGIC);
        buf[4..6].copy_from_slice(&SEGMENT_VERSION.to_le_bytes());
        buf[6..8].copy_from_slice(&(self.segment_type as u16).to_le_bytes());
        buf[8..12].copy_from_slice(&(SEGMENT_HEADER_LEN as u32).to_le_bytes());
        buf[12..20].copy_from_slice(&self.payload_len.to_le_bytes());
        buf[20..24].copy_from_slice(&self.payload_crc32c.to_le_bytes());
        buf[24] = CHECKSUM_KIND_CRC32C;

        let crc = crc32c(&buf[0..28]);
        buf[28..32].copy_from_slice(&crc.to_le_bytes());
        buf
    }
}

pub fn decode_segment_header(bytes: &[u8]) -> Result<SegmentHeader, DbError> {
    if bytes.len() < SEGMENT_HEADER_LEN {
        return Err(DbError::Format(FormatError::TruncatedSegmentHeader {
            got: bytes.len(),
            expected: SEGMENT_HEADER_LEN,
        }));
    }

    if bytes[0..4] != SEGMENT_MAGIC {
        let mut got = [0u8; 4];
        got.copy_from_slice(&bytes[0..4]);
        return Err(DbError::Format(FormatError::BadSegmentMagic { got }));
    }

    let version = u16::from_le_bytes([bytes[4], bytes[5]]);
    if version != SEGMENT_VERSION {
        return Err(DbError::Format(FormatError::UnsupportedVersion {
            major: 0,
            minor: version,
        }));
    }

    let segment_type_raw = u16::from_le_bytes([bytes[6], bytes[7]]);
    let Some(segment_type) = SegmentType::from_u16(segment_type_raw) else {
        return Err(DbError::Format(FormatError::UnsupportedVersion {
            major: 0,
            minor: segment_type_raw,
        }));
    };

    let header_len = u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);
    if header_len as usize != SEGMENT_HEADER_LEN {
        return Err(DbError::Format(FormatError::UnsupportedVersion {
            major: 0,
            minor: header_len as u16,
        }));
    }

    let payload_len = u64::from_le_bytes(bytes[12..20].try_into().unwrap());
    let payload_crc32c = u32::from_le_bytes([bytes[20], bytes[21], bytes[22], bytes[23]]);

    let checksum_kind = bytes[24];
    if checksum_kind != CHECKSUM_KIND_CRC32C {
        return Err(DbError::Format(FormatError::UnsupportedVersion {
            major: 0,
            minor: checksum_kind as u16,
        }));
    }

    let expected_crc = u32::from_le_bytes([bytes[28], bytes[29], bytes[30], bytes[31]]);
    let actual_crc = crc32c(&bytes[0..28]);
    if expected_crc != actual_crc {
        return Err(DbError::Format(FormatError::BadSegmentHeaderChecksum));
    }

    Ok(SegmentHeader {
        segment_type,
        payload_len,
        payload_crc32c,
    })
}
