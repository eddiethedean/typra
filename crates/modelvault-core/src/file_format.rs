//! Fixed-size file header (`TDB0`) and format major/minor constants.
//!
//! The crate version is unrelated to [`FORMAT_MAJOR`] / [`FORMAT_MINOR`]; see `docs/` for evolution.

use crate::error::{DbError, FormatError};

pub const FILE_MAGIC: [u8; 4] = *b"TDB0";

/// On-disk file format version (not the crate version).
///
/// This is intentionally small and conservative in 0.2.0:
/// it exists primarily so `Database::open` can recognize ModelVault files.
pub const FORMAT_MAJOR: u16 = 0;
/// Format minor for catalog-only databases (0.4.x).
pub const FORMAT_MINOR_V4: u16 = 4;
/// On-disk minor for 0.7.x files (records + catalog + indexes; no transaction markers).
pub const FORMAT_MINOR: u16 = 5;
/// Format minor 6+ uses `TxnBegin` / `TxnCommit` / `TxnAbort` segment framing (0.8.0+).
pub const FORMAT_MINOR_V6: u16 = 6;
/// Legacy `0.3` format (superblocks + segments; catalog may be empty until upgraded).
pub const FORMAT_MINOR_V3: u16 = 3;

pub const FILE_HEADER_SIZE: usize = 32;

/// Maximum number of entries in a single decoded segment payload (spill, index, etc.).
pub const MAX_SEGMENT_DECODE_ENTRIES: usize = 1_048_576;

/// Maximum segment payload size (bytes) before allocation.
pub const MAX_SEGMENT_PAYLOAD_BYTES: u64 = 64 * 1024 * 1024;

/// Maximum decoded string/bytes field size.
pub const MAX_FIELD_BYTES: usize = 16 * 1024 * 1024;

/// Maximum rows returned by a single query or SQL LIMIT clause.
pub const MAX_QUERY_LIMIT: usize = 1_048_576;

/// Maximum regex pattern length in schema constraints.
pub const MAX_REGEX_PATTERN_LEN: usize = 512;

/// Rejects corrupt or hostile payloads that claim an excessive entry count.
pub fn check_decode_entry_count(n: usize) -> Result<(), DbError> {
    if n > MAX_SEGMENT_DECODE_ENTRIES {
        return Err(DbError::Format(FormatError::InvalidCatalogPayload {
            message: format!("decode entry count {n} exceeds maximum {MAX_SEGMENT_DECODE_ENTRIES}"),
        }));
    }
    Ok(())
}

/// Rejects segment payloads larger than [`MAX_SEGMENT_PAYLOAD_BYTES`].
pub fn check_segment_payload_len(len: u64) -> Result<(), DbError> {
    if len > MAX_SEGMENT_PAYLOAD_BYTES {
        return Err(DbError::Format(FormatError::InvalidCatalogPayload {
            message: format!(
                "segment payload length {len} exceeds maximum {MAX_SEGMENT_PAYLOAD_BYTES}"
            ),
        }));
    }
    Ok(())
}

/// Rejects field blobs larger than [`MAX_FIELD_BYTES`].
pub fn check_field_bytes_len(n: usize) -> Result<(), DbError> {
    if n > MAX_FIELD_BYTES {
        return Err(DbError::Format(FormatError::InvalidCatalogPayload {
            message: format!("field length {n} exceeds maximum {MAX_FIELD_BYTES}"),
        }));
    }
    Ok(())
}

/// Parsed or constructed first [`FILE_HEADER_SIZE`] bytes of a ModelVault file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileHeader {
    pub format_major: u16,
    pub format_minor: u16,
    pub header_size: u32,
    pub flags: u64,
}

impl FileHeader {
    pub fn new_v0_3() -> Self {
        Self {
            format_major: FORMAT_MAJOR,
            format_minor: FORMAT_MINOR_V3,
            header_size: FILE_HEADER_SIZE as u32,
            flags: 0,
        }
    }

    pub fn new_v0_4() -> Self {
        Self {
            format_major: FORMAT_MAJOR,
            format_minor: FORMAT_MINOR_V4,
            header_size: FILE_HEADER_SIZE as u32,
            flags: 0,
        }
    }

    pub fn new_v0_5() -> Self {
        Self {
            format_major: FORMAT_MAJOR,
            format_minor: FORMAT_MINOR,
            header_size: FILE_HEADER_SIZE as u32,
            flags: 0,
        }
    }

    /// Header for new databases in 0.8.0+ (transaction-framed writes).
    pub fn new_v0_8() -> Self {
        Self {
            format_major: FORMAT_MAJOR,
            format_minor: FORMAT_MINOR_V6,
            header_size: FILE_HEADER_SIZE as u32,
            flags: 0,
        }
    }

    pub fn encode(self) -> [u8; FILE_HEADER_SIZE] {
        let mut buf = [0u8; FILE_HEADER_SIZE];
        buf[0..4].copy_from_slice(&FILE_MAGIC);
        buf[4..6].copy_from_slice(&self.format_major.to_le_bytes());
        buf[6..8].copy_from_slice(&self.format_minor.to_le_bytes());
        buf[8..12].copy_from_slice(&self.header_size.to_le_bytes());
        buf[12..20].copy_from_slice(&self.flags.to_le_bytes());
        buf
    }
}

pub fn decode_header(bytes: &[u8]) -> Result<FileHeader, DbError> {
    if bytes.len() < FILE_HEADER_SIZE {
        return Err(DbError::Format(FormatError::TruncatedHeader {
            got: bytes.len(),
            expected: FILE_HEADER_SIZE,
        }));
    }

    if bytes[0..4] != FILE_MAGIC {
        let mut got = [0u8; 4];
        got.copy_from_slice(&bytes[0..4]);
        return Err(DbError::Format(FormatError::BadMagic { got }));
    }

    let format_major = u16::from_le_bytes([bytes[4], bytes[5]]);
    let format_minor = u16::from_le_bytes([bytes[6], bytes[7]]);
    if format_major != FORMAT_MAJOR || !(2..=6).contains(&format_minor) {
        return Err(DbError::Format(FormatError::UnsupportedVersion {
            major: format_major,
            minor: format_minor,
        }));
    }

    let header_size = u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);
    let flags = u64::from_le_bytes([
        bytes[12], bytes[13], bytes[14], bytes[15], bytes[16], bytes[17], bytes[18], bytes[19],
    ]);

    Ok(FileHeader {
        format_major,
        format_minor,
        header_size,
        flags,
    })
}
