use crate::error::{DbError, FormatError};

pub const FILE_MAGIC: [u8; 4] = *b"TDB0";

/// On-disk file format version (not the crate version).
///
/// This is intentionally small and conservative in 0.2.0:
/// it exists primarily so `Database::open` can recognize Typra files.
pub const FORMAT_MAJOR: u16 = 0;
pub const FORMAT_MINOR: u16 = 2;

pub const FILE_HEADER_SIZE: usize = 32;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FileHeader {
    pub format_major: u16,
    pub format_minor: u16,
    pub header_size: u32,
    pub flags: u64,
}

impl FileHeader {
    pub fn new_v0_2() -> Self {
        Self {
            format_major: FORMAT_MAJOR,
            format_minor: FORMAT_MINOR,
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
    if format_major != FORMAT_MAJOR || format_minor != FORMAT_MINOR {
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
