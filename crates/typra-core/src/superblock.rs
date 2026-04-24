//! Dual redundant superblocks (`TSB0`) storing generation and manifest pointer.

use crate::checksum::{crc32c, CHECKSUM_KIND_CRC32C};
use crate::error::{DbError, FormatError};

pub const SUPERBLOCK_SIZE: usize = 4096;
pub const SUPERBLOCK_MAGIC: [u8; 4] = *b"TSB0";
pub const SUPERBLOCK_VERSION_V0: u16 = 0;
pub const SUPERBLOCK_VERSION_V1: u16 = 1;
pub const SUPERBLOCK_VERSION: u16 = SUPERBLOCK_VERSION_V1;

/// Fixed-layout block pointing at the manifest segment and carrying a monotonic `generation`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Superblock {
    pub generation: u64,
    pub manifest_offset: u64,
    pub manifest_len: u32,
    pub checkpoint_offset: u64,
    pub checkpoint_len: u32,
    pub checksum_kind: u8,
}

impl Superblock {
    pub fn empty() -> Self {
        Self {
            generation: 0,
            manifest_offset: 0,
            manifest_len: 0,
            checkpoint_offset: 0,
            checkpoint_len: 0,
            checksum_kind: CHECKSUM_KIND_CRC32C,
        }
    }

    pub fn encode(self) -> [u8; SUPERBLOCK_SIZE] {
        let mut buf = [0u8; SUPERBLOCK_SIZE];
        buf[0..4].copy_from_slice(&SUPERBLOCK_MAGIC);
        buf[4..6].copy_from_slice(&SUPERBLOCK_VERSION.to_le_bytes());

        buf[8..16].copy_from_slice(&self.generation.to_le_bytes());
        buf[16..24].copy_from_slice(&self.manifest_offset.to_le_bytes());
        buf[24..28].copy_from_slice(&self.manifest_len.to_le_bytes());
        buf[28] = self.checksum_kind;

        buf[36..44].copy_from_slice(&self.checkpoint_offset.to_le_bytes());
        buf[44..48].copy_from_slice(&self.checkpoint_len.to_le_bytes());

        let crc = crc32c(&buf[0..48]);
        buf[48..52].copy_from_slice(&crc.to_le_bytes());
        buf
    }
}

pub fn decode_superblock(bytes: &[u8]) -> Result<Superblock, DbError> {
    if bytes.len() < SUPERBLOCK_SIZE {
        return Err(DbError::Format(FormatError::TruncatedSuperblock {
            got: bytes.len(),
            expected: SUPERBLOCK_SIZE,
        }));
    }

    if bytes[0..4] != SUPERBLOCK_MAGIC {
        let mut got = [0u8; 4];
        got.copy_from_slice(&bytes[0..4]);
        return Err(DbError::Format(FormatError::BadSuperblockMagic { got }));
    }

    let version = u16::from_le_bytes([bytes[4], bytes[5]]);
    if version != SUPERBLOCK_VERSION_V0 && version != SUPERBLOCK_VERSION_V1 {
        return Err(DbError::Format(FormatError::UnsupportedVersion {
            major: 0,
            minor: version,
        }));
    }

    let checksum_kind = bytes[28];
    if checksum_kind != CHECKSUM_KIND_CRC32C {
        return Err(DbError::Format(FormatError::UnsupportedVersion {
            major: 0,
            minor: checksum_kind as u16,
        }));
    }

    let (expected_crc, actual_crc) = if version == SUPERBLOCK_VERSION_V0 {
        (
            u32::from_le_bytes([bytes[32], bytes[33], bytes[34], bytes[35]]),
            crc32c(&bytes[0..32]),
        )
    } else {
        (
            u32::from_le_bytes([bytes[48], bytes[49], bytes[50], bytes[51]]),
            crc32c(&bytes[0..48]),
        )
    };
    if expected_crc != actual_crc {
        return Err(DbError::Format(FormatError::BadSuperblockChecksum));
    }

    let generation = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
    let manifest_offset = u64::from_le_bytes(bytes[16..24].try_into().unwrap());
    let manifest_len = u32::from_le_bytes(bytes[24..28].try_into().unwrap());

    let (checkpoint_offset, checkpoint_len) = if version == SUPERBLOCK_VERSION_V0 {
        (0, 0)
    } else {
        (
            u64::from_le_bytes(bytes[36..44].try_into().unwrap()),
            u32::from_le_bytes(bytes[44..48].try_into().unwrap()),
        )
    };

    Ok(Superblock {
        generation,
        manifest_offset,
        manifest_len,
        checkpoint_offset,
        checkpoint_len,
        checksum_kind,
    })
}
