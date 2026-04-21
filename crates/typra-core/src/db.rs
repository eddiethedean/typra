use std::path::{Path, PathBuf};

use crate::error::{DbError, FormatError};
use crate::file_format::{decode_header, FileHeader, FILE_HEADER_SIZE};
use crate::storage::{FileStore, Store};
use crate::superblock::{decode_superblock, Superblock, SUPERBLOCK_SIZE};

/// Handle to an on-disk Typra database file.
///
/// Version 0.1 only ensures the backing file exists and is openable; the storage engine is still under development.
pub struct Database {
    path: PathBuf,
    _store: FileStore,
}

impl Database {
    /// Open or create a database at `path`.
    pub fn open(path: impl AsRef<Path>) -> Result<Self, DbError> {
        let path = path.as_ref().to_path_buf();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&path)?;

        let mut store = FileStore::new(file);

        let len = store.len()?;
        let segment_start = (FILE_HEADER_SIZE + 2 * SUPERBLOCK_SIZE) as u64;

        if len == 0 {
            let header = FileHeader::new_v0_3();
            store.write_all_at(0, &header.encode())?;
            Self::init_superblocks(&mut store, segment_start)?;
            store.sync()?;
        } else if len < FILE_HEADER_SIZE as u64 {
            return Err(DbError::Format(FormatError::TruncatedHeader {
                got: len as usize,
                expected: FILE_HEADER_SIZE,
            }));
        } else {
            let mut buf = [0u8; FILE_HEADER_SIZE];
            store.read_exact_at(0, &mut buf)?;
            let header = decode_header(&buf)?;

            if header.format_minor == 2 {
                if len == FILE_HEADER_SIZE as u64 {
                    // Safe, minimal in-place upgrade path: 0.2 header-only -> 0.3 layout.
                    let upgraded = FileHeader::new_v0_3();
                    store.write_all_at(0, &upgraded.encode())?;
                    Self::init_superblocks(&mut store, segment_start)?;
                    store.sync()?;
                } else {
                    return Err(DbError::Format(FormatError::UnsupportedVersion {
                        major: header.format_major,
                        minor: header.format_minor,
                    }));
                }
            } else if header.format_minor == 3 {
                if len < segment_start {
                    return Err(DbError::Format(FormatError::TruncatedSuperblock {
                        got: len as usize,
                        expected: segment_start as usize,
                    }));
                }
                let _selected = Self::read_and_select_superblock(&mut store)?;
            }
        }

        Ok(Self {
            path,
            _store: store,
        })
    }

    /// Path passed to [`Database::open`](Self::open).
    pub fn path(&self) -> &Path {
        &self.path
    }

    fn init_superblocks(store: &mut impl Store, segment_start: u64) -> Result<(), DbError> {
        // Ensure the file is at least large enough to contain the reserved superblock regions.
        store.write_all_at(segment_start - 1, &[0u8])?;

        let sb_a = Superblock {
            generation: 1,
            ..Superblock::empty()
        };
        let sb_b = Superblock::empty();

        store.write_all_at(FILE_HEADER_SIZE as u64, &sb_a.encode())?;
        store.write_all_at((FILE_HEADER_SIZE + SUPERBLOCK_SIZE) as u64, &sb_b.encode())?;
        Ok(())
    }

    fn read_and_select_superblock(store: &mut impl Store) -> Result<Superblock, DbError> {
        let mut a = [0u8; SUPERBLOCK_SIZE];
        let mut b = [0u8; SUPERBLOCK_SIZE];
        store.read_exact_at(FILE_HEADER_SIZE as u64, &mut a)?;
        store.read_exact_at((FILE_HEADER_SIZE + SUPERBLOCK_SIZE) as u64, &mut b)?;

        let sa = decode_superblock(&a).ok();
        let sb = decode_superblock(&b).ok();

        match (sa, sb) {
            (Some(sa), Some(sb)) => Ok(if sa.generation >= sb.generation {
                sa
            } else {
                sb
            }),
            (Some(sa), None) => Ok(sa),
            (None, Some(sb)) => Ok(sb),
            (None, None) => Err(DbError::Format(FormatError::BadSuperblockChecksum)),
        }
    }
}
