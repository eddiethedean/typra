use std::path::{Path, PathBuf};

use crate::error::{DbError, FormatError};
use crate::file_format::{decode_header, FileHeader, FILE_HEADER_SIZE};
use crate::manifest::decode_manifest_v0;
use crate::publish::append_manifest_and_publish;
use crate::segments::header::SegmentType;
use crate::segments::reader::{read_segment_header_at, scan_segments};
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
            let _ = append_manifest_and_publish(&mut store, segment_start)?;
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
                    let _ = append_manifest_and_publish(&mut store, segment_start)?;
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
                let selected = Self::read_and_select_superblock(&mut store)?;
                if selected.manifest_offset != 0 {
                    match Self::read_manifest(&mut store, &selected) {
                        Ok(_) => {}
                        Err(_) => {
                            // Fall back to scanning segments if manifest pointer is corrupt.
                            let _ = scan_segments(&mut store, segment_start)?;
                        }
                    }
                }
            }
        }

        let db = Self {
            path,
            _store: store,
        };
        Ok(db)
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

    fn read_manifest(store: &mut impl Store, sb: &Superblock) -> Result<(), DbError> {
        let (_, header) = read_segment_header_at(store, sb.manifest_offset)?;
        if header.segment_type != SegmentType::Manifest {
            return Err(DbError::Format(FormatError::UnsupportedVersion {
                major: 0,
                minor: 0,
            }));
        }
        let mut payload = vec![0u8; header.payload_len as usize];
        store.read_exact_at(
            sb.manifest_offset + crate::segments::header::SEGMENT_HEADER_LEN as u64,
            &mut payload,
        )?;
        let _m = decode_manifest_v0(&payload)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::Database;
    use crate::error::FormatError;
    use crate::file_format::{FileHeader, FILE_HEADER_SIZE};
    use crate::segments::header::{SegmentHeader, SegmentType};
    use crate::segments::writer::SegmentWriter;
    use crate::storage::{FileStore, Store};
    use crate::superblock::{Superblock, SUPERBLOCK_SIZE};
    use crate::DbError;

    fn new_store() -> FileStore {
        let f = tempfile::NamedTempFile::new().unwrap();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(f.path())
            .unwrap();
        FileStore::new(file)
    }

    #[test]
    fn read_and_select_superblock_errors_when_both_invalid() {
        let mut store = new_store();
        store
            .write_all_at(0, &FileHeader::new_v0_3().encode())
            .unwrap();
        let segment_start = (FILE_HEADER_SIZE + 2 * SUPERBLOCK_SIZE) as u64;
        store.write_all_at(segment_start - 1, &[0u8]).unwrap();

        let mut a = Superblock::empty().encode();
        let mut b = Superblock::empty().encode();
        a[0] ^= 0xff;
        b[0] ^= 0xff;
        store.write_all_at(FILE_HEADER_SIZE as u64, &a).unwrap();
        store
            .write_all_at((FILE_HEADER_SIZE + SUPERBLOCK_SIZE) as u64, &b)
            .unwrap();

        let res = Database::read_and_select_superblock(&mut store);
        assert!(matches!(
            res,
            Err(DbError::Format(FormatError::BadSuperblockChecksum))
        ));
    }

    #[test]
    fn read_manifest_rejects_wrong_segment_type() {
        let mut store = new_store();
        store
            .write_all_at(0, &FileHeader::new_v0_3().encode())
            .unwrap();
        let segment_start = (FILE_HEADER_SIZE + 2 * SUPERBLOCK_SIZE) as u64;
        store.write_all_at(segment_start - 1, &[0u8]).unwrap();

        let sb_a = Superblock {
            generation: 1,
            ..Superblock::empty()
        };
        store
            .write_all_at(FILE_HEADER_SIZE as u64, &sb_a.encode())
            .unwrap();
        store
            .write_all_at(
                (FILE_HEADER_SIZE + SUPERBLOCK_SIZE) as u64,
                &Superblock::empty().encode(),
            )
            .unwrap();

        // Append a non-manifest segment and point the superblock at it.
        let mut w = SegmentWriter::new(&mut store, segment_start);
        let off = w
            .append(
                SegmentHeader {
                    segment_type: SegmentType::Schema,
                    payload_len: 0,
                    payload_crc32c: 0,
                },
                b"hi",
            )
            .unwrap();

        let sb = Superblock {
            manifest_offset: off,
            manifest_len: 2,
            ..sb_a
        };
        let res = Database::read_manifest(&mut store, &sb);
        assert!(matches!(
            res,
            Err(DbError::Format(FormatError::UnsupportedVersion { .. }))
        ));
    }

    #[test]
    fn read_and_select_superblock_prefers_a_when_generation_higher() {
        let mut store = new_store();
        store
            .write_all_at(0, &FileHeader::new_v0_3().encode())
            .unwrap();
        let segment_start = (FILE_HEADER_SIZE + 2 * SUPERBLOCK_SIZE) as u64;
        store.write_all_at(segment_start - 1, &[0u8]).unwrap();

        let sb_a = Superblock {
            generation: 10,
            ..Superblock::empty()
        };
        let sb_b = Superblock {
            generation: 9,
            ..Superblock::empty()
        };
        store
            .write_all_at(FILE_HEADER_SIZE as u64, &sb_a.encode())
            .unwrap();
        store
            .write_all_at((FILE_HEADER_SIZE + SUPERBLOCK_SIZE) as u64, &sb_b.encode())
            .unwrap();

        let selected = Database::read_and_select_superblock(&mut store).unwrap();
        assert_eq!(selected.generation, sb_a.generation);
    }
}
