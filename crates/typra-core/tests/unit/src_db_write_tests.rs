    use super::commit_segment_batch;
    use crate::segments::header::SegmentType;
    use crate::storage::{Store, VecStore};

    #[test]
    fn commit_segment_batch_appends_all_segments() {
        let mut store = VecStore::new();
        // Seed enough on-disk structure for `append_manifest_and_publish` to work.
        let header = crate::file_format::FileHeader::new_v0_8();
        store.write_all_at(0, &header.encode()).unwrap();
        let segment_start =
            (crate::file_format::FILE_HEADER_SIZE + 2 * crate::superblock::SUPERBLOCK_SIZE) as u64;
        crate::db::open::init_superblocks(&mut store, segment_start).unwrap();
        let _ = crate::publish::append_manifest_and_publish(&mut store, segment_start).unwrap();

        let mut minor = crate::file_format::FORMAT_MINOR_V6;
        let segments = vec![
            (SegmentType::Temp, b"a".as_slice()),
            (SegmentType::Temp, b"b".as_slice()),
        ];
        let start = store.len().unwrap();
        commit_segment_batch(&mut store, start, &mut minor, &segments).unwrap();
        assert!(store.len().unwrap() > start);
    }

    #[test]
    fn commit_segment_batch_surfaces_segment_append_write_errors() {
        #[derive(Debug)]
        struct FailWrites<S: Store> {
            inner: S,
            remaining_ok_writes: usize,
        }

        impl<S: Store> Store for FailWrites<S> {
            fn len(&self) -> Result<u64, crate::error::DbError> {
                self.inner.len()
            }

            fn read_exact_at(
                &mut self,
                offset: u64,
                buf: &mut [u8],
            ) -> Result<(), crate::error::DbError> {
                self.inner.read_exact_at(offset, buf)
            }

            fn write_all_at(
                &mut self,
                offset: u64,
                buf: &[u8],
            ) -> Result<(), crate::error::DbError> {
                if self.remaining_ok_writes == 0 {
                    return Err(crate::error::DbError::Io(std::io::Error::other("injected write failure")));
                }
                self.remaining_ok_writes -= 1;
                self.inner.write_all_at(offset, buf)
            }

            fn sync(&mut self) -> Result<(), crate::error::DbError> {
                self.inner.sync()
            }

            fn truncate(&mut self, len: u64) -> Result<(), crate::error::DbError> {
                self.inner.truncate(len)
            }
        }

        let mut store = FailWrites {
            inner: VecStore::new(),
            remaining_ok_writes: 0,
        };
        // Exercise the `Store` impl methods (coverage) while staying no-op.
        let mut empty = [];
        store.read_exact_at(0, &mut empty).unwrap();
        store.sync().unwrap();
        store.truncate(0).unwrap();
        let _ = store.len().unwrap();
        // Cover both the success and injected-error paths of `write_all_at`.
        store.remaining_ok_writes = 1;
        store.write_all_at(0, b"ok").unwrap();
        let _ = store.write_all_at(0, b"fail").unwrap_err();

        let mut minor = crate::file_format::FORMAT_MINOR;
        let segments = vec![(SegmentType::Temp, b"x".as_slice())];
        let err = commit_segment_batch(&mut store, 0, &mut minor, &segments).unwrap_err();
        assert_eq!(err.kind(), crate::error::DbErrorKind::Io);
    }
