    use std::cell::Cell;

    use super::{PagedStore, DEFAULT_PAGE_SIZE};
    use crate::storage::{Store, VecStore};

    #[test]
    fn paged_store_reports_page_size_and_into_inner_roundtrips() {
        let raw = VecStore::new();
        let ps = PagedStore::new(raw, 1); // will clamp to 512
        assert_eq!(ps.page_size(), 512);

        let raw2 = ps.into_inner();
        assert_eq!(raw2.len().unwrap(), 0);
    }

    #[test]
    fn page_range_touched_len_zero_is_sentinel() {
        let raw = VecStore::new();
        let ps = PagedStore::new(raw, DEFAULT_PAGE_SIZE);
        assert_eq!(*ps.page_range_touched(123, 0).start(), 0);
        assert_eq!(*ps.page_range_touched(123, 0).end(), 0);
    }

    #[test]
    fn get_page_rejects_missing_page_beyond_eof() {
        let mut raw = VecStore::new();
        raw.write_all_at(0, &[1u8; 8]).unwrap();
        let mut ps = PagedStore::new(raw, DEFAULT_PAGE_SIZE);
        // Page index 1 starts at DEFAULT_PAGE_SIZE, which is beyond current len().
        let err = ps.get_page(1).unwrap_err();
        assert!(matches!(err, crate::error::DbError::Io(_)));
    }

    #[test]
    fn paged_store_roundtrips_reads() {
        let mut raw = VecStore::new();
        raw.write_all_at(0, &[1u8; 100]).unwrap();
        raw.write_all_at(DEFAULT_PAGE_SIZE, &[2u8; 100]).unwrap();

        let mut ps = PagedStore::new(raw, DEFAULT_PAGE_SIZE);
        let mut buf = [0u8; 50];
        ps.read_exact_at(10, &mut buf).unwrap();
        assert_eq!(buf, [1u8; 50]);

        ps.read_exact_at(DEFAULT_PAGE_SIZE + 10, &mut buf).unwrap();
        assert_eq!(buf, [2u8; 50]);
    }

    #[test]
    fn paged_store_invalidates_on_write() {
        let mut raw = VecStore::new();
        raw.write_all_at(0, &[1u8; 32]).unwrap();
        let mut ps = PagedStore::new(raw, DEFAULT_PAGE_SIZE);

        let mut buf = [0u8; 16];
        ps.read_exact_at(0, &mut buf).unwrap();
        assert_eq!(buf, [1u8; 16]);

        ps.write_all_at(0, &[9u8; 16]).unwrap();
        ps.read_exact_at(0, &mut buf).unwrap();
        assert_eq!(buf, [9u8; 16]);
    }

    #[test]
    fn paged_store_truncate_clears_pages() {
        let mut raw = VecStore::new();
        raw.write_all_at(0, &[1u8; (DEFAULT_PAGE_SIZE as usize) * 2])
            .unwrap();
        let mut ps = PagedStore::new(raw, DEFAULT_PAGE_SIZE);

        let mut buf = [0u8; 8];
        ps.read_exact_at(DEFAULT_PAGE_SIZE + 1, &mut buf).unwrap();
        ps.truncate(DEFAULT_PAGE_SIZE).unwrap();

        assert!(ps.read_exact_at(DEFAULT_PAGE_SIZE + 1, &mut buf).is_err());
    }

    /// Second call to `inner.len()` fails — exercises EOF sizing branch that reads `len` inside `get_page`.
    struct LenSecondFails {
        inner: VecStore,
        calls: Cell<u32>,
    }

    impl Store for LenSecondFails {
        fn len(&self) -> Result<u64, crate::error::DbError> {
            let n = self.calls.get().saturating_add(1);
            self.calls.set(n);
            if n >= 2 {
                return Err(crate::error::DbError::Io(std::io::Error::other(
                    "len fails on second call",
                )));
            }
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
            data: &[u8],
        ) -> Result<(), crate::error::DbError> {
            self.inner.write_all_at(offset, data)
        }

        fn sync(&mut self) -> Result<(), crate::error::DbError> {
            self.inner.sync()
        }

        fn truncate(&mut self, len: u64) -> Result<(), crate::error::DbError> {
            self.inner.truncate(len)
        }
    }

    #[test]
    fn read_exact_at_propagates_inner_len_error_from_get_page() {
        let mut inner = VecStore::new();
        inner.write_all_at(0, &[1u8; 100]).unwrap();
        let mut ps = PagedStore::new(
            LenSecondFails {
                inner,
                calls: Cell::new(0),
            },
            DEFAULT_PAGE_SIZE,
        );
        let mut buf = [0u8; 16];
        let err = ps.read_exact_at(0, &mut buf).unwrap_err();
        assert!(matches!(err, crate::error::DbError::Io(_)));
    }

    /// Fails the second `read_exact_at` on the inner store (multi-page logical read).
    struct FailSecondRead {
        inner: VecStore,
        n: Cell<u8>,
    }

    impl Store for FailSecondRead {
        fn len(&self) -> Result<u64, crate::error::DbError> {
            self.inner.len()
        }

        fn read_exact_at(
            &mut self,
            offset: u64,
            buf: &mut [u8],
        ) -> Result<(), crate::error::DbError> {
            let v = self.n.get().saturating_add(1);
            self.n.set(v);
            if v >= 2 {
                return Err(crate::error::DbError::Io(std::io::Error::other(
                    "inner read fails on second call",
                )));
            }
            self.inner.read_exact_at(offset, buf)
        }

        fn write_all_at(
            &mut self,
            offset: u64,
            data: &[u8],
        ) -> Result<(), crate::error::DbError> {
            self.inner.write_all_at(offset, data)
        }

        fn sync(&mut self) -> Result<(), crate::error::DbError> {
            self.inner.sync()
        }

        fn truncate(&mut self, len: u64) -> Result<(), crate::error::DbError> {
            self.inner.truncate(len)
        }
    }

    #[test]
    fn read_exact_at_propagates_inner_read_error_across_pages() {
        let mut inner = VecStore::new();
        inner
            .write_all_at(0, &[7u8; DEFAULT_PAGE_SIZE as usize])
            .unwrap();
        inner
            .write_all_at(
                DEFAULT_PAGE_SIZE,
                &[8u8; (DEFAULT_PAGE_SIZE as usize).saturating_sub(10)],
            )
            .unwrap();

        let mut ps = PagedStore::new(
            FailSecondRead {
                inner,
                n: Cell::new(0),
            },
            DEFAULT_PAGE_SIZE,
        );

        let mut buf = [0u8; 64];
        let off = DEFAULT_PAGE_SIZE - 8;
        let err = ps.read_exact_at(off, &mut buf).unwrap_err();
        assert!(matches!(err, crate::error::DbError::Io(_)));
    }
