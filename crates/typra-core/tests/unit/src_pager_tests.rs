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
