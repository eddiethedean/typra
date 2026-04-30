    use super::TempSpillFile;
    use super::TempSpillGuard;
    use crate::storage::{Store, VecStore};

    #[test]
    fn temp_spill_file_truncates_on_drop() {
        let mut base = VecStore::new();
        base.write_all_at(0, &[1u8; 10]).unwrap();
        let base_len = base.len().unwrap();

        let mut spill = TempSpillFile::new(base).unwrap();
        spill.append_temp_segment(b"hello").unwrap();
        assert!(spill.store_mut().len().unwrap() > base_len);

        let base = spill.finish().unwrap();
        assert_eq!(base.len().unwrap(), base_len);
    }

    #[test]
    fn temp_spill_guard_appends_and_reads_payload_and_truncates() {
        let mut base = VecStore::new();
        base.write_all_at(0, &[2u8; 8]).unwrap();
        let base_len = base.len().unwrap();

        {
            let mut guard = TempSpillGuard::new(&mut base).unwrap();
            assert_eq!(guard.base_len(), base_len);
            let off = guard.append_temp_segment(b"abc").unwrap();
            let got = guard.read_temp_payload(off, 3).unwrap();
            assert_eq!(got, b"abc");
            assert!(guard.store_mut().len().unwrap() > base_len);
        }

        // Dropping the guard truncates to the original length.
        assert_eq!(base.len().unwrap(), base_len);
    }
