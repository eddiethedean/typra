    use super::*;
    use fs2::FileExt;

    #[test]
    fn open_locked_readonly_returns_wouldblock_if_lock_held_elsewhere() {
        let dir = std::env::temp_dir().join(format!(
            "modelvault-storage-test-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let db_path = dir.join("db.modelvault");
        std::fs::write(&db_path, b"").unwrap();

        let lock_path = FileStore::lock_path_for_db_path(&db_path);
        let lock_file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(false)
            .open(&lock_path)
            .unwrap();
        lock_file.try_lock_exclusive().unwrap();

        let err = FileStore::open_locked(&db_path, OpenMode::ReadOnly).unwrap_err();
        assert!(matches!(
            err,
            DbError::Io(ref e) if e.kind() == std::io::ErrorKind::WouldBlock
        ));
    }

    #[test]
    fn writer_lock_guard_nested_opens_drop_reduces_refs_to_zero() {
        let dir = std::env::temp_dir().join(format!(
            "modelvault-storage-nested-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let db_path = dir.join("db.modelvault");
        std::fs::write(&db_path, b"").unwrap();

        let _g1 = FileStore::open_locked(&db_path, OpenMode::ReadWrite).unwrap();
        let _g2 = FileStore::open_locked(&db_path, OpenMode::ReadWrite).unwrap();
    }

    #[test]
    fn writer_lock_guard_drop_decrements_refs_but_keeps_lock_until_last_drop() {
        let dir = std::env::temp_dir().join(format!(
            "modelvault-storage-refs-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let db_path = dir.join("db.modelvault");
        std::fs::write(&db_path, b"").unwrap();

        let g1 = FileStore::open_locked(&db_path, OpenMode::ReadWrite).unwrap();
        let g2 = FileStore::open_locked(&db_path, OpenMode::ReadWrite).unwrap();

        // Drop one writer handle; internal refcount should decrement but the writer lock remains.
        drop(g1);

        // While writer lock is held in this process, read-only open should fail fast.
        let err = FileStore::open_locked(&db_path, OpenMode::ReadOnly).unwrap_err();
        assert!(matches!(
            err,
            DbError::Io(ref e) if e.to_string().contains("holding writer lock")
        ));

        // Drop the final writer handle; writer lock should be removed.
        drop(g2);

        // Now read-only open should proceed to the cross-process lock check (which should succeed).
        let _ro = FileStore::open_locked(&db_path, OpenMode::ReadOnly).unwrap();
    }
