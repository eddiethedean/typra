use std::path::Path;

pub(crate) trait FsOps {
    fn remove_file(&self, path: &Path) -> std::io::Result<()>;
    fn rename(&self, from: &Path, to: &Path) -> std::io::Result<()>;
    fn copy(&self, from: &Path, to: &Path) -> std::io::Result<u64>;

    fn open_read(&self, path: &Path) -> std::io::Result<std::fs::File>;
    #[cfg_attr(not(unix), allow(dead_code))]
    fn open_dir(&self, path: &Path) -> std::io::Result<std::fs::File>;

    fn open_read_write_create_truncate(&self, path: &Path) -> std::io::Result<std::fs::File>;
    fn open_read_write_create_new(&self, path: &Path) -> std::io::Result<std::fs::File>;

    fn read(&self, path: &Path) -> std::io::Result<Vec<u8>>;
    fn write(&self, path: &Path, bytes: &[u8]) -> std::io::Result<()>;
}

pub(crate) struct StdFsOps;

impl FsOps for StdFsOps {
    fn remove_file(&self, path: &Path) -> std::io::Result<()> {
        std::fs::remove_file(path)
    }

    fn rename(&self, from: &Path, to: &Path) -> std::io::Result<()> {
        std::fs::rename(from, to)
    }

    fn copy(&self, from: &Path, to: &Path) -> std::io::Result<u64> {
        std::fs::copy(from, to)
    }

    fn open_read(&self, path: &Path) -> std::io::Result<std::fs::File> {
        std::fs::OpenOptions::new().read(true).open(path)
    }

    #[cfg_attr(not(unix), allow(dead_code))]
    fn open_dir(&self, path: &Path) -> std::io::Result<std::fs::File> {
        std::fs::File::open(path)
    }

    fn open_read_write_create_truncate(&self, path: &Path) -> std::io::Result<std::fs::File> {
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(path)
    }

    fn open_read_write_create_new(&self, path: &Path) -> std::io::Result<std::fs::File> {
        std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(path)
    }

    fn read(&self, path: &Path) -> std::io::Result<Vec<u8>> {
        std::fs::read(path)
    }

    fn write(&self, path: &Path, bytes: &[u8]) -> std::io::Result<()> {
        std::fs::write(path, bytes)
    }
}

#[cfg(test)]
mod std_fs_ops_tests {
    use super::{FsOps, StdFsOps};
    use std::io::Write;

    #[test]
    fn std_fs_ops_delegates_to_std_fs() {
        let tmp = tempfile::tempdir().unwrap();
        let a = tmp.path().join("a.txt");
        let b = tmp.path().join("b.txt");
        let c = tmp.path().join("c.txt");
        let d = tmp.path().join("d.txt");
        let e = tmp.path().join("e.txt");
        let f = tmp.path().join("f.txt");
        let g = tmp.path().join("g.txt");
        let h = tmp.path().join("h.txt");

        StdFsOps.write(&a, b"hi").unwrap();
        assert_eq!(StdFsOps.read(&a).unwrap(), b"hi");
        assert_eq!(StdFsOps.copy(&a, &b).unwrap(), 2);

        let mut w = StdFsOps.open_read_write_create_truncate(&c).unwrap();
        w.write_all(b"ab").unwrap();
        drop(w);
        let mut r = StdFsOps.open_read(&c).unwrap();
        let mut buf = String::new();
        std::io::Read::read_to_string(&mut r, &mut buf).unwrap();
        assert_eq!(buf, "ab");

        StdFsOps.open_read_write_create_new(&d).unwrap();
        assert!(StdFsOps.open_read_write_create_new(&d).is_err());

        std::fs::create_dir_all(tmp.path().join("dir")).unwrap();
        let dir_path = tmp.path().join("dir");
        #[cfg(unix)]
        assert!(StdFsOps.open_dir(&dir_path).is_ok());
        #[cfg(not(unix))]
        let _ = dir_path;

        let mut nw = StdFsOps.open_read_write_create_new(&e).unwrap();
        nw.write_all(b"x").unwrap();
        drop(nw);
        StdFsOps.remove_file(&e).unwrap();
        assert!(!e.exists());

        StdFsOps.write(&f, b"1").unwrap();
        StdFsOps.rename(&f, &g).unwrap();
        assert!(!f.exists());
        assert_eq!(StdFsOps.read(&g).unwrap(), b"1");

        StdFsOps.write(&h, b"").unwrap();
        StdFsOps.remove_file(&h).unwrap();
    }
}
