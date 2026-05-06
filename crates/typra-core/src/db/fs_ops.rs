use std::path::Path;

pub(crate) trait FsOps {
    fn remove_file(&self, path: &Path) -> std::io::Result<()>;
    fn rename(&self, from: &Path, to: &Path) -> std::io::Result<()>;
    fn copy(&self, from: &Path, to: &Path) -> std::io::Result<u64>;

    fn open_read(&self, path: &Path) -> std::io::Result<std::fs::File>;
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

