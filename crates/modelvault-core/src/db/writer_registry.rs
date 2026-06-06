//! One writable [`Database`](super::Database) handle per on-disk path per process.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock};

use crate::error::DbError;

/// Held on writable file-backed databases; unregisters the path on drop.
pub struct WriterRegistryGuard {
    path: PathBuf,
}

impl WriterRegistryGuard {
    pub fn new(path: PathBuf) -> Result<Self, DbError> {
        register_writable(&path)?;
        Ok(Self { path })
    }
}

impl Drop for WriterRegistryGuard {
    fn drop(&mut self) {
        super::handle_registry::unregister(&self.path);
        unregister_writable(&self.path);
    }
}

fn map() -> &'static Mutex<HashMap<PathBuf, usize>> {
    static MAP: OnceLock<Mutex<HashMap<PathBuf, usize>>> = OnceLock::new();
    MAP.get_or_init(|| Mutex::new(HashMap::new()))
}

fn is_tracked_path(path: &Path) -> bool {
    path.as_os_str() != ":memory:"
}

/// Register a writable open for `path`. Fails if another writable handle is already registered.
pub fn register_writable(path: &Path) -> Result<(), DbError> {
    if !is_tracked_path(path) {
        return Ok(());
    }
    let mut g = map()
        .lock()
        .map_err(|_| DbError::Io(std::io::Error::other("writer registry lock poisoned")))?;
    let count = g.get(path).copied().unwrap_or(0);
    if count > 0 {
        return Err(DbError::Io(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            format!(
                "writable database already open in this process: {}",
                path.display()
            ),
        )));
    }
    g.insert(path.to_path_buf(), 1);
    Ok(())
}

/// Decrement registration when a writable [`Database`] is dropped.
pub fn unregister_writable(path: &Path) {
    if !is_tracked_path(path) {
        return;
    }
    let Ok(mut g) = map().lock() else {
        return;
    };
    if let Some(n) = g.get_mut(path) {
        *n = n.saturating_sub(1);
        if *n == 0 {
            g.remove(path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_rejects_second_writable_on_same_path() {
        let dir = std::env::temp_dir().join(format!(
            "modelvault-writer-reg-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let p = dir.join("db.modelvault");
        register_writable(&p).unwrap();
        assert!(register_writable(&p).is_err());
        unregister_writable(&p);
        register_writable(&p).unwrap();
        unregister_writable(&p);
        let _ = std::fs::remove_dir_all(dir);
    }
}
