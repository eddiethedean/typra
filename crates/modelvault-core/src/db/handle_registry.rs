//! Process-wide shared database state for same-process read-only views.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, RwLock};

use crate::catalog::Catalog;
use crate::error::DbError;
use crate::index::IndexState;

use super::LatestMap;

/// Live in-memory snapshot mirrored by the writable handle.
#[derive(Debug, Clone)]
pub struct SharedDbState {
    pub catalog: Catalog,
    pub latest: LatestMap,
    pub indexes: IndexState,
    pub segment_start: u64,
    pub format_minor: u16,
    /// Monotonic generation bumped on each mirror push (attached readers detect staleness).
    pub generation: u64,
}

/// Registry entry: readers clone the inner [`Arc`] and release the lock immediately.
pub type SharedDbHandle = Arc<RwLock<Arc<SharedDbState>>>;

/// Canonical registry key so `./db.modelvault` and `/abs/db.modelvault` share one entry.
pub fn registry_key(path: &Path) -> PathBuf {
    path.canonicalize().unwrap_or_else(|_| path.to_path_buf())
}

fn map() -> &'static Mutex<HashMap<PathBuf, SharedDbHandle>> {
    static MAP: OnceLock<Mutex<HashMap<PathBuf, SharedDbHandle>>> = OnceLock::new();
    MAP.get_or_init(|| Mutex::new(HashMap::new()))
}

pub fn register(path: &Path, state: SharedDbState) -> Result<SharedDbHandle, DbError> {
    let key = registry_key(path);
    let mut g = map()
        .lock()
        .map_err(|_| DbError::Io(std::io::Error::other("handle registry lock poisoned")))?;
    if let Some(existing) = g.get(&key) {
        let gen = existing
            .read()
            .map_err(|_| DbError::Io(std::io::Error::other("shared database lock poisoned")))?
            .generation
            .saturating_add(1);
        let mut state = state;
        state.generation = gen;
        let mut w = existing
            .write()
            .map_err(|_| DbError::Io(std::io::Error::other("shared database lock poisoned")))?;
        *w = Arc::new(state);
        return Ok(Arc::clone(existing));
    }
    let mut state = state;
    state.generation = 0;
    let arc = Arc::new(RwLock::new(Arc::new(state)));
    g.insert(key, Arc::clone(&arc));
    Ok(arc)
}

pub fn get(path: &Path) -> Option<SharedDbHandle> {
    let key = registry_key(path);
    map().lock().ok().and_then(|g| g.get(&key).cloned())
}

#[allow(dead_code)]
pub fn unregister(path: &Path) {
    let key = registry_key(path);
    if let Ok(mut g) = map().lock() {
        g.remove(&key);
    }
}
