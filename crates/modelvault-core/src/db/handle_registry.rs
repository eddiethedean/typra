//! Process-wide shared database state for same-process read-only views.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock, RwLock};

use crate::catalog::Catalog;
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
}

fn map() -> &'static Mutex<HashMap<PathBuf, Arc<RwLock<SharedDbState>>>> {
    static MAP: OnceLock<Mutex<HashMap<PathBuf, Arc<RwLock<SharedDbState>>>>> = OnceLock::new();
    MAP.get_or_init(|| Mutex::new(HashMap::new()))
}

pub fn register(path: &Path, state: SharedDbState) -> Arc<RwLock<SharedDbState>> {
    let key = path.to_path_buf();
    let arc = Arc::new(RwLock::new(state));
    if let Ok(mut g) = map().lock() {
        g.insert(key, Arc::clone(&arc));
    }
    arc
}

pub fn get(path: &Path) -> Option<Arc<RwLock<SharedDbState>>> {
    map().lock().ok().and_then(|g| g.get(path).cloned())
}

pub fn unregister(path: &Path) {
    if let Ok(mut g) = map().lock() {
        g.remove(path);
    }
}
