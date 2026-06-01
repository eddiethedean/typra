//! `RwLock` over [`modelvault_core::Database`]: concurrent readers, exclusive writers.

use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use modelvault_core::storage::Store;
use modelvault_core::{Database, DbError};

fn map_poisoned() -> DbError {
    DbError::Io(std::io::Error::other("modelvault database lock poisoned"))
}

pub(crate) struct DbState<S: Store> {
    db: RwLock<Database<S>>,
    txn_depth: AtomicUsize,
}

impl<S: Store> DbState<S> {
    pub(crate) fn new(db: Database<S>) -> Self {
        Self {
            db: RwLock::new(db),
            txn_depth: AtomicUsize::new(0),
        }
    }

    pub(crate) fn txn_enter(&self) {
        self.txn_depth.fetch_add(1, Ordering::Release);
    }

    pub(crate) fn txn_exit(&self) {
        debug_assert!(self.txn_depth.load(Ordering::Acquire) > 0);
        self.txn_depth.fetch_sub(1, Ordering::Release);
    }
}

pub(crate) type SharedDbState<S> = Arc<DbState<S>>;

pub(crate) enum DbGuard<'a, S: Store> {
    Read(RwLockReadGuard<'a, Database<S>>),
    Write(RwLockWriteGuard<'a, Database<S>>),
}

impl<S: Store> Deref for DbGuard<'_, S> {
    type Target = Database<S>;

    fn deref(&self) -> &Database<S> {
        match self {
            DbGuard::Read(g) => g,
            DbGuard::Write(g) => g,
        }
    }
}

impl<S: Store> DerefMut for DbGuard<'_, S> {
    fn deref_mut(&mut self) -> &mut Database<S> {
        match self {
            DbGuard::Write(g) => g,
            DbGuard::Read(_) => panic!("DbGuard::Read cannot deref_mut"),
        }
    }
}

pub(crate) fn read_db<S: Store>(state: &DbState<S>) -> Result<DbGuard<'_, S>, DbError> {
    if state.txn_depth.load(Ordering::Acquire) > 0 {
        Ok(DbGuard::Write(
            state.db.write().map_err(|_| map_poisoned())?,
        ))
    } else {
        Ok(DbGuard::Read(state.db.read().map_err(|_| map_poisoned())?))
    }
}

pub(crate) fn write_db<S: Store>(
    state: &DbState<S>,
) -> Result<RwLockWriteGuard<'_, Database<S>>, DbError> {
    state.db.write().map_err(|_| map_poisoned())
}
