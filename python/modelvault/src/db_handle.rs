//! Shared `RwLock` handle: concurrent readers, exclusive writers.
//!
//! While a Python/Rust transaction is open (`txn_depth > 0`), all operations take a write lock
//! so reads observe the transaction's staged snapshot.

use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, RwLock, RwLockReadGuard, RwLockWriteGuard};

use modelvault_core::catalog::CollectionInfo;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;

use crate::errors::db_error_to_py;
use crate::inner_db::InnerDb;

fn lock_err<T>(e: std::sync::PoisonError<T>) -> PyErr {
    PyRuntimeError::new_err(format!("database lock poisoned: {e}"))
}

/// Engine state behind sync `Database` and `AsyncDatabase`.
pub(crate) struct DbHandle {
    inner: RwLock<InnerDb>,
    txn_depth: AtomicUsize,
}

impl DbHandle {
    pub(crate) fn new(inner: InnerDb) -> Self {
        Self {
            inner: RwLock::new(inner),
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

    pub(crate) fn txn_depth_active(&self) -> bool {
        self.txn_depth.load(Ordering::Acquire) > 0
    }

    /// Decrement txn depth when [`Self::txn_enter`] ran and exit did not run yet.
    pub(crate) fn txn_exit_if_active(&self, active: bool) {
        if active {
            self.txn_exit();
        }
    }
}

/// Finish a transaction: rollback on Python exception; on commit failure rollback then propagate.
pub(crate) fn finish_transaction(
    db: &mut InnerDb,
    had_exception: bool,
) -> Result<(), modelvault_core::DbError> {
    if had_exception {
        db.rollback_transaction();
        Ok(())
    } else {
        match db.commit_transaction() {
            Ok(()) => Ok(()),
            Err(e) => {
                db.rollback_transaction();
                Err(e)
            }
        }
    }
}

pub(crate) type SharedDbHandle = Arc<DbHandle>;

/// Read or write guard over [`InnerDb`].
pub(crate) enum InnerGuard<'a> {
    Read(RwLockReadGuard<'a, InnerDb>),
    Write(RwLockWriteGuard<'a, InnerDb>),
}

impl Deref for InnerGuard<'_> {
    type Target = InnerDb;

    fn deref(&self) -> &InnerDb {
        match self {
            InnerGuard::Read(g) => g,
            InnerGuard::Write(g) => g,
        }
    }
}

impl DerefMut for InnerGuard<'_> {
    fn deref_mut(&mut self) -> &mut InnerDb {
        match self {
            InnerGuard::Write(g) => g,
            InnerGuard::Read(_) => panic!("InnerGuard::Read cannot deref_mut"),
        }
    }
}

pub(crate) fn lock_inner_read<'a>(handle: &'a DbHandle) -> PyResult<InnerGuard<'a>> {
    if handle.txn_depth.load(Ordering::Acquire) > 0 {
        let g = handle.inner.write().map_err(lock_err)?;
        Ok(InnerGuard::Write(g))
    } else {
        let g = handle.inner.read().map_err(lock_err)?;
        Ok(InnerGuard::Read(g))
    }
}

pub(crate) fn lock_inner_write<'a>(
    handle: &'a DbHandle,
) -> PyResult<RwLockWriteGuard<'a, InnerDb>> {
    handle.inner.write().map_err(lock_err)
}

pub(crate) fn collection_info(handle: &DbHandle, name: &str) -> PyResult<CollectionInfo> {
    let g = lock_inner_read(handle)?;
    let cid = g.collection_id_named(name).map_err(db_error_to_py)?;
    g.catalog()
        .get(cid)
        .cloned()
        .ok_or_else(|| PyValueError::new_err("collection missing after resolve"))
}
