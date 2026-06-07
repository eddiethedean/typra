//! Shared `RwLock` handle: concurrent readers, exclusive writers.
//!
//! While a Python/Rust transaction is open (`txn_depth > 0`), all operations take a write lock
//! so reads observe the transaction's staged snapshot. A per-handle transaction token blocks
//! autocommit operations from other tasks while a transaction context manager is active.

use std::ops::{Deref, DerefMut};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use std::thread::ThreadId;

use modelvault_core::catalog::CollectionInfo;
use pyo3::exceptions::{PyRuntimeError, PyValueError};
use pyo3::prelude::*;

use crate::errors::db_error_to_py;
use crate::inner_db::InnerDb;

fn lock_err<T>(e: std::sync::PoisonError<T>) -> PyErr {
    PyRuntimeError::new_err(format!("database lock poisoned: {e}"))
}

const TXN_BLOCK_MSG: &str = "operation blocked: a transaction is open on this database handle; \
     finish or roll back the transaction first";

static NEXT_TXN_TOKEN: AtomicU64 = AtomicU64::new(1);

/// Held for the lifetime of an open ``transaction()`` / ``async with db.transaction()`` block.
pub(crate) struct TxnGateLease {
    _token: u64,
    active_txn_token: Arc<AtomicU64>,
    open_txn_token: Arc<Mutex<Option<u64>>>,
    txn_owner_thread: Arc<Mutex<Option<ThreadId>>>,
    txn_is_async: Arc<AtomicBool>,
}

impl TxnGateLease {
    pub fn acquire(handle: &DbHandle, is_async: bool) -> Self {
        let token = NEXT_TXN_TOKEN.fetch_add(1, Ordering::Relaxed);
        let owner = std::thread::current().id();
        handle.active_txn_token.store(token, Ordering::Release);
        if let Ok(mut g) = handle.open_txn_token.lock() {
            *g = Some(token);
        }
        if let Ok(mut g) = handle.txn_owner_thread.lock() {
            *g = Some(owner);
        }
        handle.txn_is_async.store(is_async, Ordering::Release);
        Self {
            _token: token,
            active_txn_token: Arc::clone(&handle.active_txn_token),
            open_txn_token: Arc::clone(&handle.open_txn_token),
            txn_owner_thread: Arc::clone(&handle.txn_owner_thread),
            txn_is_async: Arc::clone(&handle.txn_is_async),
        }
    }
}

impl Drop for TxnGateLease {
    fn drop(&mut self) {
        self.active_txn_token.store(0, Ordering::Release);
        self.txn_is_async.store(false, Ordering::Release);
        if let Ok(mut g) = self.open_txn_token.lock() {
            *g = None;
        }
        if let Ok(mut g) = self.txn_owner_thread.lock() {
            *g = None;
        }
    }
}

/// Engine state behind sync `Database` and `AsyncDatabase`.
pub(crate) struct DbHandle {
    inner: RwLock<InnerDb>,
    txn_depth: AtomicUsize,
    /// Non-zero while a transaction context manager is active on this handle.
    active_txn_token: Arc<AtomicU64>,
    /// Token presented by the active transaction (mirrors `active_txn_token` for cheap reads).
    pub(crate) open_txn_token: Arc<Mutex<Option<u64>>>,
    /// Thread that entered the active transaction context (sync isolation).
    pub(crate) txn_owner_thread: Arc<Mutex<Option<ThreadId>>>,
    pub(crate) txn_is_async: Arc<AtomicBool>,
}

impl DbHandle {
    pub(crate) fn new(inner: InnerDb) -> Self {
        Self {
            inner: RwLock::new(inner),
            txn_depth: AtomicUsize::new(0),
            active_txn_token: Arc::new(AtomicU64::new(0)),
            open_txn_token: Arc::new(Mutex::new(None)),
            txn_owner_thread: Arc::new(Mutex::new(None)),
            txn_is_async: Arc::new(AtomicBool::new(false)),
        }
    }

    pub(crate) fn txn_enter(&self) {
        self.txn_depth.fetch_add(1, Ordering::Release);
    }

    pub(crate) fn txn_exit(&self) {
        let prev = self.txn_depth.load(Ordering::Acquire);
        if prev == 0 {
            return;
        }
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

    fn ensure_not_blocked_by_foreign_txn(&self) -> PyResult<()> {
        if self.txn_depth.load(Ordering::Acquire) == 0 {
            return Ok(());
        }
        let current = std::thread::current().id();
        let owner = self
            .txn_owner_thread
            .lock()
            .map_err(|_| PyRuntimeError::new_err("transaction owner lock poisoned"))?
            .unwrap_or(current);
        if owner == current {
            return Ok(());
        }
        if self.txn_is_async.load(Ordering::Acquire)
            && read_open_txn_token(&self.open_txn_token).is_some()
        {
            return Ok(());
        }
        Err(PyRuntimeError::new_err(TXN_BLOCK_MSG))
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
    handle.ensure_not_blocked_by_foreign_txn()?;
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
    handle.ensure_not_blocked_by_foreign_txn()?;
    handle.inner.write().map_err(lock_err)
}

pub(crate) fn collection_info(handle: &DbHandle, name: &str) -> PyResult<CollectionInfo> {
    let g = lock_inner_read(handle)?;
    let cid = g.collection_id_named(name).map_err(db_error_to_py)?;
    let catalog = g.catalog();
    catalog
        .get(cid)
        .cloned()
        .ok_or_else(|| PyValueError::new_err("collection missing after resolve"))
}

pub(crate) fn read_open_txn_token(open_txn_token: &Arc<Mutex<Option<u64>>>) -> Option<u64> {
    open_txn_token.lock().ok().and_then(|g| *g)
}
