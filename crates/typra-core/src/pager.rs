use std::collections::HashMap;
use std::io;
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::sync::Mutex;

use crate::error::DbError;
use crate::storage::Store;

pub const DEFAULT_PAGE_SIZE: u64 = 16 * 1024;

/// A simple fixed-size page cache wrapper over any [`Store`].
///
/// This is intentionally minimal (no eviction policy yet). It exists to decouple the engine’s
/// random-access reads from the OS file descriptor and provide a hook for future buffer pool work.
#[derive(Debug)]
pub struct PagedStore<S: Store> {
    inner: S,
    page_size: u64,
    // Interior mutability so we can keep the `Store` trait surface unchanged.
    cache: Arc<Mutex<HashMap<u64, Vec<u8>>>>,
}

impl<S: Store> PagedStore<S> {
    pub fn new(inner: S, page_size: u64) -> Self {
        let page_size = page_size.max(512); // basic sanity guard
        Self {
            inner,
            page_size,
            cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn into_inner(self) -> S {
        self.inner
    }

    pub fn page_size(&self) -> u64 {
        self.page_size
    }

    fn page_range_touched(&self, offset: u64, len: usize) -> RangeInclusive<u64> {
        if len == 0 {
            return 0..=0;
        }
        let start = offset / self.page_size;
        let end = offset.saturating_add(len as u64 - 1) / self.page_size;
        start..=end
    }

    fn get_page(&mut self, page_idx: u64) -> Result<Vec<u8>, DbError> {
        if let Some(hit) = self
            .cache
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&page_idx)
            .cloned()
        {
            return Ok(hit);
        }

        let len = self.inner.len()?;
        let page_start = page_idx
            .checked_mul(self.page_size)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "page offset overflow"))?;

        // Missing pages beyond EOF are never valid; fail deterministically.
        if page_start >= len {
            return Err(DbError::Io(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "read past end of store",
            )));
        }

        let to_read = (len - page_start).min(self.page_size) as usize;
        let mut page = vec![0u8; self.page_size as usize];
        self.inner.read_exact_at(page_start, &mut page[..to_read])?;

        self.cache
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(page_idx, page.clone());

        Ok(page)
    }

    fn invalidate_range(&mut self, offset: u64, len: usize) -> Result<(), DbError> {
        if len == 0 {
            return Ok(());
        }
        let pages = self.page_range_touched(offset, len);
        let mut cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
        for p in pages {
            cache.remove(&p);
        }
        Ok(())
    }

    fn clear_truncated(&mut self, new_len: u64) -> Result<(), DbError> {
        let mut cache = self.cache.lock().unwrap_or_else(|e| e.into_inner());
        let ps = self.page_size;
        cache.retain(|page_idx, _| {
            let start = page_idx.saturating_mul(ps);
            start < new_len && start.saturating_add(ps) <= new_len
        });
        Ok(())
    }
}

impl<S: Store> Store for PagedStore<S> {
    fn len(&self) -> Result<u64, DbError> {
        self.inner.len()
    }

    fn read_exact_at(&mut self, offset: u64, buf: &mut [u8]) -> Result<(), DbError> {
        let len = self.inner.len()?;
        let end = offset
            .checked_add(buf.len() as u64)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "overflow"))?;
        if end > len {
            return Err(DbError::Io(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "read past end of store",
            )));
        }

        let mut remaining = buf.len();
        let mut out_pos = 0usize;
        while remaining > 0 {
            let cur_off = offset + out_pos as u64;
            let page_idx = cur_off / self.page_size;
            let page_off = (cur_off % self.page_size) as usize;
            let take = remaining.min(self.page_size as usize - page_off);

            let page = self.get_page(page_idx)?;
            buf[out_pos..out_pos + take].copy_from_slice(&page[page_off..page_off + take]);

            out_pos += take;
            remaining -= take;
        }
        Ok(())
    }

    fn write_all_at(&mut self, offset: u64, buf: &[u8]) -> Result<(), DbError> {
        self.inner.write_all_at(offset, buf)?;
        self.invalidate_range(offset, buf.len())?;
        Ok(())
    }

    fn sync(&mut self) -> Result<(), DbError> {
        self.inner.sync()
    }

    fn truncate(&mut self, len: u64) -> Result<(), DbError> {
        self.inner.truncate(len)?;
        self.clear_truncated(len)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/unit/src_pager_tests.rs"
    ));
}
