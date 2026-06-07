//! Checkpoint, compaction, and snapshot export/restore.

use std::path::Path;

use crate::config::OpenMode;
use crate::error::{DbError, SchemaError};
use crate::schema::CollectionId;
use crate::segments::header::{SegmentHeader, SegmentType, SEGMENT_HEADER_LEN};
use crate::segments::writer::SegmentWriter;
use crate::storage::{FileStore, Store, VecStore};
use crate::{checkpoint, publish};

use super::fs_ops::{FsOps, StdFsOps};
use super::{handle_registry, Database};

/// Best-effort `fsync` on `dest_path`'s parent directory (Unix only).
#[cfg(unix)]
pub(crate) fn best_effort_fsync_parent_dir(fs: &dyn FsOps, dest_path: &Path) {
    let Some(parent) = dest_path.parent() else {
        return;
    };
    let Ok(dir_f) = fs.open_dir(parent) else {
        return;
    };
    let _ = dir_f.sync_all();
}

impl<S: Store> Database<S> {
    /// Write a durable checkpoint segment and publish it via the superblock.
    ///
    /// The checkpoint stores the logical state (catalog + latest rows + index state) so open can
    /// avoid scanning/replaying the full log. Works with any [`Store`] (file-backed [`FileStore`] or
    /// [`VecStore`] snapshots).
    pub fn checkpoint(&mut self) -> Result<(), DbError> {
        #[cfg(feature = "tracing")]
        let _span = tracing::info_span!("database_checkpoint").entered();
        if self.txn_staging.is_some() {
            return Err(DbError::Transaction(
                crate::error::TransactionError::NestedTransaction,
            ));
        }

        super::segment_write::ensure_header_v0_6(&mut self.store, &mut self.format_minor)?;

        let mut cp = checkpoint::checkpoint_from_state(
            self.catalog_for_read(),
            self.latest_for_read(),
            self.indexes_for_read(),
        )?;

        let file_len = self.store.len()?;
        let mut writer = SegmentWriter::new(&mut self.store, file_len.max(self.segment_start));
        let checkpoint_offset = writer.offset();

        let payload_len = checkpoint::encode_checkpoint_payload_v0(&cp).len() as u64;
        let replay_from = checkpoint_offset + SEGMENT_HEADER_LEN as u64 + payload_len;
        cp.replay_from_offset = replay_from;
        let payload = checkpoint::encode_checkpoint_payload_v0(&cp);

        let hdr = SegmentHeader {
            segment_type: SegmentType::Checkpoint,
            payload_len: 0,
            payload_crc32c: 0,
        };
        writer.append(hdr, &payload)?;

        publish::append_manifest_and_publish_with_checkpoint(
            &mut self.store,
            self.segment_start,
            Some((checkpoint_offset, payload.len() as u32)),
        )?;
        self.store.sync()?;
        #[cfg(feature = "tracing")]
        tracing::info!(
            checkpoint_offset,
            replay_from,
            payload_bytes = payload.len(),
            "database_checkpoint_ok"
        );
        Ok(())
    }

    pub(crate) fn compact_snapshot_bytes(&self) -> Result<Vec<u8>, DbError> {
        let mut out = Database::<VecStore>::open_in_memory()?;

        // Recreate catalog (stable ids if created in id order).
        let mut cols = self.catalog_for_read().collections();
        cols.sort_by_key(|c| c.id.0);
        for c in &cols {
            let pk =
                c.primary_field
                    .as_deref()
                    .ok_or(DbError::Schema(SchemaError::NoPrimaryKey {
                        collection_id: c.id.0,
                    }))?;
            let (new_id, _v1) = out.register_collection_with_indexes(
                &c.name,
                c.fields.clone(),
                c.indexes.clone(),
                pk,
            )?;
            // Bump schema version counter to match current_version (repeat identical schema).
            for _ in 2..=c.current_version.0 {
                let _ = out.register_schema_version_with_indexes_force(
                    new_id,
                    c.fields.clone(),
                    c.indexes.clone(),
                )?;
            }
        }

        // Copy latest rows (in-memory snapshot semantics).
        for ((cid, _), row) in self.latest_for_read().iter() {
            let collection_id = CollectionId(*cid);
            out.insert(collection_id, row.clone())?;
        }

        Ok(out.into_snapshot_bytes())
    }
}

impl Database<FileStore> {
    /// Rewrite the database into a compacted single-file image at `dest_path`.
    ///
    /// The destination file is truncated/overwritten if it exists.
    pub fn compact_to(&self, dest_path: impl AsRef<Path>) -> Result<(), DbError> {
        self.compact_to_with_fsops(&StdFsOps, dest_path)
    }

    pub(crate) fn compact_to_with_fsops(
        &self,
        fs: &dyn FsOps,
        dest_path: impl AsRef<Path>,
    ) -> Result<(), DbError> {
        #[cfg(feature = "tracing")]
        let _span = tracing::info_span!(
            "database_compact_to",
            dest = %dest_path.as_ref().display()
        )
        .entered();
        let bytes = self.compact_snapshot_bytes()?;
        let path = dest_path.as_ref();
        let file = fs
            .open_read_write_create_truncate(path)
            .map_err(DbError::Io)?;
        let mut store = FileStore::new(file);
        store.write_all_at(0, &bytes)?;
        store.truncate(bytes.len() as u64)?;
        store.sync()?;
        #[cfg(feature = "tracing")]
        tracing::info!(bytes = bytes.len(), "database_compact_to_ok");
        Ok(())
    }

    pub fn compact_in_place(&mut self) -> Result<(), DbError> {
        self.compact_in_place_with_fsops(&StdFsOps)
    }

    pub(crate) fn compact_in_place_with_fsops(&mut self, fs: &dyn FsOps) -> Result<(), DbError> {
        #[cfg(feature = "tracing")]
        let _span = tracing::info_span!("database_compact_in_place").entered();
        // Crash-safety: write a full new image to a sidecar file, fsync it, then atomically
        // replace the live path via rename (using a backup on platforms where rename does not
        // overwrite an existing destination).
        let bytes = self.compact_snapshot_bytes()?;
        let live_path = self.path.clone();
        let parent = live_path
            .parent()
            .ok_or_else(|| DbError::Io(std::io::Error::other("no parent")))?;

        // Pick unique temp + backup names in the same directory (so rename stays atomic on POSIX).
        let pid = std::process::id();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let base = live_path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("db.modelvault");
        let tmp_path = parent.join(format!("{base}.compact.{pid}.{nanos}.tmp"));
        let bak_path = parent.join(format!("{base}.compact.{pid}.{nanos}.bak"));

        // 1) Write the compacted image to tmp and fsync it.
        {
            let file = fs
                .open_read_write_create_new(&tmp_path)
                .map_err(DbError::Io)?;
            let mut store = FileStore::new(file);
            store.write_all_at(0, &bytes)?;
            store.truncate(bytes.len() as u64)?;
            store.sync()?;
        }

        // 2) Replace the live file path with the tmp image, preserving a backup until success.
        //
        // We do not rely on "rename over existing" being supported across platforms. Instead:
        // - move live → bak
        // - move tmp → live
        // - fsync directory (best-effort)
        // - remove bak
        //
        // If tmp → live fails, attempt to restore bak → live.
        let _ = fs.remove_file(&bak_path);
        fs.rename(&live_path, &bak_path).map_err(DbError::Io)?;
        let replace_res = fs.rename(&tmp_path, &live_path);
        if let Err(e) = replace_res {
            // Best-effort restore: move backup back into place.
            let _ = fs.rename(&bak_path, &live_path);
            // Clean up tmp if it still exists.
            let _ = fs.remove_file(&tmp_path);
            return Err(DbError::Io(e));
        }

        // Best-effort directory sync: helps make the rename durable on POSIX.
        #[cfg(unix)]
        {
            // Best-effort: on many Unix platforms, opening a directory and syncing it will persist
            // the rename in the directory entry. If this fails, the data file itself is still
            // fsync'd and the operation remains logically correct; only rename durability is weaker.
            if let Ok(dir_f) = fs.open_dir(parent) {
                let _ = dir_f.sync_all();
            }
        }

        let _ = fs.remove_file(&bak_path);

        // 3) Refresh in-memory state by reopening. Keep writer-registry registration for the
        // whole operation so another writable handle cannot open the same path mid-reopen.
        let old_registry = self.writer_registry.take();
        self.store.release_writer_lock();
        let reopened = match (|| {
            let store = FileStore::open_locked(&live_path, OpenMode::ReadWrite)?;
            Self::open_with_store(
                live_path.clone(),
                store,
                crate::config::OpenOptions::default(),
            )
        })() {
            Ok(db) => db,
            Err(e) => {
                let _ = fs.rename(&bak_path, &live_path);
                if let Ok(store) = FileStore::open_locked(&live_path, OpenMode::ReadWrite) {
                    self.store = store;
                }
                self.writer_registry = old_registry;
                return Err(e);
            }
        };
        let mut reopened = reopened;
        reopened.writer_registry = old_registry;
        *self = reopened;
        self.shared_mirror = Some(handle_registry::register(
            &live_path,
            handle_registry::SharedDbState {
                catalog: self.catalog.clone(),
                latest: self.latest.clone(),
                indexes: self.indexes.clone(),
                segment_start: self.segment_start,
                format_minor: self.format_minor,
                generation: 0,
            },
        )?);
        self.push_shared_mirror();
        #[cfg(feature = "tracing")]
        tracing::info!(bytes = bytes.len(), "database_compact_in_place_ok");
        Ok(())
    }

    /// Create a consistent backup copy of this on-disk database.
    ///
    /// This writes a checkpoint (for fast reopen and a stable state marker) and then copies the
    /// underlying file bytes to `dest_path`.
    pub fn export_snapshot_to_path(&mut self, dest_path: impl AsRef<Path>) -> Result<(), DbError> {
        self.export_snapshot_to_path_with_fsops(&StdFsOps, dest_path)
    }

    pub(crate) fn export_snapshot_to_path_with_fsops(
        &mut self,
        fs: &dyn FsOps,
        dest_path: impl AsRef<Path>,
    ) -> Result<(), DbError> {
        self.checkpoint()?;
        let dest_path = dest_path.as_ref();
        // Read through the open store handle so export works while the writer lock is held
        // (Windows rejects `fs::copy` on exclusively locked files).
        let len = self.store.len()?;
        let len_usize = usize::try_from(len)
            .map_err(|_| DbError::Io(std::io::Error::other("database file too large")))?;
        let mut bytes = vec![0u8; len_usize];
        self.store.read_exact_at(0, &mut bytes)?;
        Database::<VecStore>::export_snapshot_to_path_with_fsops(fs, dest_path, &bytes)?;
        // Strengthen durability of the copied snapshot: fsync the destination and best-effort
        // fsync its parent directory so the directory entry is persisted.
        if let Ok(f) = fs.open_read(dest_path) {
            let _ = f.sync_all();
        }
        #[cfg(unix)]
        best_effort_fsync_parent_dir(fs, dest_path);
        Ok(())
    }

    /// Restore a snapshot file into `dest_path` by atomically replacing the destination.
    ///
    /// This is a file operation helper intended for operational tooling.
    pub fn restore_snapshot_to_path(
        snapshot_path: impl AsRef<Path>,
        dest_path: impl AsRef<Path>,
    ) -> Result<(), DbError> {
        Self::restore_snapshot_to_path_with_fsops(&StdFsOps, snapshot_path, dest_path)
    }

    pub(crate) fn restore_snapshot_to_path_with_fsops(
        fs: &dyn FsOps,
        snapshot_path: impl AsRef<Path>,
        dest_path: impl AsRef<Path>,
    ) -> Result<(), DbError> {
        let snapshot_path = snapshot_path.as_ref();
        let dest_path = dest_path.as_ref();
        let parent = dest_path
            .parent()
            .ok_or_else(|| DbError::Io(std::io::Error::other("no parent")))?;

        let pid = std::process::id();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let base = dest_path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("db.modelvault");
        let tmp_path = parent.join(format!("{base}.restore.{pid}.{nanos}.tmp"));
        let bak_path = parent.join(format!("{base}.restore.{pid}.{nanos}.bak"));

        // Copy snapshot bytes into a temp file and fsync it.
        fs.copy(snapshot_path, &tmp_path).map_err(DbError::Io)?;
        if let Ok(f) = fs.open_read(&tmp_path) {
            let _ = f.sync_all();
        }

        // Replace destination with backup/restore semantics.
        if dest_path.exists() {
            let _ = fs.remove_file(&bak_path);
            fs.rename(dest_path, &bak_path).map_err(DbError::Io)?;
        }
        let replace_res = fs.rename(&tmp_path, dest_path);
        if let Err(e) = replace_res {
            // Best-effort restore original.
            if bak_path.exists() {
                let _ = fs.rename(&bak_path, dest_path);
            }
            let _ = fs.remove_file(&tmp_path);
            return Err(DbError::Io(e));
        }

        #[cfg(unix)]
        {
            if let Ok(dir_f) = fs.open_dir(parent) {
                let _ = dir_f.sync_all();
            }
        }
        let _ = fs.remove_file(&bak_path);
        Ok(())
    }

    /// Read the on-disk image via the open store (integration tests).
    ///
    /// On Windows, `std::fs::read` fails while the writer holds an exclusive file lock.
    #[doc(hidden)]
    pub fn read_image_for_test(&mut self) -> Result<Vec<u8>, DbError> {
        let len = self.store.len()?;
        let len_usize = usize::try_from(len)
            .map_err(|_| DbError::Io(std::io::Error::other("database file too large")))?;
        let mut bytes = vec![0u8; len_usize];
        self.store.read_exact_at(0, &mut bytes)?;
        Ok(bytes)
    }
}
