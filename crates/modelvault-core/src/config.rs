//! Open and recovery options for [`crate::db::Database`].

/// Metadata about recovery actions applied during open.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct OpenRecoveryInfo {
    /// Bytes truncated from the file tail during open (0 if none).
    pub truncated_bytes: u64,
    /// Human-readable reason when truncation occurred.
    pub truncate_reason: Option<String>,
}

/// How to open a database when the append log tail may be torn or hold an uncommitted transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryMode {
    /// Truncate the store to the last committed prefix (see `docs/migration_0.7_to_0.8.md`).
    AutoTruncate,
    /// Return [`crate::error::FormatError::UncleanLogTail`] if the tail is not fully committed.
    Strict,
}

/// Open mode for on-disk databases.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OpenMode {
    /// Read-only handle; does not create files and does not write.
    ReadOnly,
    /// Read/write handle; creates the file if missing.
    ReadWrite,
}

/// Options for [`crate::db::Database::open_with_options`] and in-memory open helpers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpenOptions {
    pub recovery: RecoveryMode,
    pub mode: OpenMode,
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self {
            recovery: RecoveryMode::AutoTruncate,
            mode: OpenMode::ReadWrite,
        }
    }
}

/// Builder for [`OpenOptions`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpenOptionsBuilder {
    recovery: RecoveryMode,
    mode: OpenMode,
}

impl Default for OpenOptionsBuilder {
    fn default() -> Self {
        let opts = OpenOptions::default();
        Self {
            recovery: opts.recovery,
            mode: opts.mode,
        }
    }
}

impl OpenOptions {
    /// Start building open options (defaults match [`OpenOptions::default`]).
    pub fn builder() -> OpenOptionsBuilder {
        OpenOptionsBuilder::default()
    }
}

impl OpenOptionsBuilder {
    pub fn recovery(mut self, recovery: RecoveryMode) -> Self {
        self.recovery = recovery;
        self
    }

    pub fn mode(mut self, mode: OpenMode) -> Self {
        self.mode = mode;
        self
    }

    pub fn read_only(self) -> Self {
        self.mode(OpenMode::ReadOnly)
    }

    pub fn read_write(self) -> Self {
        self.mode(OpenMode::ReadWrite)
    }

    pub fn build(self) -> OpenOptions {
        OpenOptions {
            recovery: self.recovery,
            mode: self.mode,
        }
    }
}
