//! Open and recovery options for [`crate::db::Database`].

/// How to open a database when the append log tail may be torn or hold an uncommitted transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RecoveryMode {
    /// Truncate the store to the last committed prefix (see `docs/migration_0.7_to_0.8.md`).
    AutoTruncate,
    /// Return [`crate::error::FormatError::UncleanLogTail`] if the tail is not fully committed.
    Strict,
}

/// Options for [`crate::db::Database::open_with_options`] and in-memory open helpers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OpenOptions {
    pub recovery: RecoveryMode,
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self {
            recovery: RecoveryMode::AutoTruncate,
        }
    }
}
