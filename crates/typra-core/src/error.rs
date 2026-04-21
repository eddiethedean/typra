use std::fmt;

/// Errors returned by the database engine and I/O around database files.
#[derive(Debug)]
pub enum DbError {
    /// Failed to access the database file or path.
    Io(std::io::Error),
    /// Feature not yet implemented (reserved for early releases).
    NotImplemented,
}

impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DbError::Io(e) => write!(f, "i/o error: {e}"),
            DbError::NotImplemented => write!(f, "not implemented"),
        }
    }
}

impl std::error::Error for DbError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            DbError::Io(e) => Some(e),
            DbError::NotImplemented => None,
        }
    }
}

impl From<std::io::Error> for DbError {
    fn from(value: std::io::Error) -> Self {
        DbError::Io(value)
    }
}
