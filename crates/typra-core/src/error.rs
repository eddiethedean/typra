use std::fmt;

/// Errors returned by the database engine and I/O around database files.
#[derive(Debug)]
pub enum DbError {
    /// Failed to access the database file or path.
    Io(std::io::Error),
    /// Failed to parse/validate the on-disk format.
    Format(FormatError),
    /// Failed to build or validate schema metadata.
    Schema(SchemaError),
    /// Feature not yet implemented (reserved for early releases).
    NotImplemented,
}

#[derive(Debug)]
pub enum FormatError {
    BadMagic {
        got: [u8; 4],
    },
    TruncatedHeader {
        got: usize,
        expected: usize,
    },
    UnsupportedVersion {
        major: u16,
        minor: u16,
    },
    TruncatedSuperblock {
        got: usize,
        expected: usize,
    },
    BadSuperblockMagic {
        got: [u8; 4],
    },
    BadSuperblockChecksum,
    TruncatedSegmentHeader {
        got: usize,
        expected: usize,
    },
    BadSegmentMagic {
        got: [u8; 4],
    },
    BadSegmentHeaderChecksum,
    BadSegmentPayloadChecksum,
    SegmentPayloadPastEof,
    /// Invalid catalog segment payload (binary layout).
    InvalidCatalogPayload {
        message: String,
    },
}

#[derive(Debug)]
pub enum SchemaError {
    InvalidFieldPath,
    DuplicateCollectionName { name: String },
    UnknownCollection { id: u32 },
    InvalidCollectionName,
    InvalidSchemaVersion { expected: u32, got: u32 },
    UnexpectedCollectionId { expected: u32, got: u32 },
}

impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DbError::Io(e) => write!(f, "i/o error: {e}"),
            DbError::Format(e) => write!(f, "format error: {e}"),
            DbError::Schema(e) => write!(f, "schema error: {e}"),
            DbError::NotImplemented => write!(f, "not implemented"),
        }
    }
}

impl fmt::Display for FormatError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FormatError::BadMagic { got } => {
                write!(f, "bad magic bytes: expected \"TDB0\", got {:02x?}", got)
            }
            FormatError::TruncatedHeader { got, expected } => {
                write!(f, "truncated header: got {got} bytes, expected {expected}")
            }
            FormatError::UnsupportedVersion { major, minor } => {
                write!(f, "unsupported format version {major}.{minor}")
            }
            FormatError::TruncatedSuperblock { got, expected } => {
                write!(
                    f,
                    "truncated superblock: got {got} bytes, expected {expected}"
                )
            }
            FormatError::BadSuperblockMagic { got } => {
                write!(
                    f,
                    "bad superblock magic bytes: expected \"TSB0\", got {:02x?}",
                    got
                )
            }
            FormatError::BadSuperblockChecksum => write!(f, "superblock checksum mismatch"),
            FormatError::TruncatedSegmentHeader { got, expected } => {
                write!(
                    f,
                    "truncated segment header: got {got} bytes, expected {expected}"
                )
            }
            FormatError::BadSegmentMagic { got } => {
                write!(
                    f,
                    "bad segment magic bytes: expected \"TSG0\", got {:02x?}",
                    got
                )
            }
            FormatError::BadSegmentHeaderChecksum => write!(f, "segment header checksum mismatch"),
            FormatError::BadSegmentPayloadChecksum => {
                write!(f, "segment payload checksum mismatch")
            }
            FormatError::SegmentPayloadPastEof => {
                write!(f, "segment payload extends past end of file")
            }
            FormatError::InvalidCatalogPayload { message } => {
                write!(f, "invalid catalog payload: {message}")
            }
        }
    }
}

impl fmt::Display for SchemaError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SchemaError::InvalidFieldPath => write!(f, "invalid field path"),
            SchemaError::DuplicateCollectionName { name } => {
                write!(f, "duplicate collection name: {name:?}")
            }
            SchemaError::UnknownCollection { id } => {
                write!(f, "unknown collection id {id}")
            }
            SchemaError::InvalidCollectionName => write!(f, "invalid collection name"),
            SchemaError::InvalidSchemaVersion { expected, got } => {
                write!(f, "invalid schema version: expected {expected}, got {got}")
            }
            SchemaError::UnexpectedCollectionId { expected, got } => {
                write!(
                    f,
                    "unexpected collection id in catalog replay: expected {expected}, got {got}"
                )
            }
        }
    }
}

impl std::error::Error for DbError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            DbError::Io(e) => Some(e),
            DbError::Format(_) => None,
            DbError::Schema(_) => None,
            DbError::NotImplemented => None,
        }
    }
}

impl From<std::io::Error> for DbError {
    fn from(value: std::io::Error) -> Self {
        DbError::Io(value)
    }
}
