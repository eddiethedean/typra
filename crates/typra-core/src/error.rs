use std::fmt;

/// Top-level error for [`crate::db::Database`] and storage: I/O, on-disk layout, or schema rules.
///
/// Convert from [`std::io::Error`] via `?` for convenience on file operations.
#[derive(Debug)]
pub enum DbError {
    /// Failed to access the database file or path.
    Io(std::io::Error),
    /// Failed to parse or validate the on-disk format (header, superblock, segments, payloads).
    Format(FormatError),
    /// Catalog or row did not satisfy schema invariants.
    Schema(SchemaError),
    /// Requested capability is not implemented in this release (e.g. nested field paths in rows).
    NotImplemented,
}

/// Low-level decode/validation failures for bytes read from the store.
#[derive(Debug)]
pub enum FormatError {
    /// File magic was not `TDB0`.
    BadMagic { got: [u8; 4] },
    /// Fewer bytes than expected for a fixed-size header region.
    TruncatedHeader { got: usize, expected: usize },
    /// Header or manifest reported an unsupported format or manifest version.
    UnsupportedVersion { major: u16, minor: u16 },
    /// Superblock slice shorter than [`crate::superblock::SUPERBLOCK_SIZE`].
    TruncatedSuperblock { got: usize, expected: usize },
    /// Superblock magic was not `TSB0`.
    BadSuperblockMagic { got: [u8; 4] },
    /// Superblock CRC did not match payload.
    BadSuperblockChecksum,
    /// Segment header slice shorter than expected.
    TruncatedSegmentHeader { got: usize, expected: usize },
    /// Segment header magic was not `TSG0`.
    BadSegmentMagic { got: [u8; 4] },
    /// Header CRC32C did not match header bytes.
    BadSegmentHeaderChecksum,
    /// Payload CRC32C did not match segment body.
    BadSegmentPayloadChecksum,
    /// Declared payload length would extend past the file end.
    SegmentPayloadPastEof,
    /// Invalid catalog segment payload (binary layout).
    InvalidCatalogPayload { message: String },
    /// Record segment payload truncated or malformed.
    TruncatedRecordPayload,
    /// Record payload type tag did not match schema.
    RecordPayloadTypeMismatch,
    /// UTF-8 in a record string field was invalid.
    InvalidRecordUtf8,
    /// Record payload used a composite type not supported in v1 row encoding.
    RecordPayloadUnsupportedType,
    /// Record payload version not supported.
    UnknownRecordPayloadVersion { got: u16 },
    /// Extra bytes after a decoded record payload.
    TrailingRecordPayload,
}

/// Schema and row-level validation errors (catalog replay, registration, insert/get).
#[derive(Debug, Clone)]
pub enum SchemaError {
    /// Field path had no segments or an empty segment.
    InvalidFieldPath,
    /// Another collection already uses this name.
    DuplicateCollectionName {
        name: String,
    },
    /// No collection registered with this id.
    UnknownCollection {
        id: u32,
    },
    /// No collection registered under this name.
    UnknownCollectionName {
        name: String,
    },
    InvalidCollectionName,
    InvalidSchemaVersion {
        expected: u32,
        got: u32,
    },
    /// `u32` schema version counter cannot be incremented further.
    SchemaVersionExhausted,
    UnexpectedCollectionId {
        expected: u32,
        got: u32,
    },
    /// Collection was created without a primary key (catalog v1); inserts are not supported.
    NoPrimaryKey {
        collection_id: u32,
    },
    /// Declared primary field is not a single top-level segment or not present in fields.
    PrimaryFieldNotFound {
        name: String,
    },
    /// New schema version drops or renames the primary-key field.
    PrimaryFieldMissingInSchema {
        name: String,
    },
    /// Insert row did not include the primary key field.
    RowMissingPrimary {
        name: String,
    },
    /// Insert row referenced an unknown field name.
    RowUnknownField {
        name: String,
    },
    /// Insert row omitted a non-primary field.
    RowMissingField {
        name: String,
    },
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
            FormatError::TruncatedRecordPayload => write!(f, "truncated record payload"),
            FormatError::RecordPayloadTypeMismatch => {
                write!(f, "record payload type does not match schema")
            }
            FormatError::InvalidRecordUtf8 => write!(f, "invalid UTF-8 in record string"),
            FormatError::RecordPayloadUnsupportedType => {
                write!(f, "unsupported type in record payload v1")
            }
            FormatError::UnknownRecordPayloadVersion { got } => {
                write!(f, "unknown record payload version {got}")
            }
            FormatError::TrailingRecordPayload => write!(f, "trailing bytes in record payload"),
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
            SchemaError::UnknownCollectionName { name } => {
                write!(f, "unknown collection name {name:?}")
            }
            SchemaError::InvalidCollectionName => write!(f, "invalid collection name"),
            SchemaError::InvalidSchemaVersion { expected, got } => {
                write!(f, "invalid schema version: expected {expected}, got {got}")
            }
            SchemaError::SchemaVersionExhausted => {
                write!(f, "schema version limit reached (cannot bump further)")
            }
            SchemaError::UnexpectedCollectionId { expected, got } => {
                write!(
                    f,
                    "unexpected collection id in catalog replay: expected {expected}, got {got}"
                )
            }
            SchemaError::NoPrimaryKey { collection_id } => {
                write!(
                    f,
                    "collection {collection_id} has no primary key (upgrade catalog or re-register)"
                )
            }
            SchemaError::PrimaryFieldNotFound { name } => {
                write!(f, "primary field {name:?} not found as a top-level field")
            }
            SchemaError::PrimaryFieldMissingInSchema { name } => {
                write!(
                    f,
                    "schema update must retain top-level primary field {name:?}"
                )
            }
            SchemaError::RowMissingPrimary { name } => {
                write!(f, "insert row missing primary key field {name:?}")
            }
            SchemaError::RowUnknownField { name } => {
                write!(f, "insert row has unknown field {name:?}")
            }
            SchemaError::RowMissingField { name } => {
                write!(f, "insert row missing field {name:?}")
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
