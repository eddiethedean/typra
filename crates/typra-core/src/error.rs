use std::fmt;

/// Top-level error for [`crate::db::Database`] and storage: I/O, on-disk layout, or schema rules.
///
/// Convert from [`std::io::Error`] via `?` for convenience on file operations.
/// Structured validation failure (0.6+): nested path and human-readable detail.
#[derive(Debug, Clone)]
pub struct ValidationError {
    pub path: Vec<String>,
    pub message: String,
}

#[derive(Debug)]
pub enum DbError {
    /// Failed to access the database file or path.
    Io(std::io::Error),
    /// Failed to parse or validate the on-disk format (header, superblock, segments, payloads).
    Format(FormatError),
    /// Catalog or row did not satisfy schema invariants.
    Schema(SchemaError),
    /// Row value failed type or constraint checks before persistence.
    Validation(ValidationError),
    /// Transaction nesting or API misuse (0.8+).
    Transaction(TransactionError),
    /// Query construction, parsing, or execution error (SQL adapter and query planner).
    Query(QueryError),
    /// Requested capability is not implemented in this release (e.g. nested field paths in rows).
    NotImplemented,
}

/// Stable classification of core errors (suitable for matching in higher-level bindings).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DbErrorKind {
    Io,
    Format,
    Schema,
    Validation,
    Transaction,
    Query,
    NotImplemented,
}

impl DbError {
    pub fn kind(&self) -> DbErrorKind {
        match self {
            DbError::Io(_) => DbErrorKind::Io,
            DbError::Format(_) => DbErrorKind::Format,
            DbError::Schema(_) => DbErrorKind::Schema,
            DbError::Validation(_) => DbErrorKind::Validation,
            DbError::Transaction(_) => DbErrorKind::Transaction,
            DbError::Query(_) => DbErrorKind::Query,
            DbError::NotImplemented => DbErrorKind::NotImplemented,
        }
    }
}

/// Query errors: unsupported query forms, bad syntax, or invalid paths.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryError {
    pub message: String,
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
    /// Transaction marker segment payload was malformed.
    InvalidTxnPayload { message: String },
    /// On-disk log ends with an incomplete transaction or torn write; strict open refuses to modify.
    UncleanLogTail {
        /// First byte offset that may be discarded to reach a committed prefix (truncate target).
        safe_end: u64,
        reason: &'static str,
    },
}

/// Transaction session errors (0.8+).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransactionError {
    /// `Database::transaction` was called while a transaction is already active.
    NestedTransaction,
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
    /// Unique secondary index was violated (key already mapped to another primary key).
    UniqueIndexViolation,
    /// Proposed schema update is not compatible with the existing schema.
    IncompatibleSchemaChange {
        message: String,
    },
    /// Proposed schema update is supported, but requires an explicit migration step.
    MigrationRequired {
        message: String,
    },
}

impl fmt::Display for ValidationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.path.is_empty() {
            return write!(f, "validation error: {}", self.message);
        }
        write!(
            f,
            "validation error at {}: {}",
            self.path.join("."),
            self.message
        )
    }
}

impl fmt::Display for DbError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DbError::Io(e) => write!(f, "i/o error: {e}"),
            DbError::Format(e) => write!(f, "format error: {e}"),
            DbError::Schema(e) => write!(f, "schema error: {e}"),
            DbError::Validation(e) => write!(f, "{e}"),
            DbError::Transaction(e) => write!(f, "transaction error: {e}"),
            DbError::Query(e) => write!(f, "query error: {}", e.message),
            DbError::NotImplemented => write!(f, "not implemented"),
        }
    }
}

impl fmt::Display for TransactionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TransactionError::NestedTransaction => {
                write!(f, "nested transactions are not supported")
            }
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
            FormatError::InvalidTxnPayload { message } => {
                write!(f, "invalid transaction marker payload: {message}")
            }
            FormatError::UncleanLogTail { safe_end, reason } => {
                write!(
                    f,
                    "unclean log tail (strict open): {reason}; safe truncate end offset {safe_end}"
                )
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
            SchemaError::UniqueIndexViolation => write!(f, "unique index violation"),
            SchemaError::IncompatibleSchemaChange { message } => {
                write!(f, "incompatible schema change: {message}")
            }
            SchemaError::MigrationRequired { message } => {
                write!(f, "migration required: {message}")
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
            DbError::Validation(_) => None,
            DbError::Transaction(_) => None,
            DbError::Query(_) => None,
            DbError::NotImplemented => None,
        }
    }
}

impl From<std::io::Error> for DbError {
    fn from(value: std::io::Error) -> Self {
        DbError::Io(value)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error;
    use std::io;

    #[test]
    fn db_error_kind_display_and_source_smoke() {
        let io_err = io::Error::new(io::ErrorKind::Other, "nope");
        let e: DbError = io_err.into();
        assert_eq!(e.kind(), DbErrorKind::Io);
        assert!(e.source().is_some());
        assert!(format!("{e}").contains("i/o error"));

        let fmt = DbError::Format(FormatError::BadMagic { got: *b"NOPE" });
        assert_eq!(fmt.kind(), DbErrorKind::Format);
        assert!(fmt.source().is_none());
        assert!(format!("{fmt}").contains("format error"));

        let schema = DbError::Schema(SchemaError::UniqueIndexViolation);
        assert_eq!(schema.kind(), DbErrorKind::Schema);
        assert!(schema.source().is_none());
        assert!(format!("{schema}").contains("unique index violation"));

        let val = DbError::Validation(ValidationError {
            path: vec!["a".to_string(), "b".to_string()],
            message: "bad".to_string(),
        });
        assert_eq!(val.kind(), DbErrorKind::Validation);
        assert!(format!("{val}").contains("validation error at a.b"));

        let val2 = DbError::Validation(ValidationError {
            path: vec![],
            message: "bad".to_string(),
        });
        assert!(format!("{val2}").contains("validation error: bad"));

        let txn = DbError::Transaction(TransactionError::NestedTransaction);
        assert_eq!(txn.kind(), DbErrorKind::Transaction);
        assert!(txn.source().is_none());
        assert!(format!("{txn}").contains("nested transactions"));

        let q = DbError::Query(QueryError {
            message: "q".to_string(),
        });
        assert_eq!(q.kind(), DbErrorKind::Query);
        assert!(q.source().is_none());
        assert!(format!("{q}").contains("query error"));

        let ni = DbError::NotImplemented;
        assert_eq!(ni.kind(), DbErrorKind::NotImplemented);
        assert!(format!("{ni}").contains("not implemented"));

        // Exercise some FormatError display arms that are otherwise hard to hit.
        let s = format!(
            "{}",
            FormatError::UncleanLogTail {
                safe_end: 7,
                reason: "torn"
            }
        );
        assert!(s.contains("unclean log tail"));
        assert!(s.contains("7"));

        // Exercise remaining SchemaError display arms.
        let msgs = [
            format!("{}", SchemaError::DuplicateCollectionName { name: "x".to_string() }),
            format!("{}", SchemaError::UnknownCollection { id: 7 }),
            format!(
                "{}",
                SchemaError::InvalidSchemaVersion {
                    expected: 1,
                    got: 2
                }
            ),
            format!("{}", SchemaError::SchemaVersionExhausted),
            format!(
                "{}",
                SchemaError::UnexpectedCollectionId {
                    expected: 1,
                    got: 2
                }
            ),
            format!("{}", SchemaError::NoPrimaryKey { collection_id: 1 }),
            format!(
                "{}",
                SchemaError::PrimaryFieldNotFound {
                    name: "id".to_string()
                }
            ),
            format!(
                "{}",
                SchemaError::PrimaryFieldMissingInSchema {
                    name: "id".to_string()
                }
            ),
            format!(
                "{}",
                SchemaError::IncompatibleSchemaChange {
                    message: "no".to_string()
                }
            ),
            format!(
                "{}",
                SchemaError::MigrationRequired {
                    message: "yes".to_string()
                }
            ),
        ];
        for m in msgs {
            assert!(!m.is_empty());
        }
    }
}
