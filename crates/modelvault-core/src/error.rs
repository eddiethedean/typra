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

impl DbErrorKind {
    /// Stable snake_case code for bindings and logging.
    pub const fn as_str(self) -> &'static str {
        match self {
            DbErrorKind::Io => "io",
            DbErrorKind::Format => "format",
            DbErrorKind::Schema => "schema",
            DbErrorKind::Validation => "validation",
            DbErrorKind::Transaction => "transaction",
            DbErrorKind::Query => "query",
            DbErrorKind::NotImplemented => "not_implemented",
        }
    }
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

    /// Machine-readable key/value pairs for the specific error variant (empty for I/O).
    pub fn details(&self) -> std::collections::BTreeMap<String, String> {
        match self {
            DbError::Io(_) => std::collections::BTreeMap::new(),
            DbError::Format(e) => format_error_details(e),
            DbError::Schema(e) => schema_error_details(e),
            DbError::Validation(e) => {
                let mut m = std::collections::BTreeMap::new();
                if !e.path.is_empty() {
                    m.insert("path".to_string(), e.path.join("."));
                }
                m.insert("message".to_string(), e.message.clone());
                m
            }
            DbError::Transaction(e) => transaction_error_details(e),
            DbError::Query(e) => {
                let mut m = std::collections::BTreeMap::new();
                m.insert("message".to_string(), e.message.clone());
                m
            }
            DbError::NotImplemented => std::collections::BTreeMap::new(),
        }
    }
}

fn format_error_details(e: &FormatError) -> std::collections::BTreeMap<String, String> {
    use std::collections::BTreeMap;
    let mut m = BTreeMap::new();
    match e {
        FormatError::BadMagic { got } => {
            m.insert("variant".to_string(), "bad_magic".to_string());
            m.insert("got".to_string(), format!("{got:02x?}"));
        }
        FormatError::TruncatedHeader { got, expected } => {
            m.insert("variant".to_string(), "truncated_header".to_string());
            m.insert("got".to_string(), got.to_string());
            m.insert("expected".to_string(), expected.to_string());
        }
        FormatError::UnsupportedVersion { major, minor } => {
            m.insert("variant".to_string(), "unsupported_version".to_string());
            m.insert("major".to_string(), major.to_string());
            m.insert("minor".to_string(), minor.to_string());
        }
        FormatError::TruncatedSuperblock { got, expected } => {
            m.insert("variant".to_string(), "truncated_superblock".to_string());
            m.insert("got".to_string(), got.to_string());
            m.insert("expected".to_string(), expected.to_string());
        }
        FormatError::BadSuperblockMagic { got } => {
            m.insert("variant".to_string(), "bad_superblock_magic".to_string());
            m.insert("got".to_string(), format!("{got:02x?}"));
        }
        FormatError::BadSuperblockChecksum => {
            m.insert("variant".to_string(), "bad_superblock_checksum".to_string());
        }
        FormatError::TruncatedSegmentHeader { got, expected } => {
            m.insert(
                "variant".to_string(),
                "truncated_segment_header".to_string(),
            );
            m.insert("got".to_string(), got.to_string());
            m.insert("expected".to_string(), expected.to_string());
        }
        FormatError::BadSegmentMagic { got } => {
            m.insert("variant".to_string(), "bad_segment_magic".to_string());
            m.insert("got".to_string(), format!("{got:02x?}"));
        }
        FormatError::BadSegmentHeaderChecksum => {
            m.insert(
                "variant".to_string(),
                "bad_segment_header_checksum".to_string(),
            );
        }
        FormatError::BadSegmentPayloadChecksum => {
            m.insert(
                "variant".to_string(),
                "bad_segment_payload_checksum".to_string(),
            );
        }
        FormatError::SegmentPayloadPastEof => {
            m.insert(
                "variant".to_string(),
                "segment_payload_past_eof".to_string(),
            );
        }
        FormatError::InvalidCatalogPayload { message } => {
            m.insert("variant".to_string(), "invalid_catalog_payload".to_string());
            m.insert("message".to_string(), message.clone());
        }
        FormatError::TruncatedRecordPayload => {
            m.insert(
                "variant".to_string(),
                "truncated_record_payload".to_string(),
            );
        }
        FormatError::RecordPayloadTypeMismatch => {
            m.insert(
                "variant".to_string(),
                "record_payload_type_mismatch".to_string(),
            );
        }
        FormatError::InvalidRecordUtf8 => {
            m.insert("variant".to_string(), "invalid_record_utf8".to_string());
        }
        FormatError::RecordPayloadUnsupportedType => {
            m.insert(
                "variant".to_string(),
                "record_payload_unsupported_type".to_string(),
            );
        }
        FormatError::UnknownRecordPayloadVersion { got } => {
            m.insert(
                "variant".to_string(),
                "unknown_record_payload_version".to_string(),
            );
            m.insert("got".to_string(), got.to_string());
        }
        FormatError::TrailingRecordPayload => {
            m.insert("variant".to_string(), "trailing_record_payload".to_string());
        }
        FormatError::InvalidTxnPayload { message } => {
            m.insert("variant".to_string(), "invalid_txn_payload".to_string());
            m.insert("message".to_string(), message.clone());
        }
        FormatError::InvalidCheckpointPayload { message } => {
            m.insert(
                "variant".to_string(),
                "invalid_checkpoint_payload".to_string(),
            );
            m.insert("message".to_string(), message.clone());
        }
        FormatError::UncleanLogTail { safe_end, reason } => {
            m.insert("variant".to_string(), "unclean_log_tail".to_string());
            m.insert("safe_end".to_string(), safe_end.to_string());
            m.insert("reason".to_string(), (*reason).to_string());
        }
    }
    m
}

fn schema_error_details(e: &SchemaError) -> std::collections::BTreeMap<String, String> {
    use std::collections::BTreeMap;
    let mut m = BTreeMap::new();
    match e {
        SchemaError::InvalidFieldPath => {
            m.insert("variant".to_string(), "invalid_field_path".to_string());
        }
        SchemaError::DuplicateCollectionName { name } => {
            m.insert(
                "variant".to_string(),
                "duplicate_collection_name".to_string(),
            );
            m.insert("name".to_string(), name.clone());
        }
        SchemaError::UnknownCollection { id } => {
            m.insert("variant".to_string(), "unknown_collection".to_string());
            m.insert("id".to_string(), id.to_string());
        }
        SchemaError::UnknownCollectionName { name } => {
            m.insert("variant".to_string(), "unknown_collection_name".to_string());
            m.insert("name".to_string(), name.clone());
        }
        SchemaError::InvalidCollectionName => {
            m.insert("variant".to_string(), "invalid_collection_name".to_string());
        }
        SchemaError::InvalidSchemaVersion { expected, got } => {
            m.insert("variant".to_string(), "invalid_schema_version".to_string());
            m.insert("expected".to_string(), expected.to_string());
            m.insert("got".to_string(), got.to_string());
        }
        SchemaError::SchemaVersionExhausted => {
            m.insert(
                "variant".to_string(),
                "schema_version_exhausted".to_string(),
            );
        }
        SchemaError::UnexpectedCollectionId { expected, got } => {
            m.insert(
                "variant".to_string(),
                "unexpected_collection_id".to_string(),
            );
            m.insert("expected".to_string(), expected.to_string());
            m.insert("got".to_string(), got.to_string());
        }
        SchemaError::NoPrimaryKey { collection_id } => {
            m.insert("variant".to_string(), "no_primary_key".to_string());
            m.insert("collection_id".to_string(), collection_id.to_string());
        }
        SchemaError::PrimaryFieldNotFound { name } => {
            m.insert("variant".to_string(), "primary_field_not_found".to_string());
            m.insert("name".to_string(), name.clone());
        }
        SchemaError::PrimaryFieldMissingInSchema { name } => {
            m.insert(
                "variant".to_string(),
                "primary_field_missing_in_schema".to_string(),
            );
            m.insert("name".to_string(), name.clone());
        }
        SchemaError::RowMissingPrimary { name } => {
            m.insert("variant".to_string(), "row_missing_primary".to_string());
            m.insert("name".to_string(), name.clone());
        }
        SchemaError::RowUnknownField { name } => {
            m.insert("variant".to_string(), "row_unknown_field".to_string());
            m.insert("name".to_string(), name.clone());
        }
        SchemaError::RowMissingField { name } => {
            m.insert("variant".to_string(), "row_missing_field".to_string());
            m.insert("name".to_string(), name.clone());
        }
        SchemaError::UniqueIndexViolation => {
            m.insert("variant".to_string(), "unique_index_violation".to_string());
        }
        SchemaError::IncompatibleSchemaChange { message } => {
            m.insert(
                "variant".to_string(),
                "incompatible_schema_change".to_string(),
            );
            m.insert("message".to_string(), message.clone());
        }
        SchemaError::MigrationRequired { message } => {
            m.insert("variant".to_string(), "migration_required".to_string());
            m.insert("message".to_string(), message.clone());
        }
        SchemaError::IndexRowMissing {
            collection_id,
            index_name,
        } => {
            m.insert("variant".to_string(), "index_row_missing".to_string());
            m.insert("collection_id".to_string(), collection_id.to_string());
            m.insert("index_name".to_string(), index_name.clone());
        }
        SchemaError::PrimaryKeyTypeMismatch { collection_id } => {
            m.insert(
                "variant".to_string(),
                "primary_key_type_mismatch".to_string(),
            );
            m.insert("collection_id".to_string(), collection_id.to_string());
        }
    }
    m
}

fn transaction_error_details(e: &TransactionError) -> std::collections::BTreeMap<String, String> {
    use std::collections::BTreeMap;
    let mut m = BTreeMap::new();
    match e {
        TransactionError::NestedTransaction => {
            m.insert("variant".to_string(), "nested_transaction".to_string());
        }
        TransactionError::NoActiveTransaction => {
            m.insert("variant".to_string(), "no_active_transaction".to_string());
        }
    }
    m
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
    /// Checkpoint payload references a replay offset before the checkpoint segment end.
    InvalidCheckpointPayload { message: String },
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
    /// `commit_transaction` was called with no active transaction.
    NoActiveTransaction,
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
    /// Secondary index references a primary key with no row in `latest`.
    IndexRowMissing {
        collection_id: u32,
        index_name: String,
    },
    /// Primary key scalar type does not match the collection primary field type.
    PrimaryKeyTypeMismatch {
        collection_id: u32,
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
            TransactionError::NoActiveTransaction => {
                write!(f, "no active transaction")
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
            FormatError::InvalidCheckpointPayload { message } => {
                write!(f, "invalid checkpoint payload: {message}")
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
                write!(
                    f,
                    "schema version limit reached (u32::MAX); cannot register another schema version"
                )
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
            SchemaError::IndexRowMissing {
                collection_id,
                index_name,
            } => {
                write!(
                    f,
                    "index {index_name:?} on collection {collection_id} references missing row"
                )
            }
            SchemaError::PrimaryKeyTypeMismatch { collection_id } => {
                write!(
                    f,
                    "primary key type mismatch for collection {collection_id}"
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
