use std::error::Error;

use typra_core::error::{FormatError, SchemaError, ValidationError};
use typra_core::DbError;

#[test]
fn not_implemented_display_and_source() {
    let e = DbError::NotImplemented;
    assert_eq!(e.to_string(), "not implemented");
    assert!(e.source().is_none());
}

#[test]
fn format_error_display_and_source() {
    let e = DbError::Format(FormatError::UnsupportedVersion { major: 9, minor: 9 });
    assert!(e.to_string().contains("format error"));
    assert!(e.source().is_none());
}

#[test]
fn schema_error_display_and_source() {
    let e = DbError::Schema(SchemaError::InvalidFieldPath);
    assert!(e.to_string().contains("schema error"));
    assert!(e.source().is_none());
}

#[test]
fn validation_error_display_and_source() {
    let e = DbError::Validation(ValidationError {
        path: vec!["a".into(), "b".into()],
        message: "expected int64".into(),
    });
    let s = e.to_string();
    assert!(s.contains("validation"));
    assert!(s.contains("a.b"));
    assert!(e.source().is_none());
}

#[test]
fn format_error_display_variants() {
    let e = DbError::Format(FormatError::BadMagic { got: *b"NOPE" });
    let s = e.to_string();
    assert!(s.contains("format error"));
    assert!(s.contains("bad magic"));

    let e = DbError::Format(FormatError::TruncatedHeader {
        got: 1,
        expected: 32,
    });
    let s = e.to_string();
    assert!(s.contains("truncated header"));

    let e = DbError::Format(FormatError::BadSuperblockChecksum);
    let s = e.to_string();
    assert!(s.contains("superblock checksum"));

    let e = DbError::Format(FormatError::BadSegmentPayloadChecksum);
    let s = e.to_string();
    assert!(s.contains("payload checksum"));

    let e = DbError::Format(FormatError::BadSuperblockMagic { got: *b"NOPE" });
    assert!(e.to_string().contains("superblock magic"));

    let e = DbError::Format(FormatError::TruncatedSuperblock {
        got: 1,
        expected: 2,
    });
    assert!(e.to_string().contains("truncated superblock"));

    let e = DbError::Format(FormatError::BadSegmentMagic { got: *b"NOPE" });
    assert!(e.to_string().contains("segment magic"));

    let e = DbError::Format(FormatError::TruncatedSegmentHeader {
        got: 1,
        expected: 2,
    });
    assert!(e.to_string().contains("truncated segment header"));

    let e = DbError::Format(FormatError::BadSegmentHeaderChecksum);
    assert!(e.to_string().contains("header checksum"));

    let e = DbError::Format(FormatError::SegmentPayloadPastEof);
    assert!(e.to_string().contains("past end of file"));

    let e = DbError::Format(FormatError::InvalidCatalogPayload {
        message: "x".into(),
    });
    assert!(e.to_string().contains("invalid catalog payload"));

    let e = DbError::Format(FormatError::TruncatedRecordPayload);
    assert!(e.to_string().contains("truncated record payload"));

    let e = DbError::Format(FormatError::RecordPayloadTypeMismatch);
    assert!(e.to_string().contains("record payload type"));

    let e = DbError::Format(FormatError::InvalidRecordUtf8);
    assert!(e.to_string().contains("UTF-8"));

    let e = DbError::Format(FormatError::RecordPayloadUnsupportedType);
    assert!(e.to_string().contains("unsupported type"));

    let e = DbError::Format(FormatError::UnknownRecordPayloadVersion { got: 9 });
    assert!(e.to_string().contains("unknown record payload version"));

    let e = DbError::Format(FormatError::TrailingRecordPayload);
    assert!(e.to_string().contains("trailing"));
}

#[test]
fn schema_error_display_all_variants() {
    let cases: &[(SchemaError, &[&str])] = &[
        (SchemaError::InvalidFieldPath, &["invalid field path"]),
        (
            SchemaError::DuplicateCollectionName { name: "a".into() },
            &["duplicate", "a"],
        ),
        (
            SchemaError::UnknownCollection { id: 3 },
            &["unknown collection id", "3"],
        ),
        (
            SchemaError::UnknownCollectionName { name: "z".into() },
            &["unknown collection name", "z"],
        ),
        (
            SchemaError::InvalidCollectionName,
            &["invalid collection name"],
        ),
        (
            SchemaError::InvalidSchemaVersion {
                expected: 1,
                got: 2,
            },
            &["invalid schema version", "1", "2"],
        ),
        (
            SchemaError::SchemaVersionExhausted,
            &["schema version limit"],
        ),
        (
            SchemaError::UnexpectedCollectionId {
                expected: 1,
                got: 2,
            },
            &["unexpected collection id", "1", "2"],
        ),
        (
            SchemaError::NoPrimaryKey { collection_id: 7 },
            &["no primary key", "7"],
        ),
        (
            SchemaError::PrimaryFieldNotFound { name: "pk".into() },
            &["primary field", "pk"],
        ),
        (
            SchemaError::PrimaryFieldMissingInSchema { name: "pk".into() },
            &["schema update", "pk"],
        ),
        (
            SchemaError::RowMissingPrimary { name: "id".into() },
            &["missing primary", "id"],
        ),
        (
            SchemaError::RowUnknownField { name: "bad".into() },
            &["unknown field", "bad"],
        ),
        (
            SchemaError::RowMissingField {
                name: "need".into(),
            },
            &["missing field", "need"],
        ),
    ];
    for (err, needles) in cases {
        let s = DbError::Schema(err.clone()).to_string();
        for n in *needles {
            assert!(
                s.contains(n),
                "expected {s:?} to contain {n:?} (error {err:?})"
            );
        }
    }
}

#[test]
fn io_error_display_includes_message() {
    let inner = std::io::Error::new(std::io::ErrorKind::NotFound, "missing");
    let e = DbError::Io(inner);
    assert!(e.to_string().contains("i/o error"));
    assert!(e.to_string().contains("missing"));
    assert!(e.source().is_some());
}

#[test]
fn from_io_error() {
    let inner = std::io::Error::from_raw_os_error(2);
    let e: DbError = inner.into();
    assert!(matches!(e, DbError::Io(_)));
}
