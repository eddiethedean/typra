use modelvault_core::error::{
    DbError, DbErrorKind, FormatError, QueryError, SchemaError, TransactionError, ValidationError,
};

fn assert_details_has_variant(err: &DbError, variant: &str) {
    let d = err.details();
    assert_eq!(d.get("variant").map(String::as_str), Some(variant), "{d:?}");
}

#[test]
fn db_error_kind_covers_all_variants() {
    let io = DbError::Io(std::io::Error::other("x"));
    assert_eq!(io.kind(), DbErrorKind::Io);

    let fmt = DbError::Format(FormatError::TruncatedRecordPayload);
    assert_eq!(fmt.kind(), DbErrorKind::Format);

    let sch = DbError::Schema(SchemaError::InvalidCollectionName);
    assert_eq!(sch.kind(), DbErrorKind::Schema);

    let val = DbError::Validation(ValidationError {
        path: vec![],
        message: "nope".into(),
    });
    assert_eq!(val.kind(), DbErrorKind::Validation);

    let txn = DbError::Transaction(TransactionError::NestedTransaction);
    assert_eq!(txn.kind(), DbErrorKind::Transaction);

    let qry = DbError::Query(QueryError {
        message: "bad".into(),
    });
    assert_eq!(qry.kind(), DbErrorKind::Query);

    let ni = DbError::NotImplemented;
    assert_eq!(ni.kind(), DbErrorKind::NotImplemented);
}

#[test]
fn display_covers_validation_path_empty_and_query_and_migration_errors() {
    let v = ValidationError {
        path: vec![],
        message: "m".into(),
    };
    assert_eq!(v.to_string(), "validation error: m");

    let q = DbError::Query(QueryError {
        message: "oops".into(),
    });
    assert_eq!(q.to_string(), "query error: oops");

    let s1 = SchemaError::IncompatibleSchemaChange {
        message: "x".into(),
    };
    assert!(s1.to_string().contains("incompatible schema change"));

    let s2 = SchemaError::MigrationRequired {
        message: "y".into(),
    };
    assert!(s2.to_string().contains("migration required"));

    let s3 = SchemaError::UniqueIndexViolation;
    assert_eq!(s3.to_string(), "unique index violation");
}

#[test]
fn db_error_kind_as_str_and_details_cover_all_variants() {
    assert_eq!(DbErrorKind::Io.as_str(), "io");
    assert_eq!(DbErrorKind::Format.as_str(), "format");
    assert_eq!(DbErrorKind::Schema.as_str(), "schema");
    assert_eq!(DbErrorKind::Validation.as_str(), "validation");
    assert_eq!(DbErrorKind::Transaction.as_str(), "transaction");
    assert_eq!(DbErrorKind::Query.as_str(), "query");
    assert_eq!(DbErrorKind::NotImplemented.as_str(), "not_implemented");

    assert!(DbError::Io(std::io::Error::other("x")).details().is_empty());
    assert!(DbError::NotImplemented.details().is_empty());

    let format_cases: Vec<FormatError> = vec![
        FormatError::BadMagic { got: *b"NOPE" },
        FormatError::TruncatedHeader {
            got: 1,
            expected: 32,
        },
        FormatError::UnsupportedVersion { major: 9, minor: 9 },
        FormatError::TruncatedSuperblock {
            got: 1,
            expected: 96,
        },
        FormatError::BadSuperblockMagic { got: *b"BAD!" },
        FormatError::BadSuperblockChecksum,
        FormatError::TruncatedSegmentHeader {
            got: 1,
            expected: 16,
        },
        FormatError::BadSegmentMagic { got: *b"BAD!" },
        FormatError::BadSegmentHeaderChecksum,
        FormatError::BadSegmentPayloadChecksum,
        FormatError::SegmentPayloadPastEof,
        FormatError::InvalidCatalogPayload {
            message: "bad".into(),
        },
        FormatError::TruncatedRecordPayload,
        FormatError::RecordPayloadTypeMismatch,
        FormatError::InvalidRecordUtf8,
        FormatError::RecordPayloadUnsupportedType,
        FormatError::UnknownRecordPayloadVersion { got: 99 },
        FormatError::TrailingRecordPayload,
        FormatError::InvalidTxnPayload {
            message: "txn".into(),
        },
        FormatError::InvalidCheckpointPayload {
            message: "chk".into(),
        },
        FormatError::UncleanLogTail {
            safe_end: 42,
            reason: "torn",
        },
    ];
    for fe in format_cases {
        let err = DbError::Format(fe);
        assert!(!err.details().is_empty());
        assert!(err.to_string().starts_with("format error:"));
    }

    let schema_cases: Vec<(SchemaError, &str)> = vec![
        (SchemaError::InvalidFieldPath, "invalid_field_path"),
        (
            SchemaError::DuplicateCollectionName {
                name: "x".into(),
            },
            "duplicate_collection_name",
        ),
        (
            SchemaError::UnknownCollection { id: 3 },
            "unknown_collection",
        ),
        (
            SchemaError::UnknownCollectionName {
                name: "n".into(),
            },
            "unknown_collection_name",
        ),
        (SchemaError::InvalidCollectionName, "invalid_collection_name"),
        (
            SchemaError::InvalidSchemaVersion {
                expected: 1,
                got: 2,
            },
            "invalid_schema_version",
        ),
        (
            SchemaError::SchemaVersionExhausted,
            "schema_version_exhausted",
        ),
        (
            SchemaError::UnexpectedCollectionId {
                expected: 1,
                got: 2,
            },
            "unexpected_collection_id",
        ),
        (
            SchemaError::NoPrimaryKey { collection_id: 1 },
            "no_primary_key",
        ),
        (
            SchemaError::PrimaryFieldNotFound {
                name: "id".into(),
            },
            "primary_field_not_found",
        ),
        (
            SchemaError::PrimaryFieldMissingInSchema {
                name: "id".into(),
            },
            "primary_field_missing_in_schema",
        ),
        (
            SchemaError::RowMissingPrimary {
                name: "id".into(),
            },
            "row_missing_primary",
        ),
        (
            SchemaError::RowUnknownField {
                name: "z".into(),
            },
            "row_unknown_field",
        ),
        (
            SchemaError::RowMissingField {
                name: "z".into(),
            },
            "row_missing_field",
        ),
        (
            SchemaError::UniqueIndexViolation,
            "unique_index_violation",
        ),
        (
            SchemaError::IncompatibleSchemaChange {
                message: "no".into(),
            },
            "incompatible_schema_change",
        ),
        (
            SchemaError::MigrationRequired {
                message: "migrate".into(),
            },
            "migration_required",
        ),
        (
            SchemaError::IndexRowMissing {
                collection_id: 1,
                index_name: "i".into(),
            },
            "index_row_missing",
        ),
        (
            SchemaError::PrimaryKeyTypeMismatch { collection_id: 1 },
            "primary_key_type_mismatch",
        ),
    ];
    for (se, variant) in schema_cases {
        let err = DbError::Schema(se);
        assert_details_has_variant(&err, variant);
        assert!(err.to_string().starts_with("schema error:"));
    }

    let val_path = DbError::Validation(ValidationError {
        path: vec!["a".into(), "b".into()],
        message: "bad".into(),
    });
    let d = val_path.details();
    assert_eq!(d.get("path").map(String::as_str), Some("a.b"));
    assert_eq!(d.get("message").map(String::as_str), Some("bad"));
    assert!(val_path.to_string().contains("validation error at a.b"));

    let txn_nested = DbError::Transaction(TransactionError::NestedTransaction);
    assert_details_has_variant(&txn_nested, "nested_transaction");
    assert!(txn_nested.to_string().contains("nested transactions"));

    let txn_none = DbError::Transaction(TransactionError::NoActiveTransaction);
    assert_details_has_variant(&txn_none, "no_active_transaction");
    assert!(txn_none.to_string().contains("no active transaction"));
}

#[test]
fn db_error_source_covers_transaction_and_query_none() {
    use std::error::Error;

    let txn = DbError::Transaction(TransactionError::NestedTransaction);
    assert!(txn.source().is_none());

    let qry = DbError::Query(QueryError {
        message: "oops".into(),
    });
    assert!(qry.source().is_none());
}
