use typra_core::error::{
    DbError, DbErrorKind, FormatError, QueryError, SchemaError, TransactionError, ValidationError,
};

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
fn db_error_source_covers_transaction_and_query_none() {
    use std::error::Error;

    let txn = DbError::Transaction(TransactionError::NestedTransaction);
    assert!(txn.source().is_none());

    let qry = DbError::Query(QueryError {
        message: "oops".into(),
    });
    assert!(qry.source().is_none());
}
