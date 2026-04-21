use std::error::Error;

use typra_core::error::{FormatError, SchemaError};
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
