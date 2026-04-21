use typra_core::error::FormatError;
use typra_core::segments::header::{decode_segment_header, SegmentHeader, SegmentType};
use typra_core::segments::reader::scan_segments;
use typra_core::storage::{FileStore, Store};
use typra_core::DbError;

#[test]
fn segment_header_bad_magic_is_format_error() {
    let mut bytes = SegmentHeader {
        segment_type: SegmentType::Schema,
        payload_len: 1,
        payload_crc32c: 0,
    }
    .encode();
    bytes[0] ^= 0xff;
    let res = decode_segment_header(&bytes);
    assert!(matches!(
        res,
        Err(DbError::Format(FormatError::BadSegmentMagic { .. }))
    ));
}

#[test]
fn segment_header_checksum_mismatch_is_format_error() {
    let mut bytes = SegmentHeader {
        segment_type: SegmentType::Schema,
        payload_len: 1,
        payload_crc32c: 0,
    }
    .encode();
    bytes[28] ^= 0xff;
    let res = decode_segment_header(&bytes);
    assert!(matches!(
        res,
        Err(DbError::Format(FormatError::BadSegmentHeaderChecksum))
    ));
}

#[test]
fn segment_payload_checksum_mismatch_is_format_error() {
    let f = tempfile::NamedTempFile::new().unwrap();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(f.path())
        .unwrap();
    let mut store = FileStore::new(file);

    let start = 128u64;
    store.write_all_at(start - 1, &[0u8]).unwrap();

    let header = SegmentHeader {
        segment_type: SegmentType::Schema,
        payload_len: 3,
        payload_crc32c: 123, // wrong on purpose
    }
    .encode();
    store.write_all_at(start, &header).unwrap();
    store
        .write_all_at(start + header.len() as u64, b"abc")
        .unwrap();

    let res = scan_segments(&mut store, start);
    assert!(matches!(
        res,
        Err(DbError::Format(FormatError::BadSegmentPayloadChecksum))
    ));
}

#[test]
fn scan_segments_rejects_payload_past_eof() {
    let f = tempfile::NamedTempFile::new().unwrap();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(f.path())
        .unwrap();
    let mut store = FileStore::new(file);

    let start = 128u64;
    store.write_all_at(start - 1, &[0u8]).unwrap();

    let header = SegmentHeader {
        segment_type: SegmentType::Schema,
        payload_len: 9999,
        payload_crc32c: 0,
    }
    .encode();
    store.write_all_at(start, &header).unwrap();

    let res = scan_segments(&mut store, start);
    assert!(matches!(
        res,
        Err(DbError::Format(FormatError::SegmentPayloadPastEof))
    ));
}

#[test]
fn scan_segments_rejects_trailing_truncated_header() {
    let f = tempfile::NamedTempFile::new().unwrap();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(f.path())
        .unwrap();
    let mut store = FileStore::new(file);

    let start = 128u64;
    store.write_all_at(start - 1, &[0u8]).unwrap();
    store.write_all_at(start, &[0u8; 7]).unwrap();

    let res = scan_segments(&mut store, start);
    assert!(matches!(
        res,
        Err(DbError::Format(FormatError::TruncatedSegmentHeader { .. }))
    ));
}
