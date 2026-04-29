//! Append catalog or record segments and bump format headers when required.

use crate::file_format::{decode_header, FILE_HEADER_SIZE};
use crate::publish::append_manifest_and_publish;
use crate::segments::header::{SegmentHeader, SegmentType};
use crate::segments::writer::SegmentWriter;
use crate::storage::Store;

use crate::error::DbError;

pub(crate) fn ensure_header_v0_4<S: Store>(
    store: &mut S,
    format_minor: &mut u16,
) -> Result<(), DbError> {
    if *format_minor >= crate::file_format::FORMAT_MINOR_V4 {
        return Ok(());
    }
    let mut buf = [0u8; FILE_HEADER_SIZE];
    store.read_exact_at(0, &mut buf)?;
    let mut h = decode_header(&buf)?;
    h.format_minor = crate::file_format::FORMAT_MINOR_V4;
    store.write_all_at(0, &h.encode())?;
    *format_minor = crate::file_format::FORMAT_MINOR_V4;
    store.sync()?;
    Ok(())
}

pub(crate) fn ensure_header_v0_5<S: Store>(
    store: &mut S,
    format_minor: &mut u16,
) -> Result<(), DbError> {
    if *format_minor >= crate::file_format::FORMAT_MINOR {
        return Ok(());
    }
    let mut buf = [0u8; FILE_HEADER_SIZE];
    store.read_exact_at(0, &mut buf)?;
    let mut h = decode_header(&buf)?;
    h.format_minor = crate::file_format::FORMAT_MINOR;
    store.write_all_at(0, &h.encode())?;
    *format_minor = crate::file_format::FORMAT_MINOR;
    store.sync()?;
    Ok(())
}

pub(crate) fn ensure_header_v0_6<S: Store>(
    store: &mut S,
    format_minor: &mut u16,
) -> Result<(), DbError> {
    ensure_header_v0_4(store, format_minor)?;
    ensure_header_v0_5(store, format_minor)?;
    if *format_minor >= crate::file_format::FORMAT_MINOR_V6 {
        return Ok(());
    }
    let mut buf = [0u8; FILE_HEADER_SIZE];
    store.read_exact_at(0, &mut buf)?;
    let mut h = decode_header(&buf)?;
    h.format_minor = crate::file_format::FORMAT_MINOR_V6;
    store.write_all_at(0, &h.encode())?;
    *format_minor = crate::file_format::FORMAT_MINOR_V6;
    store.sync()?;
    Ok(())
}

/// Append several segments then publish manifest + superblock once and [`Store::sync`].
pub(crate) fn commit_segment_batch<S: Store>(
    store: &mut S,
    segment_start: u64,
    format_minor: &mut u16,
    segments: &[(SegmentType, &[u8])],
) -> Result<(), DbError> {
    ensure_header_v0_6(store, format_minor)?;
    let file_len = store.len()?;
    let mut writer = SegmentWriter::new(store, file_len.max(segment_start));
    for (segment_type, payload) in segments {
        writer.append(
            SegmentHeader {
                segment_type: *segment_type,
                payload_len: 0,
                payload_crc32c: 0,
            },
            payload,
        )?;
    }
    let _ = append_manifest_and_publish(store, segment_start)?;
    store.sync()?;
    Ok(())
}

/// Wrap `body` with matching `TxnBegin` / `TxnCommit` markers using `txn_id`.
pub(crate) fn commit_write_txn_v6<S: Store>(
    store: &mut S,
    segment_start: u64,
    format_minor: &mut u16,
    txn_id: u64,
    body: &[(SegmentType, &[u8])],
) -> Result<(), DbError> {
    let begin = crate::txn::encode_txn_payload_v0(txn_id);
    let commit = crate::txn::encode_txn_payload_v0(txn_id);
    let mut batch: Vec<(SegmentType, &[u8])> = Vec::with_capacity(2 + body.len());
    batch.push((SegmentType::TxnBegin, begin.as_slice()));
    batch.extend_from_slice(body);
    batch.push((SegmentType::TxnCommit, commit.as_slice()));
    commit_segment_batch(store, segment_start, format_minor, &batch)
}

#[cfg(test)]
mod commit_batch_tests {
    use super::*;
    use crate::error::DbError;
    use crate::segments::header::SegmentType;
    use crate::storage::Store;

    /// Store with a synthetic length; all writes fail so [`SegmentWriter::append`] errors.
    struct FailOnWriteStore {
        len: u64,
    }

    impl Store for FailOnWriteStore {
        fn len(&self) -> Result<u64, DbError> {
            Ok(self.len)
        }

        fn read_exact_at(&mut self, _offset: u64, buf: &mut [u8]) -> Result<(), DbError> {
            buf.fill(0);
            Ok(())
        }

        fn write_all_at(&mut self, _offset: u64, _buf: &[u8]) -> Result<(), DbError> {
            Err(DbError::NotImplemented)
        }

        fn sync(&mut self) -> Result<(), DbError> {
            Ok(())
        }

        fn truncate(&mut self, _len: u64) -> Result<(), DbError> {
            Ok(())
        }
    }

    #[test]
    fn commit_segment_batch_propagates_append_error() {
        let mut store = FailOnWriteStore { len: 64 };
        let mut minor = crate::file_format::FORMAT_MINOR_V6;
        let err = commit_segment_batch(
            &mut store,
            64,
            &mut minor,
            &[(SegmentType::Schema, b"x")],
        )
        .unwrap_err();
        assert!(matches!(err, DbError::NotImplemented));
    }
}
