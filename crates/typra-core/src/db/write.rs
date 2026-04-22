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

pub(crate) fn append_schema_segment_and_publish<S: Store>(
    store: &mut S,
    segment_start: u64,
    format_minor: &mut u16,
    payload: &[u8],
) -> Result<(), DbError> {
    ensure_header_v0_4(store, format_minor)?;
    let file_len = store.len()?;
    let mut writer = SegmentWriter::new(store, file_len.max(segment_start));
    writer.append(
        SegmentHeader {
            segment_type: SegmentType::Schema,
            payload_len: 0,
            payload_crc32c: 0,
        },
        payload,
    )?;
    let _ = append_manifest_and_publish(store, segment_start)?;
    store.sync()?;
    Ok(())
}

pub(crate) fn append_record_segment_and_publish<S: Store>(
    store: &mut S,
    segment_start: u64,
    format_minor: &mut u16,
    payload: &[u8],
) -> Result<(), DbError> {
    ensure_header_v0_5(store, format_minor)?;
    let file_len = store.len()?;
    let mut writer = SegmentWriter::new(store, file_len.max(segment_start));
    writer.append(
        SegmentHeader {
            segment_type: SegmentType::Record,
            payload_len: 0,
            payload_crc32c: 0,
        },
        payload,
    )?;
    let _ = append_manifest_and_publish(store, segment_start)?;
    store.sync()?;
    Ok(())
}
