use typra_core::segments::header::{SegmentHeader, SegmentType, SEGMENT_HEADER_LEN};
use typra_core::segments::reader::scan_segments;
use typra_core::storage::{FileStore, Store, VecStore};

fn write_one_segment_with_bad_payload_crc(
    store: &mut impl Store,
    segment_type: SegmentType,
    payload: &[u8],
) {
    let header = SegmentHeader {
        segment_type,
        payload_len: payload.len() as u64,
        // Intentionally wrong: payload CRC does not match bytes.
        payload_crc32c: 0,
    }
    .encode();
    store.write_all_at(0, &header).unwrap();
    store.write_all_at(SEGMENT_HEADER_LEN as u64, payload).unwrap();
}

#[test]
fn scan_segments_ignores_bad_payload_crc_for_temp_segments() {
    let mut store = VecStore::new();
    write_one_segment_with_bad_payload_crc(&mut store, SegmentType::Temp, b"spill");
    let metas = scan_segments(&mut store, 0).unwrap();
    assert_eq!(metas.len(), 1);
    assert_eq!(metas[0].header.segment_type, SegmentType::Temp);
}

#[test]
fn scan_segments_ignores_bad_payload_crc_for_checkpoint_segments() {
    let mut store = VecStore::new();
    write_one_segment_with_bad_payload_crc(&mut store, SegmentType::Checkpoint, b"not-a-checkpoint");
    let metas = scan_segments(&mut store, 0).unwrap();
    assert_eq!(metas.len(), 1);
    assert_eq!(metas[0].header.segment_type, SegmentType::Checkpoint);
}

#[test]
fn scan_segments_file_store_ignores_bad_payload_crc_for_temp_segments() {
    let f = tempfile::NamedTempFile::new().unwrap();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(f.path())
        .unwrap();
    let mut store = FileStore::new(file);
    write_one_segment_with_bad_payload_crc(&mut store, SegmentType::Temp, b"spill");
    let metas = scan_segments(&mut store, 0).unwrap();
    assert_eq!(metas.len(), 1);
    assert_eq!(metas[0].header.segment_type, SegmentType::Temp);
}

#[test]
fn scan_segments_file_store_ignores_bad_payload_crc_for_checkpoint_segments() {
    let f = tempfile::NamedTempFile::new().unwrap();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(true)
        .open(f.path())
        .unwrap();
    let mut store = FileStore::new(file);
    write_one_segment_with_bad_payload_crc(&mut store, SegmentType::Checkpoint, b"x");
    let metas = scan_segments(&mut store, 0).unwrap();
    assert_eq!(metas.len(), 1);
    assert_eq!(metas[0].header.segment_type, SegmentType::Checkpoint);
}

