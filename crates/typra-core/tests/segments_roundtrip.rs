use typra_core::segments::header::{SegmentHeader, SegmentType, SEGMENT_HEADER_LEN};
use typra_core::segments::reader::{read_segment_header_at, scan_segments};
use typra_core::segments::writer::SegmentWriter;
use typra_core::storage::{FileStore, Store};

#[test]
fn segments_roundtrip_and_scan() {
    let f = tempfile::NamedTempFile::new().unwrap();
    let file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(true)
        .truncate(false)
        .open(f.path())
        .unwrap();
    let mut store = FileStore::new(file);

    // Reserve space to simulate post-superblock segment start offset.
    let start = 4096u64;
    store.write_all_at(start - 1, &[0u8]).unwrap();

    let mut w = SegmentWriter::new(&mut store, start);
    assert_eq!(w.offset(), start);
    let p1 = b"hello";
    let p2 = b"world!";
    let o1 = w
        .append(
            SegmentHeader {
                segment_type: SegmentType::Schema,
                payload_len: 0,
                payload_crc32c: 0,
            },
            p1,
        )
        .unwrap();
    let o2 = w
        .append(
            SegmentHeader {
                segment_type: SegmentType::Record,
                payload_len: 0,
                payload_crc32c: 0,
            },
            p2,
        )
        .unwrap();

    let _o3 = w
        .append(
            SegmentHeader {
                segment_type: SegmentType::Manifest,
                payload_len: 0,
                payload_crc32c: 0,
            },
            b"",
        )
        .unwrap();

    let _o4 = w
        .append(
            SegmentHeader {
                segment_type: SegmentType::Checkpoint,
                payload_len: 0,
                payload_crc32c: 0,
            },
            b"",
        )
        .unwrap();
    assert_eq!(o1, start);
    assert_eq!(o2, start + SEGMENT_HEADER_LEN as u64 + p1.len() as u64);

    let metas = scan_segments(&mut store, start).unwrap();
    assert_eq!(metas.len(), 4);
    assert_eq!(metas[0].offset, o1);
    assert_eq!(metas[0].header.segment_type, SegmentType::Schema);
    assert_eq!(metas[1].offset, o2);
    assert_eq!(metas[1].header.segment_type, SegmentType::Record);
    assert_eq!(metas[2].header.segment_type, SegmentType::Manifest);
    assert_eq!(metas[3].header.segment_type, SegmentType::Checkpoint);

    let (_raw1, h1) = read_segment_header_at(&mut store, o1).unwrap();
    assert_eq!(h1.payload_len, p1.len() as u64);

    let (_raw2, h2) = read_segment_header_at(&mut store, o2).unwrap();
    assert_eq!(h2.payload_len, p2.len() as u64);
}
