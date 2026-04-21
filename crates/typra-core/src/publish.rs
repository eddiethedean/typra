use crate::error::DbError;
use crate::manifest::ManifestV0;
use crate::segments::header::{SegmentHeader, SegmentType, SEGMENT_HEADER_LEN};
use crate::segments::writer::SegmentWriter;
use crate::storage::Store;
use crate::superblock::{Superblock, SUPERBLOCK_SIZE};
use crate::{file_format::FILE_HEADER_SIZE, superblock::decode_superblock};

pub fn append_manifest_and_publish(
    store: &mut impl Store,
    segment_start: u64,
) -> Result<Superblock, DbError> {
    let file_len = store.len()?;
    let mut writer = SegmentWriter::new(store, file_len.max(segment_start));

    let manifest_offset = writer.offset();
    let manifest = ManifestV0 {
        last_segment_offset: manifest_offset,
        last_segment_len: (SEGMENT_HEADER_LEN + crate::manifest::MANIFEST_V0_LEN) as u64,
    };
    let manifest_payload = manifest.encode();
    writer.append(
        SegmentHeader {
            segment_type: SegmentType::Manifest,
            payload_len: 0,
            payload_crc32c: 0,
        },
        &manifest_payload,
    )?;

    // Read both superblocks and select current (reuse decode behavior).
    let mut a = [0u8; SUPERBLOCK_SIZE];
    let mut b = [0u8; SUPERBLOCK_SIZE];
    store.read_exact_at(FILE_HEADER_SIZE as u64, &mut a)?;
    store.read_exact_at((FILE_HEADER_SIZE + SUPERBLOCK_SIZE) as u64, &mut b)?;

    let sa = decode_superblock(&a).ok();
    let sb = decode_superblock(&b).ok();

    let selected = match (sa, sb) {
        (Some(sa), Some(sb)) => {
            if sa.generation >= sb.generation {
                (sa, true)
            } else {
                (sb, false)
            }
        }
        (Some(sa), None) => (sa, true),
        (None, Some(sb)) => (sb, false),
        (None, None) => (Superblock::empty(), true),
    };

    let (current, current_is_a) = selected;
    let next_generation = current.generation.saturating_add(1);
    let next = Superblock {
        generation: next_generation,
        manifest_offset,
        manifest_len: manifest_payload.len() as u32,
        checksum_kind: current.checksum_kind,
    };

    let target_offset = if current_is_a {
        (FILE_HEADER_SIZE + SUPERBLOCK_SIZE) as u64
    } else {
        FILE_HEADER_SIZE as u64
    };
    store.write_all_at(target_offset, &next.encode())?;
    store.sync()?;

    Ok(next)
}
