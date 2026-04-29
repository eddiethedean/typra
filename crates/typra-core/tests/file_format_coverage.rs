//! Integration coverage for [`typra_core::file_format`] (`cargo llvm-cov` uses `--tests` only).

use typra_core::error::{DbError, FormatError};
use typra_core::file_format::{
    decode_header, FileHeader, OpenableMinor, FILE_HEADER_SIZE, FILE_MAGIC, FORMAT_MAJOR,
};

#[test]
fn constructors_and_decode_matrix() {
    for h in [
        FileHeader::new_v0_3(),
        FileHeader::new_v0_4(),
        FileHeader::new_v0_5(),
        FileHeader::new_v0_8(),
    ] {
        let buf = h.encode();
        let got = decode_header(&buf).unwrap();
        assert_eq!(got.format_major, FORMAT_MAJOR);
        assert_eq!(got.format_minor, h.format_minor);
        assert_eq!(got.header_size, FILE_HEADER_SIZE as u32);
    }

    let e = decode_header(&[0u8; FILE_HEADER_SIZE - 1]).unwrap_err();
    assert!(matches!(
        e,
        DbError::Format(FormatError::TruncatedHeader { .. })
    ));

    let mut bad_magic = FileHeader::new_v0_5().encode();
    bad_magic[0..4].copy_from_slice(b"XXXX");
    let e = decode_header(&bad_magic).unwrap_err();
    assert!(matches!(e, DbError::Format(FormatError::BadMagic { .. })));

    for bad_minor in [0u16, 1u16, 7u16] {
        let mut buf = FileHeader::new_v0_5().encode();
        buf[6..8].copy_from_slice(&bad_minor.to_le_bytes());
        let e = decode_header(&buf).unwrap_err();
        assert!(matches!(
            e,
            DbError::Format(FormatError::UnsupportedVersion { .. })
        ));
    }

    let mut bad_major = FileHeader::new_v0_5().encode();
    bad_major[4..6].copy_from_slice(&9u16.to_le_bytes());
    let e = decode_header(&bad_major).unwrap_err();
    assert!(matches!(
        e,
        DbError::Format(FormatError::UnsupportedVersion { .. })
    ));

    assert_eq!(FILE_MAGIC, *b"TDB0");
}

#[test]
fn classify_for_open_all_branches() {
    let h2 = FileHeader {
        format_major: FORMAT_MAJOR,
        format_minor: 2,
        header_size: FILE_HEADER_SIZE as u32,
        flags: 0,
    };
    assert_eq!(h2.classify_for_open().unwrap(), OpenableMinor::V2);

    for m in 3u16..=6u16 {
        let h = FileHeader {
            format_major: FORMAT_MAJOR,
            format_minor: m,
            header_size: FILE_HEADER_SIZE as u32,
            flags: 0,
        };
        assert_eq!(h.classify_for_open().unwrap(), OpenableMinor::V3to6);
    }

    for bad_minor in [0u16, 1u16, 7u16, u16::MAX] {
        let h = FileHeader {
            format_major: FORMAT_MAJOR,
            format_minor: bad_minor,
            header_size: FILE_HEADER_SIZE as u32,
            flags: 0,
        };
        let e = h.classify_for_open().unwrap_err();
        assert!(matches!(
            e,
            FormatError::UnsupportedVersion { minor, .. } if minor == bad_minor
        ));
    }
}
