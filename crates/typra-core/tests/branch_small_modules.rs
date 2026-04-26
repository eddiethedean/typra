use typra_core::checkpoint::{
    decode_checkpoint_payload, encode_checkpoint_payload_v0, state_from_checkpoint_payload, CheckpointV0,
    CHECKPOINT_VERSION_V0,
};
use typra_core::error::{DbError, FormatError, ValidationError};
use typra_core::spill::TempSpillFile;
use typra_core::config::OpenMode;
use typra_core::storage::{FileStore, Store};
use typra_core::txn::{decode_txn_payload_v0, encode_txn_payload_v0, TXN_PAYLOAD_V0_LEN};

#[test]
fn checkpoint_decode_rejects_unsupported_version_and_trailing_bytes() {
    let cp = CheckpointV0 {
        replay_from_offset: 0,
        catalog_records: vec![],
        record_payloads: vec![],
        index_entries: vec![],
    };
    let mut bytes = encode_checkpoint_payload_v0(&cp);

    // Unsupported version.
    bytes[0..2].copy_from_slice(&(CHECKPOINT_VERSION_V0 + 1).to_le_bytes());
    let err = decode_checkpoint_payload(&bytes).unwrap_err();
    match err {
        DbError::Format(FormatError::UnsupportedVersion { major, minor }) => {
            assert_eq!(major, 0);
            assert_eq!(minor, CHECKPOINT_VERSION_V0 + 1);
        }
        other => panic!("unexpected error: {other:?}"),
    }

    // Trailing bytes.
    let mut ok = encode_checkpoint_payload_v0(&cp);
    ok.extend_from_slice(&[1, 2, 3]);
    let err = decode_checkpoint_payload(&ok).unwrap_err();
    match err {
        DbError::Format(FormatError::InvalidCatalogPayload { message }) => {
            assert!(message.contains("trailing bytes"));
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn checkpoint_decode_rejects_truncated_and_record_payload_apply_errors() {
    // Truncated: cannot even read the version.
    let err = decode_checkpoint_payload(&[0u8]).unwrap_err();
    match err {
        DbError::Format(FormatError::InvalidCatalogPayload { message }) => {
            assert!(message.contains("unexpected eof"));
        }
        other => panic!("unexpected error: {other:?}"),
    }

    // Build a checkpoint with a record payload that is too short (<6 bytes).
    let cp = CheckpointV0 {
        replay_from_offset: 0,
        catalog_records: vec![],
        record_payloads: vec![vec![1, 2, 3, 4, 5]],
        index_entries: vec![],
    };
    let bytes = encode_checkpoint_payload_v0(&cp);
    let err = state_from_checkpoint_payload(&bytes).unwrap_err();
    match err {
        DbError::Format(FormatError::TruncatedRecordPayload) => {}
        other => panic!("unexpected error: {other:?}"),
    }

    // Record payload long enough to parse a collection id, but catalog has none -> UnknownCollection.
    let cp2 = CheckpointV0 {
        replay_from_offset: 0,
        catalog_records: vec![],
        record_payloads: vec![vec![0, 0, 1, 0, 0, 0]],
        index_entries: vec![],
    };
    let bytes2 = encode_checkpoint_payload_v0(&cp2);
    let err = state_from_checkpoint_payload(&bytes2).unwrap_err();
    match err {
        DbError::Schema(se) => {
            let s = se.to_string();
            assert!(s.contains("unknown collection id"));
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

#[test]
fn validation_error_display_empty_path_vs_nonempty() {
    let e = ValidationError {
        path: vec![],
        message: "bad".to_string(),
    };
    assert_eq!(e.to_string(), "validation error: bad");

    let e2 = ValidationError {
        path: vec!["a".into(), "b".into()],
        message: "bad".to_string(),
    };
    assert_eq!(e2.to_string(), "validation error at a.b: bad");
}

#[test]
fn temp_spill_file_drop_truncates_when_not_finished() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("spill.db");

    // Create a file-backed store so we can observe length after drop.
    let mut store = FileStore::open_locked(&path, OpenMode::ReadWrite).unwrap();
    store.write_all_at(0, &[9u8; 16]).unwrap();
    let base_len = store.len().unwrap();

    {
        let mut spill = TempSpillFile::new(store).unwrap();
        spill.append_temp_segment(b"payload").unwrap();
        assert!(spill.store_mut().len().unwrap() > base_len);
        // Drop without calling finish(): Drop impl should truncate and sync.
    }

    let store2 = FileStore::open_locked(&path, OpenMode::ReadWrite).unwrap();
    assert_eq!(store2.len().unwrap(), base_len);
}

#[test]
fn txn_payload_decode_rejects_wrong_len_wrong_version_and_bad_crc() {
    let ok = encode_txn_payload_v0(123);
    assert_eq!(ok.len(), TXN_PAYLOAD_V0_LEN);
    assert_eq!(decode_txn_payload_v0(&ok).unwrap(), 123);

    // Wrong length.
    let err = decode_txn_payload_v0(&ok[0..TXN_PAYLOAD_V0_LEN - 1]).unwrap_err();
    match err {
        DbError::Format(FormatError::InvalidTxnPayload { message }) => {
            assert!(message.contains("expected"));
        }
        other => panic!("unexpected error: {other:?}"),
    }

    // Wrong version.
    let mut bad_ver = ok;
    bad_ver[0..2].copy_from_slice(&99u16.to_le_bytes());
    let err = decode_txn_payload_v0(&bad_ver).unwrap_err();
    match err {
        DbError::Format(FormatError::InvalidTxnPayload { message }) => {
            assert!(message.contains("unsupported"));
        }
        other => panic!("unexpected error: {other:?}"),
    }

    // Bad crc.
    let mut bad_crc = encode_txn_payload_v0(123);
    bad_crc[21] ^= 0xFF;
    let err = decode_txn_payload_v0(&bad_crc).unwrap_err();
    match err {
        DbError::Format(FormatError::InvalidTxnPayload { message }) => {
            assert!(message.contains("crc"));
        }
        other => panic!("unexpected error: {other:?}"),
    }
}

