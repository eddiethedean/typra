//! AutoTruncate opens databases with txn id mismatch at commit.

use modelvault_core::config::{OpenOptions, RecoveryMode};
use modelvault_core::segments::header::{SegmentHeader, SegmentType};
use modelvault_core::segments::writer::SegmentWriter;
use modelvault_core::storage::Store;
use modelvault_core::txn::encode_txn_payload_v0;
use modelvault_core::Database;
use modelvault_core::FieldDef;
use modelvault_core::Type;
use std::borrow::Cow;
use std::collections::BTreeMap;

use modelvault_core::record::{RowValue, ScalarValue};
use modelvault_core::schema::FieldPath;

#[test]
fn auto_truncate_opens_after_txn_id_mismatch_and_prefix_is_readable() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("db.modelvault");

    {
        let mut db = Database::open(&path).unwrap();
        let fields = vec![FieldDef {
            path: FieldPath::new([Cow::Borrowed("id")]).unwrap(),
            ty: Type::Int64,
            constraints: vec![],
        }];
        let (cid, _) = db.register_collection("t", fields, "id").unwrap();
        let mut row = BTreeMap::new();
        row.insert("id".to_string(), RowValue::Int64(1));
        db.insert(cid, row).unwrap();
    }

    let mut bytes = std::fs::read(&path).unwrap();
    let mut store = modelvault_core::storage::VecStore::from_vec(bytes.clone());
    let tail_off = store.len().unwrap();
    let mut w = SegmentWriter::new(&mut store, tail_off);
    let pb = encode_txn_payload_v0(1);
    let pm = encode_txn_payload_v0(2);
    let _ = w
        .append(
            SegmentHeader {
                segment_type: SegmentType::TxnBegin,
                payload_len: 0,
                payload_crc32c: 0,
            },
            &pb,
        )
        .unwrap();
    let _ = w
        .append(
            SegmentHeader {
                segment_type: SegmentType::TxnCommit,
                payload_len: 0,
                payload_crc32c: 0,
            },
            &pm,
        )
        .unwrap();
    bytes = store.into_inner();

    std::fs::write(&path, &bytes).unwrap();

    let opts = OpenOptions {
        recovery: RecoveryMode::AutoTruncate,
        ..OpenOptions::default()
    };
    let db = Database::open_with_options(&path, opts).unwrap();
    let cid = db.collection_id_named("t").unwrap();
    let row = db
        .get(cid, &ScalarValue::Int64(1))
        .unwrap()
        .expect("row from prefix before bad txn");
    assert!(matches!(row.get("id"), Some(RowValue::Int64(1))));
}
