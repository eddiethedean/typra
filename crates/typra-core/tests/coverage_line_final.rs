//! Targeted integration tests for remaining strict line-coverage gaps.

use std::borrow::Cow;
use std::collections::BTreeMap;

use typra_core::catalog::{Catalog, CatalogRecordWire};
use typra_core::checkpoint::checkpoint_from_state;
use typra_core::db::row_subset_by_field_defs;
use typra_core::index::IndexState;
use typra_core::record::{
    decode_record_payload_any, decode_row_value, encode_record_payload_v3_op, encode_row_value,
    Cursor, RowValue, ScalarValue, OP_INSERT,
};
use typra_core::config::{OpenOptions, RecoveryMode};
use typra_core::schema::{FieldDef, FieldPath, IndexDef, IndexKind, Type};
use typra_core::{CollectionId, Database, LatestMap};

fn fd(path: &[&str], ty: Type) -> FieldDef {
    FieldDef {
        path: FieldPath(
            path.iter()
                .map(|s| Cow::Owned((*s).to_string()))
                .collect(),
        ),
        ty,
        constraints: vec![],
    }
}

#[test]
fn checkpoint_from_state_propagates_encode_error_on_row_type_mismatch() {
    let mut cat = Catalog::default();
    cat.apply_record(CatalogRecordWire::CreateCollection {
        collection_id: 1,
        name: "t".into(),
        schema_version: 1,
        fields: vec![fd(&["id"], Type::Int64), fd(&["name"], Type::String)],
        indexes: vec![],
        primary_field: Some("id".into()),
    })
    .unwrap();

    let pk_key = ScalarValue::Int64(1).canonical_key_bytes();
    let mut latest = LatestMap::new();
    let mut row = BTreeMap::new();
    row.insert("id".into(), RowValue::Int64(1));
    row.insert("name".into(), RowValue::Int64(999));
    latest.insert((1, pk_key), row);

    let err = checkpoint_from_state(&cat, &latest, &IndexState::default()).unwrap_err();
    assert!(matches!(err, typra_core::DbError::Format(_)));
}

#[test]
fn v3_decode_nested_path_roundtrips() {
    let fields = vec![
        fd(&["id"], Type::Int64),
        fd(&["meta", "k"], Type::String),
    ];
    let pk = ScalarValue::Int64(1);
    let non_pk = vec![(
        fd(&["meta", "k"], Type::String),
        RowValue::String("v".into()),
    )];
    let bytes = encode_record_payload_v3_op(1, 1, OP_INSERT, &pk, &Type::Int64, &non_pk).unwrap();
    let got = decode_record_payload_any(&bytes, "id", &Type::Int64, &fields).unwrap();
    match got.fields.get("meta") {
        Some(RowValue::Object(m)) => {
            assert_eq!(m.get("k"), Some(&RowValue::String("v".into())));
        }
        _ => panic!("expected meta object"),
    }
}

#[test]
fn row_value_scalar_roundtrips_and_encode_rejects_mismatch() {
    for s in [
        ScalarValue::Bool(true),
        ScalarValue::Int64(-3),
        ScalarValue::Uint64(9),
        ScalarValue::Float64(1.5),
        ScalarValue::String("x".into()),
        ScalarValue::Bytes(vec![1, 2]),
        ScalarValue::Uuid([7u8; 16]),
        ScalarValue::Timestamp(99),
    ] {
        let rv = RowValue::from_scalar(s.clone());
        assert_eq!(rv.as_scalar(), Some(s));
    }

    let mut cur = Cursor::new(&[]);
    assert!(decode_row_value(&mut cur, &Type::Enum(vec!["a".into()])).is_err());

    assert!(RowValue::Object(BTreeMap::new()).into_scalar().is_err());
    let mut out = Vec::new();
    assert!(encode_row_value(&mut out, &RowValue::Int64(1), &Type::String).is_err());
}

#[test]
fn row_subset_merges_nested_fields_under_shared_root() {
    let wanted = vec![fd(&["a", "b"], Type::Int64), fd(&["a", "c"], Type::String)];
    let mut row = BTreeMap::new();
    row.insert(
        "a".into(),
        RowValue::Object(BTreeMap::from([
            ("b".into(), RowValue::Int64(2)),
            ("c".into(), RowValue::String("hi".into())),
        ])),
    );
    let out = row_subset_by_field_defs(&row, &wanted);
    match out.get("a") {
        Some(RowValue::Object(m)) => {
            assert_eq!(m.get("b"), Some(&RowValue::Int64(2)));
            assert_eq!(m.get("c"), Some(&RowValue::String("hi".into())));
        }
        _ => panic!("expected merged a"),
    }
}

#[test]
fn nested_schema_insert_delete_and_file_checkpoint() {
    let mut db = Database::open_in_memory().unwrap();
    let fields = vec![
        fd(&["id"], Type::Int64),
        fd(&["meta", "k"], Type::String),
    ];
    let (cid, _) = db
        .register_collection_with_indexes("docs", fields, vec![], "id")
        .unwrap();
    let mut row = BTreeMap::new();
    row.insert("id".into(), RowValue::Int64(1));
    row.insert(
        "meta".into(),
        RowValue::Object(BTreeMap::from([("k".into(), RowValue::String("v".into()))])),
    );
    db.insert(cid, row).unwrap();
    db.delete(cid, &ScalarValue::Int64(1)).unwrap();

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("ck.typra");
    let mut disk = Database::open(&path).unwrap();
    disk.register_collection(
        "x",
        vec![fd(&["id"], Type::Int64), fd(&["y"], Type::String)],
        "id",
    )
    .unwrap();
    disk.insert(
        CollectionId(1),
        BTreeMap::from([
            ("id".into(), RowValue::Int64(1)),
            ("y".into(), RowValue::String("z".into())),
        ]),
    )
    .unwrap();
    disk.checkpoint().unwrap();
}

#[test]
fn transaction_register_insert_commit_exercises_staging_paths() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("txn_cov.typra");
    let mut db = Database::open(&path).unwrap();

    db.begin_transaction().unwrap();
    db.register_collection("t", vec![fd(&["id"], Type::Int64)], "id")
        .unwrap();
    let cid = CollectionId(1);
    db.insert(
        cid,
        BTreeMap::from([("id".into(), RowValue::Int64(1))]),
    )
    .unwrap();
    db.commit_transaction().unwrap();

    let db2 = Database::open(&path).unwrap();
    assert!(db2.get(cid, &ScalarValue::Int64(1)).unwrap().is_some());
}

#[test]
fn plan_migration_steps_cover_backfill_and_rebuild_branches() {
    let mut db = Database::open_in_memory().unwrap();
    let v1 = vec![fd(&["id"], Type::Int64)];
    let (cid, _) = db.register_collection("t", v1, "id").unwrap();
    db.insert(
        cid,
        BTreeMap::from([("id".into(), RowValue::Int64(1))]),
    )
    .unwrap();

    let v2 = vec![fd(&["id"], Type::Int64), fd(&["extra"], Type::String)];
    let plan = db
        .plan_schema_version_with_indexes(cid, v2.clone(), vec![])
        .unwrap();
    assert!(format!("{:?}", plan.change).contains("NeedsMigration"));
    assert!(plan.steps.iter().any(|s| format!("{s:?}").contains("Backfill")));

    let mut db2 = Database::open_in_memory().unwrap();
    let (cid2, _) = db2
        .register_collection(
            "u",
            vec![fd(&["id"], Type::Int64), fd(&["sku"], Type::String)],
            "id",
        )
        .unwrap();
    let plan2 = db2
        .plan_schema_version_with_indexes(
            cid2,
            vec![fd(&["id"], Type::Int64), fd(&["sku"], Type::String)],
            vec![IndexDef {
                name: "sku_u".into(),
                path: FieldPath(vec![Cow::Owned("sku".into())]),
                kind: IndexKind::Unique,
            }],
        )
        .unwrap();
    assert!(plan2.steps.iter().any(|s| format!("{s:?}").contains("Rebuild")));
}

#[test]
fn compact_snapshot_bumps_multiple_schema_versions() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("c.typra");
    let dest = dir.path().join("out.typra");
    let mut db = Database::open(&path).unwrap();
    let (cid, _) = db
        .register_collection(
            "t",
            vec![fd(&["id"], Type::Int64), fd(&["a"], Type::Int64)],
            "id",
        )
        .unwrap();
    db.register_schema_version_with_indexes(
        cid,
        vec![
            fd(&["id"], Type::Int64),
            fd(&["a"], Type::Int64),
            fd(
                &["extra"],
                Type::Optional(Box::new(Type::String)),
            ),
        ],
        vec![],
    )
    .unwrap();

    db.compact_to(&dest).unwrap();
    let db2 = Database::open(&dest).unwrap();
    let c = db2.catalog().get(cid).unwrap();
    assert_eq!(c.current_version.0, 2);
}

#[test]
fn vec_store_export_snapshot_writes_file() {
    let mut db = Database::open_in_memory().unwrap();
    db.register_collection("t", vec![fd(&["id"], Type::Int64)], "id")
        .unwrap();
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("snap.bin");
    db.export_snapshot_to_path(&p).unwrap();
    assert!(std::fs::metadata(&p).unwrap().len() > 0);
}

#[test]
fn restore_snapshot_replaces_existing_destination() {
    let dir = tempfile::tempdir().unwrap();
    let snap = dir.path().join("snap.typra");
    let dest = dir.path().join("live.typra");
    {
        let mut db = Database::open(&snap).unwrap();
        db.register_collection("t", vec![fd(&["id"], Type::Int64)], "id")
            .unwrap();
    }
    std::fs::write(&dest, b"placeholder").unwrap();
    Database::restore_snapshot_to_path(&snap, &dest).unwrap();
    let db = Database::open(&dest).unwrap();
    assert!(db.catalog().collection_names().contains(&"t".to_string()));
}

#[test]
fn reopen_replays_committed_tail_after_checkpoint() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("tail_cp.typra");
    let cid = {
        let mut db = Database::open(&path).unwrap();
        let (cid, _) = db.register_collection("t", vec![fd(&["id"], Type::Int64)], "id")
            .unwrap();
        db.insert(
            cid,
            BTreeMap::from([("id".into(), RowValue::Int64(1))]),
        )
        .unwrap();
        db.checkpoint().unwrap();
        db.insert(
            cid,
            BTreeMap::from([("id".into(), RowValue::Int64(2))]),
        )
        .unwrap();
        cid
    };
    let db2 = Database::open(&path).unwrap();
    assert!(db2
        .get(cid, &ScalarValue::Int64(2))
        .unwrap()
        .is_some());
}

#[test]
fn autotruncate_open_recoveres_from_bad_checkpoint_payload() {
    use typra_core::segments::header::SEGMENT_HEADER_LEN;
    use typra_core::superblock::{decode_superblock, SUPERBLOCK_SIZE};
    use typra_core::file_format::FILE_HEADER_SIZE;

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("bad_cp.typra");
    {
        let mut db = Database::open(&path).unwrap();
        db.register_collection("t", vec![fd(&["id"], Type::Int64)], "id")
            .unwrap();
        db.checkpoint().unwrap();
    }

    let mut bytes = std::fs::read(&path).unwrap();
    let sb_a_off = FILE_HEADER_SIZE;
    let sb_b_off = FILE_HEADER_SIZE + SUPERBLOCK_SIZE;
    let sa = decode_superblock(&bytes[sb_a_off..sb_a_off + SUPERBLOCK_SIZE]).ok();
    let sb = decode_superblock(&bytes[sb_b_off..sb_b_off + SUPERBLOCK_SIZE]).ok();
    let picked = match (sa, sb) {
        (Some(a), Some(b)) => {
            if a.generation >= b.generation {
                a
            } else {
                b
            }
        }
        (Some(a), None) => a,
        (None, Some(b)) => b,
        (None, None) => panic!("no superblock"),
    };
    assert!(picked.checkpoint_offset > 0);
    let sb = picked;
    let seg_start = sb.checkpoint_offset as usize;
    let payload_off = seg_start + SEGMENT_HEADER_LEN;
    assert!(payload_off < bytes.len());
    bytes[payload_off] ^= 0xFF;
    std::fs::write(&path, &bytes).unwrap();

    let strict = OpenOptions {
        recovery: RecoveryMode::Strict,
        ..OpenOptions::default()
    };
    assert!(Database::open_with_options(&path, strict).is_err());

    let auto = OpenOptions {
        recovery: RecoveryMode::AutoTruncate,
        ..OpenOptions::default()
    };
    let _ = Database::open_with_options(&path, auto).unwrap();
}

#[test]
fn delete_inside_transaction_commits_on_disk() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("del_txn.typra");
    let mut db = Database::open(&path).unwrap();
    let (cid, _) = db.register_collection("t", vec![fd(&["id"], Type::Int64)], "id")
        .unwrap();
    db.insert(
        cid,
        BTreeMap::from([("id".into(), RowValue::Int64(1))]),
    )
    .unwrap();
    db.begin_transaction().unwrap();
    db.delete(cid, &ScalarValue::Int64(1)).unwrap();
    db.commit_transaction().unwrap();
    drop(db);

    let db2 = Database::open(&path).unwrap();
    assert!(db2.get(cid, &ScalarValue::Int64(1)).unwrap().is_none());
}
