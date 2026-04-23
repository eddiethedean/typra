use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};

use typra_core::index::{decode_index_payload, encode_index_payload, IndexEntry, IndexState};
use typra_core::query::{Predicate, Query};
use typra_core::record::RowValue;
use typra_core::schema::{Constraint, FieldDef, FieldPath, IndexDef, IndexKind, Type};
use typra_core::validation::{ensure_pk_type_primitive, validate_top_level_row, validate_value};
use typra_core::{Database, ScalarValue};

fn field(name: &str, ty: Type, constraints: Vec<Constraint>) -> FieldDef {
    FieldDef {
        path: FieldPath(vec![Cow::Owned(name.to_string())]),
        ty,
        constraints,
    }
}

#[test]
fn index_payload_decode_covers_error_branches() {
    // Unsupported version.
    let mut bad_ver = encode_index_payload(&[]);
    bad_ver[0..2].copy_from_slice(&999u16.to_le_bytes());
    assert!(decode_index_payload(&bad_ver).is_err());

    // Unknown kind tag.
    let e = IndexEntry {
        collection_id: 1,
        index_name: "i".to_string(),
        kind: IndexKind::Unique,
        index_key: vec![1],
        pk_key: vec![2],
    };
    let mut bytes = encode_index_payload(&[e]);
    // Patch kind tag byte: after ver(2) + n(4) + collection_id(4) => offset 10.
    bytes[10] = 9;
    assert!(decode_index_payload(&bytes).is_err());

    // Trailing bytes.
    let mut ok = encode_index_payload(&[]);
    ok.extend_from_slice(&[1, 2, 3]);
    assert!(decode_index_payload(&ok).is_err());

    // Unexpected EOF.
    let trunc = encode_index_payload(&[])[..3].to_vec();
    assert!(decode_index_payload(&trunc).is_err());
}

#[test]
fn index_state_non_unique_and_unique_branches() {
    let mut st = IndexState::default();
    // Unique idempotent apply.
    st.apply(IndexEntry {
        collection_id: 1,
        index_name: "u".to_string(),
        kind: IndexKind::Unique,
        index_key: b"k".to_vec(),
        pk_key: b"p".to_vec(),
    })
    .unwrap();
    st.apply(IndexEntry {
        collection_id: 1,
        index_name: "u".to_string(),
        kind: IndexKind::Unique,
        index_key: b"k".to_vec(),
        pk_key: b"p".to_vec(),
    })
    .unwrap();
    assert_eq!(st.unique_lookup(1, "u", b"k"), Some(&b"p"[..]));

    // Non-unique accumulates set.
    st.apply(IndexEntry {
        collection_id: 1,
        index_name: "n".to_string(),
        kind: IndexKind::NonUnique,
        index_key: b"k".to_vec(),
        pk_key: b"p1".to_vec(),
    })
    .unwrap();
    st.apply(IndexEntry {
        collection_id: 1,
        index_name: "n".to_string(),
        kind: IndexKind::NonUnique,
        index_key: b"k".to_vec(),
        pk_key: b"p2".to_vec(),
    })
    .unwrap();
    let got = st.non_unique_lookup(1, "n", b"k").unwrap();
    let set: BTreeSet<Vec<u8>> = got.into_iter().collect();
    assert_eq!(set, BTreeSet::from([b"p1".to_vec(), b"p2".to_vec()]));
}

#[test]
fn validation_covers_more_constraints_and_wrong_type_messages() {
    // ensure_pk_type_primitive rejects composite.
    assert!(ensure_pk_type_primitive(&Type::Optional(Box::new(Type::Int64))).is_err());

    // validate_value wrong type branches.
    let mut path = vec!["x".to_string()];
    assert!(validate_value(&mut path, &Type::Bool, &[], &RowValue::Int64(1)).is_err());

    // Constraints: Min/Max for numeric + length + regex.
    let mut path = vec!["n".to_string()];
    assert!(validate_value(
        &mut path,
        &Type::Int64,
        &[Constraint::MinI64(5)],
        &RowValue::Int64(4)
    )
    .is_err());
    assert!(validate_value(
        &mut path,
        &Type::Uint64,
        &[Constraint::MaxU64(2)],
        &RowValue::Uint64(3)
    )
    .is_err());
    assert!(validate_value(
        &mut path,
        &Type::Float64,
        &[Constraint::MinF64(1.5)],
        &RowValue::Float64(1.0)
    )
    .is_err());

    let mut path = vec!["s".to_string()];
    assert!(validate_value(
        &mut path,
        &Type::String,
        &[Constraint::MinLength(3)],
        &RowValue::String("hi".into())
    )
    .is_err());
    assert!(validate_value(
        &mut path,
        &Type::String,
        &[Constraint::Regex("^a+$".into())],
        &RowValue::String("bbb".into())
    )
    .is_err());

    // Wrong-type constraint application branches.
    let mut path = vec!["w".to_string()];
    assert!(validate_value(
        &mut path,
        &Type::String,
        &[Constraint::MinI64(1)],
        &RowValue::String("x".into())
    )
    .is_err());
}

#[test]
fn query_iter_covers_scan_and_index_paths_with_limits_and_residual() {
    let mut db = Database::open_in_memory().unwrap();
    let fields = vec![
        field("id", Type::Int64, vec![]),
        field("status", Type::String, vec![]),
    ];
    let indexes = vec![IndexDef {
        name: "status_idx".to_string(),
        path: FieldPath(vec![Cow::Borrowed("status")]),
        kind: IndexKind::NonUnique,
    }];
    let (cid, _) = db
        .register_collection_with_indexes("t", fields, indexes, "id")
        .unwrap();
    for (id, st) in [(1, "open"), (2, "open"), (3, "closed")] {
        let mut row = BTreeMap::new();
        row.insert("id".into(), RowValue::Int64(id));
        row.insert("status".into(), RowValue::String(st.into()));
        db.insert(cid, row).unwrap();
    }

    // Index path + residual that filters everything.
    let q = Query {
        collection: cid,
        predicate: Some(Predicate::And(vec![
            Predicate::Eq {
                path: FieldPath(vec![Cow::Borrowed("status")]),
                value: ScalarValue::String("open".into()),
            },
            Predicate::Eq {
                path: FieldPath(vec![Cow::Borrowed("status")]),
                value: ScalarValue::String("nope".into()),
            },
        ])),
        limit: Some(1),
    };
    let rows: Vec<_> = db
        .query_iter(&q)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert!(rows.is_empty());

    // Scan path: predicate on non-indexed field.
    let q2 = Query {
        collection: cid,
        predicate: Some(Predicate::Eq {
            path: FieldPath(vec![Cow::Borrowed("id")]),
            value: ScalarValue::Int64(3),
        }),
        limit: Some(10),
    };
    let rows2 = db
        .query_iter(&q2)
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap();
    assert_eq!(rows2.len(), 1);
    assert_eq!(
        rows2[0].get("status"),
        Some(&RowValue::String("closed".into()))
    );
}

#[test]
fn validate_top_level_row_covers_unknown_field_and_absent_optional() {
    let fields = vec![
        field("id", Type::Int64, vec![]),
        field("note", Type::Optional(Box::new(Type::String)), vec![]),
    ];
    let mut row = BTreeMap::new();
    row.insert("id".into(), RowValue::Int64(1));
    // Optional is absent -> ok.
    validate_top_level_row(&fields, "id", &row).unwrap();

    // Unknown field -> error.
    row.insert("extra".into(), RowValue::Bool(true));
    assert!(validate_top_level_row(&fields, "id", &row).is_err());
}
