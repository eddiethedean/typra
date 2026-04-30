use typra_core::catalog::{decode_catalog_payload, encode_catalog_payload, CatalogRecordWire};
use typra_core::error::DbError;
use typra_core::schema::{FieldPath, IndexDef, IndexKind};

fn v4_create_with_one_index() -> Vec<u8> {
    let wire = CatalogRecordWire::CreateCollection {
        collection_id: 1,
        name: "a".into(),
        schema_version: 1,
        fields: vec![],
        indexes: vec![IndexDef {
            name: "i".into(),
            path: FieldPath(vec![std::borrow::Cow::Borrowed("x")]),
            kind: IndexKind::Unique,
        }],
        primary_field: Some("id".into()),
    };
    encode_catalog_payload(&wire)
}

fn indexes_count_pos_for_name_len(name_len: usize) -> usize {
    // ver(u16) kind(u16) cid(u32) name_len(u32) name schema_ver(u32) fields_count(u32)
    2 + 2 + 4 + 4 + name_len + 4 + 4
}

#[test]
fn decode_indexes_rejects_unknown_kind_tag() {
    let mut b = v4_create_with_one_index();
    let count_pos = indexes_count_pos_for_name_len(1);
    let kind_tag_pos = count_pos + 4 /*count*/ + 0 /*start entry*/;
    // After count u32, first byte is kind_tag.
    b[kind_tag_pos] = 9;
    let err = decode_catalog_payload(&b).unwrap_err();
    assert!(matches!(err, DbError::Format(_)));
}

#[test]
fn decode_indexes_rejects_empty_index_name() {
    let mut b = v4_create_with_one_index();
    let count_pos = indexes_count_pos_for_name_len(1);
    let kind_tag_pos = count_pos + 4;
    // kind_tag + field_path (for ["x"] is 1 + u16(1) + 'x' => 4 bytes total)
    let name_len_pos = kind_tag_pos + 1 + 4;
    b[name_len_pos..name_len_pos + 4].copy_from_slice(&0u32.to_le_bytes());
    let err = decode_catalog_payload(&b).unwrap_err();
    assert!(matches!(err, DbError::Format(_) | DbError::Schema(_)));
}

#[test]
fn decode_indexes_rejects_index_name_too_long() {
    let mut b = v4_create_with_one_index();
    let count_pos = indexes_count_pos_for_name_len(1);
    let kind_tag_pos = count_pos + 4;
    let name_len_pos = kind_tag_pos + 1 + 4;
    b[name_len_pos..name_len_pos + 4].copy_from_slice(&(10_000u32).to_le_bytes());
    let err = decode_catalog_payload(&b).unwrap_err();
    assert!(matches!(err, DbError::Format(_)));
}

#[test]
fn decode_indexes_rejects_invalid_utf8_in_index_name() {
    let mut b = v4_create_with_one_index();
    let count_pos = indexes_count_pos_for_name_len(1);
    let kind_tag_pos = count_pos + 4;
    let name_len_pos = kind_tag_pos + 1 + 4;
    // name_len=1, and the one byte comes right after it.
    b[name_len_pos..name_len_pos + 4].copy_from_slice(&1u32.to_le_bytes());
    let name_byte_pos = name_len_pos + 4;
    b[name_byte_pos] = 0xFF;
    let err = decode_catalog_payload(&b).unwrap_err();
    assert!(matches!(err, DbError::Format(_)));
}
