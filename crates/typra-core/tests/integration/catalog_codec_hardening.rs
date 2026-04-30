//! Decode hardening and rich roundtrips for catalog segment payloads.

use std::borrow::Cow;

use typra_core::catalog::{
    decode_catalog_payload, encode_catalog_payload, CatalogRecordWire, CATALOG_PAYLOAD_VERSION,
    CATALOG_PAYLOAD_VERSION_V1, ENTRY_KIND_CREATE_COLLECTION, MAX_COLLECTION_NAME_BYTES,
};
use typra_core::error::FormatError;
use typra_core::schema::{FieldDef, FieldPath, Type};
use typra_core::DbError;
use typra_core::SchemaError;

fn path(parts: &[&str]) -> FieldPath {
    FieldPath(parts.iter().map(|s| Cow::Owned(s.to_string())).collect())
}

#[test]
fn decode_rejects_unknown_payload_version() {
    let mut b = vec![0u8; 4];
    b[0..2].copy_from_slice(&999u16.to_le_bytes());
    b[2..4].copy_from_slice(&ENTRY_KIND_CREATE_COLLECTION.to_le_bytes());
    let err = decode_catalog_payload(&b).unwrap_err();
    assert!(matches!(
        err,
        DbError::Format(FormatError::InvalidCatalogPayload { .. })
    ));
}

#[test]
fn decode_rejects_unknown_entry_kind() {
    let mut b = vec![0u8; 6];
    b[0..2].copy_from_slice(&CATALOG_PAYLOAD_VERSION.to_le_bytes());
    b[2..4].copy_from_slice(&999u16.to_le_bytes());
    let err = decode_catalog_payload(&b).unwrap_err();
    assert!(matches!(
        err,
        DbError::Format(FormatError::InvalidCatalogPayload { .. })
    ));
}

#[test]
fn decode_rejects_trailing_garbage_after_valid_payload() {
    let rec = CatalogRecordWire::NewSchemaVersion {
        collection_id: 1,
        schema_version: 2,
        fields: vec![],
        indexes: vec![],
    };
    let mut bytes = encode_catalog_payload(&rec);
    bytes.push(0xff);
    let err = decode_catalog_payload(&bytes).unwrap_err();
    assert!(matches!(
        err,
        DbError::Format(FormatError::InvalidCatalogPayload { .. })
    ));
}

#[test]
fn decode_rejects_empty_slice() {
    let err = decode_catalog_payload(&[]).unwrap_err();
    assert!(matches!(
        err,
        DbError::Format(FormatError::InvalidCatalogPayload { .. })
    ));
}

#[test]
fn decode_rejects_truncated_mid_stream() {
    let rec = CatalogRecordWire::CreateCollection {
        collection_id: 1,
        name: "x".to_string(),
        schema_version: 1,
        fields: vec![],
        indexes: vec![],
        primary_field: None,
    };
    let full = encode_catalog_payload(&rec);
    for take in 1..full.len() {
        let err = decode_catalog_payload(&full[..take]).unwrap_err();
        assert!(
            matches!(
                err,
                DbError::Format(FormatError::InvalidCatalogPayload { .. })
            ),
            "take={take} got {err:?}"
        );
    }
}

#[test]
fn roundtrip_all_primitive_field_types() {
    let fields = vec![
        FieldDef {
            path: path(&["a"]),
            ty: Type::Bool,
            constraints: vec![],
        },
        FieldDef {
            path: path(&["b"]),
            ty: Type::Int64,
            constraints: vec![],
        },
        FieldDef {
            path: path(&["c"]),
            ty: Type::Uint64,
            constraints: vec![],
        },
        FieldDef {
            path: path(&["d"]),
            ty: Type::Float64,
            constraints: vec![],
        },
        FieldDef {
            path: path(&["e"]),
            ty: Type::String,
            constraints: vec![],
        },
        FieldDef {
            path: path(&["f"]),
            ty: Type::Bytes,
            constraints: vec![],
        },
        FieldDef {
            path: path(&["g"]),
            ty: Type::Uuid,
            constraints: vec![],
        },
        FieldDef {
            path: path(&["h"]),
            ty: Type::Timestamp,
            constraints: vec![],
        },
    ];
    let rec = CatalogRecordWire::CreateCollection {
        collection_id: 1,
        name: "prim".to_string(),
        schema_version: 1,
        fields,
        indexes: vec![],
        primary_field: Some("a".to_string()),
    };
    let bytes = encode_catalog_payload(&rec);
    let got = decode_catalog_payload(&bytes).unwrap();
    assert_eq!(got, rec);
}

#[test]
fn roundtrip_nested_types_optional_list_object_enum() {
    let inner_obj = Type::Object(vec![FieldDef {
        path: path(&["n"]),
        ty: Type::String,
        constraints: vec![],
    }]);
    let fields = vec![
        FieldDef {
            path: path(&["opt"]),
            ty: Type::Optional(Box::new(Type::Int64)),
            constraints: vec![],
        },
        FieldDef {
            path: path(&["tags"]),
            ty: Type::List(Box::new(Type::String)),
            constraints: vec![],
        },
        FieldDef {
            path: path(&["profile"]),
            ty: inner_obj,
            constraints: vec![],
        },
        FieldDef {
            path: path(&["kind"]),
            ty: Type::Enum(vec!["a".to_string(), "b".to_string()]),
            constraints: vec![],
        },
    ];
    let rec = CatalogRecordWire::CreateCollection {
        collection_id: 1,
        name: "nested".to_string(),
        schema_version: 1,
        fields,
        indexes: vec![],
        primary_field: Some("opt".to_string()),
    };
    let bytes = encode_catalog_payload(&rec);
    let got = decode_catalog_payload(&bytes).unwrap();
    assert_eq!(got, rec);
}

#[test]
fn decode_rejects_invalid_utf8_in_collection_name() {
    // Valid header + create kind + id + name_len 1 + invalid UTF-8 byte + schema_version + empty fields
    let mut b = Vec::new();
    b.extend_from_slice(&CATALOG_PAYLOAD_VERSION.to_le_bytes());
    b.extend_from_slice(&ENTRY_KIND_CREATE_COLLECTION.to_le_bytes());
    b.extend_from_slice(&1u32.to_le_bytes());
    b.extend_from_slice(&1u32.to_le_bytes()); // name len
    b.push(0xff);
    b.extend_from_slice(&1u32.to_le_bytes()); // schema_version must be 1 for apply, decode still reads
    b.extend_from_slice(&0u32.to_le_bytes()); // 0 fields
    b.extend_from_slice(&0u32.to_le_bytes()); // 0 indexes (v4+)
    b.extend_from_slice(&0u32.to_le_bytes()); // v2 optional primary absent
    let err = decode_catalog_payload(&b).unwrap_err();
    assert!(matches!(
        err,
        DbError::Format(FormatError::InvalidCatalogPayload { .. })
    ));
}

/// v1 catalog entries omit the optional primary name tail; decoder must take the `None` branch.
#[test]
fn decode_v1_create_collection_has_no_primary_field_tail() {
    let mut b = Vec::new();
    b.extend_from_slice(&CATALOG_PAYLOAD_VERSION_V1.to_le_bytes());
    b.extend_from_slice(&ENTRY_KIND_CREATE_COLLECTION.to_le_bytes());
    b.extend_from_slice(&1u32.to_le_bytes());
    b.extend_from_slice(&1u32.to_le_bytes()); // name len
    b.push(b'x');
    b.extend_from_slice(&1u32.to_le_bytes()); // schema_version
    b.extend_from_slice(&0u32.to_le_bytes()); // 0 fields
    let rec = decode_catalog_payload(&b).unwrap();
    match rec {
        CatalogRecordWire::CreateCollection {
            primary_field: None,
            name,
            ..
        } => assert_eq!(name, "x"),
        _ => panic!("expected create"),
    }
}

#[test]
fn decode_v2_create_rejects_trailing_bytes_after_primary() {
    let rec = CatalogRecordWire::CreateCollection {
        collection_id: 1,
        name: "c".to_string(),
        schema_version: 1,
        fields: vec![],
        indexes: vec![],
        primary_field: None,
    };
    let mut bytes = encode_catalog_payload(&rec);
    bytes.extend_from_slice(&[0xff, 0xfe]);
    let err = decode_catalog_payload(&bytes).unwrap_err();
    assert!(matches!(
        err,
        DbError::Format(FormatError::InvalidCatalogPayload { .. })
    ));
}

#[test]
fn decode_rejects_optional_primary_name_too_long() {
    let mut b = Vec::new();
    b.extend_from_slice(&CATALOG_PAYLOAD_VERSION.to_le_bytes());
    b.extend_from_slice(&ENTRY_KIND_CREATE_COLLECTION.to_le_bytes());
    b.extend_from_slice(&1u32.to_le_bytes());
    b.extend_from_slice(&1u32.to_le_bytes());
    b.push(b'x');
    b.extend_from_slice(&1u32.to_le_bytes());
    b.extend_from_slice(&0u32.to_le_bytes()); // fields
    b.extend_from_slice(&0u32.to_le_bytes()); // indexes (v4+)
    let too_long = MAX_COLLECTION_NAME_BYTES + 1;
    b.extend_from_slice(&(too_long as u32).to_le_bytes());
    b.extend(vec![b'a'; too_long]);
    let err = decode_catalog_payload(&b).unwrap_err();
    assert!(matches!(
        err,
        DbError::Format(FormatError::InvalidCatalogPayload { .. })
    ));
}

#[test]
fn decode_rejects_empty_collection_name_length() {
    let mut b = Vec::new();
    b.extend_from_slice(&CATALOG_PAYLOAD_VERSION.to_le_bytes());
    b.extend_from_slice(&ENTRY_KIND_CREATE_COLLECTION.to_le_bytes());
    b.extend_from_slice(&1u32.to_le_bytes());
    b.extend_from_slice(&0u32.to_le_bytes()); // name len 0
    let err = decode_catalog_payload(&b).unwrap_err();
    assert!(matches!(
        err,
        DbError::Format(FormatError::InvalidCatalogPayload { .. })
    ));
}

#[test]
fn decode_rejects_collection_name_too_long() {
    let mut b = Vec::new();
    b.extend_from_slice(&CATALOG_PAYLOAD_VERSION.to_le_bytes());
    b.extend_from_slice(&ENTRY_KIND_CREATE_COLLECTION.to_le_bytes());
    b.extend_from_slice(&1u32.to_le_bytes());
    let n = MAX_COLLECTION_NAME_BYTES + 1;
    b.extend_from_slice(&(n as u32).to_le_bytes());
    b.extend(vec![b'b'; n]);
    let err = decode_catalog_payload(&b).unwrap_err();
    assert!(matches!(
        err,
        DbError::Format(FormatError::InvalidCatalogPayload { .. })
    ));
}

#[test]
fn decode_rejects_field_path_with_zero_segments() {
    let mut b = Vec::new();
    b.extend_from_slice(&CATALOG_PAYLOAD_VERSION.to_le_bytes());
    b.extend_from_slice(&ENTRY_KIND_CREATE_COLLECTION.to_le_bytes());
    b.extend_from_slice(&1u32.to_le_bytes());
    b.extend_from_slice(&1u32.to_le_bytes());
    b.push(b'x');
    b.extend_from_slice(&1u32.to_le_bytes());
    b.extend_from_slice(&1u32.to_le_bytes()); // 1 field
    b.extend_from_slice(&0u32.to_le_bytes()); // path: 0 segments
    let err = decode_catalog_payload(&b).unwrap_err();
    assert!(matches!(
        err,
        DbError::Schema(SchemaError::InvalidFieldPath)
    ));
}

#[test]
fn decode_rejects_empty_field_path_segment() {
    let mut b = Vec::new();
    b.extend_from_slice(&CATALOG_PAYLOAD_VERSION.to_le_bytes());
    b.extend_from_slice(&ENTRY_KIND_CREATE_COLLECTION.to_le_bytes());
    b.extend_from_slice(&1u32.to_le_bytes());
    b.extend_from_slice(&1u32.to_le_bytes());
    b.push(b'x');
    b.extend_from_slice(&1u32.to_le_bytes());
    b.extend_from_slice(&1u32.to_le_bytes()); // 1 field
    b.extend_from_slice(&1u32.to_le_bytes()); // 1 segment
    b.extend_from_slice(&0u32.to_le_bytes()); // segment len 0
    let err = decode_catalog_payload(&b).unwrap_err();
    assert!(matches!(
        err,
        DbError::Schema(SchemaError::InvalidFieldPath)
    ));
}

#[test]
fn decode_rejects_unknown_field_type_tag() {
    let mut b = Vec::new();
    b.extend_from_slice(&CATALOG_PAYLOAD_VERSION.to_le_bytes());
    b.extend_from_slice(&ENTRY_KIND_CREATE_COLLECTION.to_le_bytes());
    b.extend_from_slice(&1u32.to_le_bytes());
    b.extend_from_slice(&1u32.to_le_bytes());
    b.push(b'x');
    b.extend_from_slice(&1u32.to_le_bytes());
    b.extend_from_slice(&1u32.to_le_bytes()); // 1 field
    b.extend_from_slice(&1u32.to_le_bytes()); // 1 path segment
    b.extend_from_slice(&1u32.to_le_bytes());
    b.push(b'f');
    b.push(200u8); // unknown type tag
    b.extend_from_slice(&0u32.to_le_bytes()); // 0 indexes (v4+)
    b.extend_from_slice(&0u32.to_le_bytes()); // v2 optional primary absent
    let err = decode_catalog_payload(&b).unwrap_err();
    assert!(matches!(
        err,
        DbError::Format(FormatError::InvalidCatalogPayload { .. })
    ));
}

#[test]
fn decode_rejects_optional_type_when_inner_tag_missing() {
    let mut b = Vec::new();
    b.extend_from_slice(&CATALOG_PAYLOAD_VERSION.to_le_bytes());
    b.extend_from_slice(&ENTRY_KIND_CREATE_COLLECTION.to_le_bytes());
    b.extend_from_slice(&1u32.to_le_bytes());
    b.extend_from_slice(&1u32.to_le_bytes());
    b.push(b'x');
    b.extend_from_slice(&1u32.to_le_bytes());
    b.extend_from_slice(&1u32.to_le_bytes()); // 1 field
    b.extend_from_slice(&1u32.to_le_bytes());
    b.extend_from_slice(&1u32.to_le_bytes());
    b.push(b'f');
    b.push(8u8); // TAG_OPTIONAL — truncated before inner type tag
    b.extend_from_slice(&0u32.to_le_bytes()); // 0 indexes (v4+)
    b.extend_from_slice(&0u32.to_le_bytes()); // v2 optional primary absent
    let err = decode_catalog_payload(&b).unwrap_err();
    assert!(matches!(
        err,
        DbError::Format(FormatError::InvalidCatalogPayload { .. })
    ));
}
