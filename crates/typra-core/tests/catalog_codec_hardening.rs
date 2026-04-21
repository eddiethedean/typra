//! Decode hardening and rich roundtrips for catalog segment payloads.

use std::borrow::Cow;

use typra_core::catalog::{
    decode_catalog_payload, encode_catalog_payload, CatalogRecordWire, CATALOG_PAYLOAD_VERSION,
    ENTRY_KIND_CREATE_COLLECTION,
};
use typra_core::error::FormatError;
use typra_core::schema::{FieldDef, FieldPath, Type};
use typra_core::DbError;

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
        },
        FieldDef {
            path: path(&["b"]),
            ty: Type::Int64,
        },
        FieldDef {
            path: path(&["c"]),
            ty: Type::Uint64,
        },
        FieldDef {
            path: path(&["d"]),
            ty: Type::Float64,
        },
        FieldDef {
            path: path(&["e"]),
            ty: Type::String,
        },
        FieldDef {
            path: path(&["f"]),
            ty: Type::Bytes,
        },
        FieldDef {
            path: path(&["g"]),
            ty: Type::Uuid,
        },
        FieldDef {
            path: path(&["h"]),
            ty: Type::Timestamp,
        },
    ];
    let rec = CatalogRecordWire::CreateCollection {
        collection_id: 1,
        name: "prim".to_string(),
        schema_version: 1,
        fields,
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
    }]);
    let fields = vec![
        FieldDef {
            path: path(&["opt"]),
            ty: Type::Optional(Box::new(Type::Int64)),
        },
        FieldDef {
            path: path(&["tags"]),
            ty: Type::List(Box::new(Type::String)),
        },
        FieldDef {
            path: path(&["profile"]),
            ty: inner_obj,
        },
        FieldDef {
            path: path(&["kind"]),
            ty: Type::Enum(vec!["a".to_string(), "b".to_string()]),
        },
    ];
    let rec = CatalogRecordWire::CreateCollection {
        collection_id: 1,
        name: "nested".to_string(),
        schema_version: 1,
        fields,
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
    let err = decode_catalog_payload(&b).unwrap_err();
    assert!(matches!(
        err,
        DbError::Format(FormatError::InvalidCatalogPayload { .. })
    ));
}
