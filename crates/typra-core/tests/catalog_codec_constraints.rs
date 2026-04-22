//! Cover catalog v3 constraint encoding/decoding and hardening branches.

use std::borrow::Cow;

use typra_core::catalog::{decode_catalog_payload, encode_catalog_payload, CatalogRecordWire};
use typra_core::error::{DbError, FormatError};
use typra_core::schema::{Constraint, FieldDef, FieldPath, Type};

fn path(parts: &[&str]) -> FieldPath {
    FieldPath(parts.iter().map(|s| Cow::Owned((*s).to_string())).collect())
}

#[test]
fn roundtrip_create_collection_with_constraints_v3() {
    let rec = CatalogRecordWire::CreateCollection {
        collection_id: 1,
        name: "t".to_string(),
        schema_version: 1,
        fields: vec![
            FieldDef {
                path: path(&["id"]),
                ty: Type::String,
                constraints: vec![],
            },
            FieldDef {
                path: path(&["year"]),
                ty: Type::Int64,
                constraints: vec![Constraint::MinI64(2000), Constraint::MaxI64(2100)],
            },
            FieldDef {
                path: path(&["email"]),
                ty: Type::String,
                constraints: vec![Constraint::Email],
            },
        ],
        primary_field: Some("id".to_string()),
    };
    let bytes = encode_catalog_payload(&rec);
    let got = decode_catalog_payload(&bytes).unwrap();
    assert_eq!(got, rec);
}

#[test]
fn decode_rejects_unknown_constraint_tag() {
    // Start from a valid record, then patch the first constraint tag byte.
    let rec = CatalogRecordWire::CreateCollection {
        collection_id: 1,
        name: "t".to_string(),
        schema_version: 1,
        fields: vec![FieldDef {
            path: path(&["x"]),
            ty: Type::Int64,
            constraints: vec![Constraint::MinI64(0)],
        }],
        primary_field: Some("x".to_string()),
    };
    let mut bytes = encode_catalog_payload(&rec);
    // Find the `CT_MIN_I64` tag (1) and replace with 99.
    let idx = bytes.iter().position(|b| *b == 1).expect("tag byte");
    bytes[idx] = 99;
    let e = decode_catalog_payload(&bytes).unwrap_err();
    assert!(matches!(
        e,
        DbError::Format(FormatError::InvalidCatalogPayload { .. })
    ));
}

#[test]
fn decode_rejects_unknown_type_tag() {
    let rec = CatalogRecordWire::CreateCollection {
        collection_id: 1,
        name: "t".to_string(),
        schema_version: 1,
        fields: vec![FieldDef {
            path: path(&["x"]),
            ty: Type::String,
            constraints: vec![],
        }],
        primary_field: Some("x".to_string()),
    };
    let mut bytes = encode_catalog_payload(&rec);
    // Find the `TAG_STRING` tag (4) and replace with 255.
    let idx = bytes.iter().position(|b| *b == 4).expect("type tag");
    bytes[idx] = 255;
    let e = decode_catalog_payload(&bytes).unwrap_err();
    assert!(matches!(
        e,
        DbError::Format(FormatError::InvalidCatalogPayload { .. })
    ));
}
