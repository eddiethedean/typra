use std::borrow::Cow;
use std::collections::BTreeMap;

use typra_core::query::{Predicate, Query};
use typra_core::schema::{FieldDef, FieldPath, IndexDef, IndexKind, Type};
use typra_core::{Database, RowValue, ScalarValue};

fn fd(path: &[&'static str], ty: Type) -> FieldDef {
    FieldDef {
        path: FieldPath(path.iter().map(|s| Cow::Borrowed(*s)).collect()),
        ty,
        constraints: vec![],
    }
}

#[test]
fn production_journey_register_insert_index_query_txn_reopen_schema_bump_compact_verify() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("app.typra");

    // Create + register.
    {
        let mut db = Database::open(&path).unwrap();
        let fields = vec![fd(&["id"], Type::Int64), fd(&["tag"], Type::String)];
        let indexes = vec![IndexDef {
            name: "tag_idx".to_string(),
            path: FieldPath(vec![Cow::Borrowed("tag")]),
            kind: IndexKind::NonUnique,
        }];
        let (cid, _) = db
            .register_collection_with_indexes("items", fields, indexes, "id")
            .unwrap();

        // Transaction insert.
        db.transaction(|tx| {
            for (id, tag) in [(1i64, "a"), (2, "b"), (3, "a")] {
                let mut row = BTreeMap::new();
                row.insert("id".to_string(), RowValue::Int64(id));
                row.insert("tag".to_string(), RowValue::String(tag.to_string()));
                tx.insert(cid, row)?;
            }
            Ok::<(), typra_core::DbError>(())
        })
        .unwrap();

        // Indexed query.
        let q = Query {
            collection: cid,
            predicate: Some(Predicate::Eq {
                path: FieldPath(vec![Cow::Borrowed("tag")]),
                value: ScalarValue::String("a".to_string()),
            }),
            limit: None,
            order_by: None,
        };
        assert!(db.explain_query(&q).unwrap().contains("IndexLookup"));
        let rows = db.query(&q).unwrap();
        assert_eq!(rows.len(), 2);

        db.checkpoint().unwrap();
    }

    // Reopen and verify state.
    let mut db = Database::open(&path).unwrap();
    let cid = db.collection_id_named("items").unwrap();
    let got = db.get(cid, &ScalarValue::Int64(2)).unwrap().unwrap();
    assert_eq!(got.get("tag"), Some(&RowValue::String("b".to_string())));

    // Safe schema bump: add an optional field.
    let current = db.catalog().get(cid).unwrap();
    let mut fields2 = current.fields.clone();
    fields2.push(fd(&["note"], Type::Optional(Box::new(Type::String))));
    let v2 = db.register_schema_version_with_indexes(cid, fields2, current.indexes.clone());
    assert!(v2.is_ok());

    // Insert row including new optional field.
    let mut row = BTreeMap::new();
    row.insert("id".to_string(), RowValue::Int64(4));
    row.insert("tag".to_string(), RowValue::String("c".to_string()));
    row.insert("note".to_string(), RowValue::String("hi".to_string()));
    db.insert(cid, row).unwrap();

    // Compact + reopen.
    db.compact_in_place().unwrap();
    let db2 = Database::open(&path).unwrap();
    let got2 = db2.get(cid, &ScalarValue::Int64(4)).unwrap().unwrap();
    assert_eq!(got2.get("note"), Some(&RowValue::String("hi".to_string())));

    // Verify segment scan succeeds (integrity-ish).
    let file = std::fs::OpenOptions::new().read(true).open(&path).unwrap();
    let mut store = typra_core::storage::FileStore::new(file);
    let start = (typra_core::file_format::FILE_HEADER_SIZE
        + 2 * typra_core::superblock::SUPERBLOCK_SIZE) as u64;
    let _ = typra_core::segments::reader::scan_segments(&mut store, start).unwrap();
}
