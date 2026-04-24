use std::borrow::Cow;
use std::collections::BTreeMap;

use tempfile::tempdir;
use typra_core::query::{OrderBy, OrderDirection, Predicate, Query};
use typra_core::schema::{FieldDef, FieldPath, IndexDef, IndexKind, Type};
use typra_core::{Database, DbError, RowValue, ScalarValue};

fn field(name: &'static str, ty: Type) -> FieldDef {
    FieldDef {
        path: FieldPath(vec![Cow::Borrowed(name)]),
        ty,
        constraints: vec![],
    }
}

fn insert_product(
    db: &mut Database,
    products: typra_core::schema::CollectionId,
    sku: &str,
    name: &str,
    category: &str,
    price: i64,
    created_at: i64,
) -> Result<(), DbError> {
    let mut row = BTreeMap::new();
    row.insert("sku".to_string(), RowValue::String(sku.to_string()));
    row.insert("name".to_string(), RowValue::String(name.to_string()));
    row.insert(
        "category".to_string(),
        RowValue::String(category.to_string()),
    );
    row.insert("price".to_string(), RowValue::Int64(price));
    row.insert("created_at".to_string(), RowValue::Int64(created_at));
    db.insert(products, row)?;
    Ok(())
}

#[test]
fn e2e_inventory_like_workflow_roundtrips_txn_query_checkpoint_compact_snapshot() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("inventory.typra");
    let snap_path = dir.path().join("inventory.snapshot.typra");

    // Create and populate.
    {
        let mut db = Database::open(&path).unwrap();
        let products_fields = vec![
            field("sku", Type::String),
            field("name", Type::String),
            field("category", Type::String),
            field("price", Type::Int64),
            field("created_at", Type::Int64),
        ];
        let products_indexes = vec![
            IndexDef {
                name: "name_unique".to_string(),
                path: FieldPath(vec![Cow::Borrowed("name")]),
                kind: IndexKind::Unique,
            },
            IndexDef {
                name: "category_idx".to_string(),
                path: FieldPath(vec![Cow::Borrowed("category")]),
                kind: IndexKind::NonUnique,
            },
        ];
        let (products, _) = db
            .register_collection_with_indexes("products", products_fields, products_indexes, "sku")
            .unwrap();

        // Transaction rollback on unique index violation should discard all writes in the txn.
        let res = db.transaction(|tdb| {
            insert_product(tdb, products, "sku1", "Widget", "tools", 199, 10)?;
            insert_product(tdb, products, "sku2", "Widget", "tools", 299, 20)?; // violates name_unique
            Ok(())
        });
        assert!(res.is_err());
        assert!(
            db.get(products, &ScalarValue::String("sku1".to_string()))
                .unwrap()
                .is_none()
        );

        // Now insert a valid set.
        db.transaction(|tdb| {
            insert_product(tdb, products, "sku1", "Widget", "tools", 199, 10)?;
            insert_product(tdb, products, "sku2", "Gadget", "tools", 299, 20)?;
            insert_product(tdb, products, "sku3", "Book", "media", 25, 30)?;
            insert_product(tdb, products, "sku4", "Premium", "tools", 499, 40)?;
            Ok(())
        })
        .unwrap();

        // Query: price range + order_by + limit.
        let q = Query {
            collection: products,
            predicate: Some(Predicate::And(vec![
                Predicate::Gte {
                    path: FieldPath(vec![Cow::Borrowed("price")]),
                    value: ScalarValue::Int64(100),
                },
                Predicate::Lt {
                    path: FieldPath(vec![Cow::Borrowed("price")]),
                    value: ScalarValue::Int64(400),
                },
            ])),
            limit: Some(2),
            order_by: Some(OrderBy {
                path: FieldPath(vec![Cow::Borrowed("created_at")]),
                direction: OrderDirection::Desc,
            }),
        };
        let rows = db.query(&q).unwrap();
        assert_eq!(rows.len(), 2);
        let created: Vec<i64> = rows
            .iter()
            .map(|r| match r.get("created_at").unwrap() {
                RowValue::Int64(x) => *x,
                _ => panic!("expected int64 created_at"),
            })
            .collect();
        assert_eq!(created, vec![20, 10]);

        db.checkpoint().unwrap();
        db.export_snapshot_to_path(&snap_path).unwrap();
    }

    // Reopen from file and ensure query still holds.
    {
        let db = Database::open(&path).unwrap();
        let products = db.collection_id_named("products").unwrap();
        let q = Query {
            collection: products,
            predicate: Some(Predicate::Eq {
                path: FieldPath(vec![Cow::Borrowed("category")]),
                value: ScalarValue::String("tools".to_string()),
            }),
            limit: None,
            order_by: Some(OrderBy {
                path: FieldPath(vec![Cow::Borrowed("price")]),
                direction: OrderDirection::Asc,
            }),
        };
        let rows = db.query(&q).unwrap();
        let skus: Vec<String> = rows
            .iter()
            .map(|r| match r.get("sku").unwrap() {
                RowValue::String(s) => s.clone(),
                _ => panic!("expected string sku"),
            })
            .collect();
        assert_eq!(skus, vec!["sku1".to_string(), "sku2".to_string(), "sku4".to_string()]);
    }

    // Compact in place and ensure reopen still works.
    {
        let mut db = Database::open(&path).unwrap();
        db.compact_in_place().unwrap();
    }
    {
        let db = Database::open(&path).unwrap();
        let products = db.collection_id_named("products").unwrap();
        let got = db
            .get(products, &ScalarValue::String("sku3".to_string()))
            .unwrap()
            .unwrap();
        assert_eq!(
            got.get("name"),
            Some(&RowValue::String("Book".to_string()))
        );
    }

    // Snapshot restore: open snapshot file into memory and validate a couple reads.
    {
        let snap = Database::open_snapshot_path(&snap_path).unwrap();
        let products = snap.collection_id_named("products").unwrap();
        let got = snap
            .get(products, &ScalarValue::String("sku2".to_string()))
            .unwrap()
            .unwrap();
        assert_eq!(
            got.get("name"),
            Some(&RowValue::String("Gadget".to_string()))
        );
    }
}

