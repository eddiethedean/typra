use std::borrow::Cow;
use std::collections::BTreeMap;

use typra_core::record::RowValue;
use typra_core::schema::{DbModel, FieldDef, FieldPath, IndexDef, IndexKind, Type};
use typra_core::{CollectionId, Database, ScalarValue};

fn field(name: &str, ty: Type) -> FieldDef {
    FieldDef {
        path: FieldPath(vec![Cow::Owned(name.to_string())]),
        ty,
        constraints: vec![],
    }
}

struct Orders;

impl DbModel for Orders {
    fn collection_name() -> &'static str {
        "orders"
    }
    fn fields() -> Vec<FieldDef> {
        vec![field("id", Type::Int64), field("status", Type::String)]
    }
    fn primary_field() -> &'static str {
        "id"
    }
    fn indexes() -> Vec<IndexDef> {
        vec![IndexDef {
            name: "status_idx".to_string(),
            path: FieldPath(vec![Cow::Borrowed("status")]),
            kind: IndexKind::NonUnique,
        }]
    }
}

// Subset model: only a projection of Orders.
struct OrdersSubset;

impl DbModel for OrdersSubset {
    fn collection_name() -> &'static str {
        "orders"
    }
    fn fields() -> Vec<FieldDef> {
        vec![field("id", Type::Int64)]
    }
    fn primary_field() -> &'static str {
        "id"
    }
}

#[test]
fn transaction_empty_commit_and_commit_without_begin_are_ok() {
    let mut db = Database::open_in_memory().unwrap();
    db.register_model::<Orders>().unwrap();

    // Commit without begin is a no-op.
    db.commit_transaction().unwrap();

    // Begin + commit with no staged segments is allowed.
    db.begin_transaction().unwrap();
    db.commit_transaction().unwrap();
}

#[test]
fn transaction_closure_error_rolls_back_written_rows() {
    let mut db = Database::open_in_memory().unwrap();
    db.register_model::<Orders>().unwrap();
    let cid = CollectionId(1);

    let res = db.transaction(|d| {
        let mut row = BTreeMap::new();
        row.insert("id".into(), RowValue::Int64(1));
        row.insert("status".into(), RowValue::String("open".into()));
        d.insert(cid, row)?;
        Err::<(), typra_core::DbError>(typra_core::DbError::NotImplemented)
    });
    assert!(res.is_err());

    // Nothing should have been committed.
    assert!(db.get(cid, &ScalarValue::Int64(1)).unwrap().is_none());
}

#[test]
fn register_schema_version_with_indexes_inside_transaction_roundtrips() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("schema_v.typra");

    {
        let mut db = Database::open(&path).unwrap();
        db.register_model::<Orders>().unwrap();
        let cid = CollectionId(1);

        db.transaction(|d| {
            let fields = vec![field("id", Type::Int64), field("status", Type::String)];
            let indexes = vec![IndexDef {
                name: "status2".to_string(),
                path: FieldPath(vec![Cow::Borrowed("status")]),
                kind: IndexKind::NonUnique,
            }];
            d.register_schema_version_with_indexes(cid, fields, indexes)?;
            Ok::<(), typra_core::DbError>(())
        })
        .unwrap();
    }

    let db2 = Database::open(&path).unwrap();
    let cid = CollectionId(1);
    let c = db2.catalog().get(cid).unwrap();
    assert_eq!(c.current_version.0, 2);
    assert!(c.indexes.iter().any(|i| i.name == "status2"));
}

#[test]
fn collection_and_query_builder_and_subset_projection_paths_work() {
    let mut db = Database::open_in_memory().unwrap();
    db.register_model::<Orders>().unwrap();
    let cid = CollectionId(1);

    for (id, status) in [(1, "open"), (2, "shipped"), (3, "open")] {
        let mut row = BTreeMap::new();
        row.insert("id".into(), RowValue::Int64(id));
        row.insert("status".into(), RowValue::String(status.into()));
        db.insert(cid, row).unwrap();
    }

    // Typed handle + query builder + explain.
    let c = db.collection::<Orders>().unwrap();
    let q = c
        .where_eq(
            FieldPath(vec![Cow::Borrowed("status")]),
            ScalarValue::String("open".into()),
        )
        .limit(10);
    let explain = c
        .where_eq(
            FieldPath(vec![Cow::Borrowed("status")]),
            ScalarValue::String("open".into()),
        )
        .limit(10)
        .explain()
        .unwrap();
    assert!(explain.contains("IndexLookup"));
    let rows = q.all().unwrap();
    assert_eq!(rows.len(), 2);

    // Subset projection only includes declared fields.
    let subset_rows = db.collection::<OrdersSubset>().unwrap().all().unwrap();
    assert_eq!(subset_rows.len(), 3);
    assert!(subset_rows
        .iter()
        .all(|r: &BTreeMap<String, RowValue>| r.contains_key("id") && !r.contains_key("status")));
}
