#![cfg(feature = "async")]

use std::borrow::Cow;
use std::collections::BTreeMap;

use typra::prelude::*;
use typra::schema::FieldPath;
use typra::OpenOptions;
use typra::Type;

#[tokio::test]
async fn async_in_memory_register_insert_get_delete_roundtrip() {
    let db = typra::AsyncDatabase::open_in_memory().await.unwrap();

    let fields = vec![
        FieldDef {
            path: FieldPath::new([Cow::Borrowed("title")]).unwrap(),
            ty: Type::String,
            constraints: vec![],
        },
        FieldDef {
            path: FieldPath::new([Cow::Borrowed("year")]).unwrap(),
            ty: Type::Int64,
            constraints: vec![],
        },
    ];
    let (cid, _v) = db
        .register_collection("books".to_string(), fields, "title".to_string())
        .await
        .unwrap();

    db.insert(cid, {
        let mut m = BTreeMap::new();
        m.insert("title".to_string(), RowValue::String("Hello".to_string()));
        m.insert("year".to_string(), RowValue::Int64(2020));
        m
    })
    .await
    .unwrap();

    let got = db
        .get(cid, ScalarValue::String("Hello".to_string()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(got.get("year"), Some(&RowValue::Int64(2020)));

    db.delete(cid, ScalarValue::String("Hello".to_string()))
        .await
        .unwrap();
    let got2 = db
        .get(cid, ScalarValue::String("Hello".to_string()))
        .await
        .unwrap();
    assert!(got2.is_none());
}

#[tokio::test]
async fn async_transaction_commits_on_ok_and_rolls_back_on_err() {
    let db = typra::AsyncDatabase::open_in_memory().await.unwrap();

    let fields = vec![FieldDef {
        path: FieldPath::new([Cow::Borrowed("title")]).unwrap(),
        ty: Type::String,
        constraints: vec![],
    }];
    let (cid, _v) = db
        .register_collection("books".to_string(), fields, "title".to_string())
        .await
        .unwrap();

    // Commit case.
    db.transaction(move |tx| {
        tx.insert(
            cid,
            BTreeMap::from([("title".to_string(), RowValue::String("A".to_string()))]),
        )?;
        Ok(())
    })
    .await
    .unwrap();

    // Rollback case.
    let res: Result<(), DbError> = db
        .transaction(move |tx| {
            tx.insert(
                cid,
                BTreeMap::from([("title".to_string(), RowValue::String("B".to_string()))]),
            )?;
            Err(DbError::NotImplemented)
        })
        .await;
    assert!(res.is_err());

    // Only A should exist.
    let a = db
        .get(cid, ScalarValue::String("A".to_string()))
        .await
        .unwrap();
    assert!(a.is_some());
    let b = db
        .get(cid, ScalarValue::String("B".to_string()))
        .await
        .unwrap();
    assert!(b.is_none());
}

#[tokio::test]
async fn async_open_on_disk_and_collection_names_work() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("t.typra");

    let db = typra::AsyncDatabase::open(path.clone()).await.unwrap();
    assert_eq!(db.collection_names().await.unwrap(), Vec::<String>::new());

    // Re-open with explicit options to ensure the API stays available.
    let _db2 = typra::AsyncDatabase::open_with_options(path, OpenOptions::default())
        .await
        .unwrap();
}
