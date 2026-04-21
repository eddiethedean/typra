use typra_core::schema::FieldPath;
use typra_core::DbError;

#[test]
fn field_path_rejects_empty() {
    let res = FieldPath::new([]);
    assert!(matches!(res, Err(DbError::Schema(_))));
}

#[test]
fn field_path_accepts_simple_path() {
    let path = FieldPath::new([
        std::borrow::Cow::Borrowed("profile"),
        std::borrow::Cow::Borrowed("timezone"),
    ])
    .expect("field path");
    assert_eq!(path.0.len(), 2);
}

#[test]
fn field_path_rejects_empty_segment() {
    let res = FieldPath::new([
        std::borrow::Cow::Borrowed("profile"),
        std::borrow::Cow::Borrowed(""),
    ]);
    assert!(matches!(res, Err(DbError::Schema(_))));
}
