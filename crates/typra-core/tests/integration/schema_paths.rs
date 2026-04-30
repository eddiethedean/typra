use typra_core::schema::{DbModel, FieldDef, FieldPath, Type};
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

#[test]
fn db_model_default_indexes_is_empty_vec() {
    struct OnlyDefaults;
    impl DbModel for OnlyDefaults {
        fn collection_name() -> &'static str {
            "c"
        }
        fn fields() -> Vec<FieldDef> {
            vec![FieldDef::new(
                FieldPath::new([std::borrow::Cow::Borrowed("id")]).unwrap(),
                Type::String,
            )]
        }
        fn primary_field() -> &'static str {
            "id"
        }
    }
    assert!(OnlyDefaults::indexes().is_empty());
}
