use typra_core::sql::{parse_select, SqlColumns, SqlPredicate};

#[test]
fn sql_parse_select_star_simple() {
    let s = parse_select("SELECT * FROM books").unwrap();
    assert!(matches!(s.columns, SqlColumns::Star));
    assert_eq!(s.collection, "books");
    assert_eq!(s.param_count, 0);
    assert!(s.predicate.is_none());
    assert!(s.order_by.is_none());
    assert!(s.limit.is_none());
}

#[test]
fn sql_parse_where_and_or_and_order_limit() {
    let s = parse_select(
        "select title from books where year >= ? and rating < ? or title = ? order by year desc limit 10",
    )
    .unwrap();
    assert_eq!(s.collection, "books");
    assert_eq!(s.param_count, 3);
    assert!(s.order_by.is_some());
    assert_eq!(s.limit, Some(10));
    assert!(matches!(s.columns, SqlColumns::Paths(_)));
    assert!(matches!(s.predicate, Some(SqlPredicate::Or(_))));
}
