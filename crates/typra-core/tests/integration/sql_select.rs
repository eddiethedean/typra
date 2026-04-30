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

#[test]
fn sql_lex_and_parse_errors_and_branches() {
    assert!(parse_select("SELECT * FORM books").is_err());
    assert!(parse_select("SELECT * FROM").is_err());
    assert!(parse_select("SELECT * FROM books extra").is_err());
    assert!(parse_select("SELECT * FROM books WHERE (title = ?").is_err());
    assert!(parse_select("SELECT * FROM books WHERE title + ?").is_err());

    let bad_chars = parse_select("SELECT * FROM books WHERE title @ ?");
    assert!(bad_chars.is_err());

    let s = parse_select("SELECT a.b FROM t WHERE x < ? AND y > ? OR z <= ?").unwrap();
    assert_eq!(s.param_count, 3);

    let ord = parse_select("SELECT * FROM books ORDER BY title ASC LIMIT 3").unwrap();
    assert_eq!(ord.limit, Some(3));
    assert!(ord.order_by.as_ref().unwrap().path.0[0].as_ref() == "title");

    let ord_def = parse_select("SELECT * FROM books ORDER BY title").unwrap();
    assert!(ord_def.order_by.is_some());

    let lim_bad = parse_select("SELECT * FROM books LIMIT three");
    assert!(lim_bad.is_err());

    let cols = parse_select("SELECT x, y.z FROM items").unwrap();
    assert!(matches!(cols.columns, SqlColumns::Paths(_)));

    // Lex `)` for parenthesized predicates.
    let paren = parse_select("SELECT * FROM books WHERE (title = ? AND sku = ?)").unwrap();
    assert_eq!(paren.param_count, 2);

    assert!(parse_select("SELECT * FROM books WHERE title * ?").is_err());

    assert!(parse_select("SELECT * FROM books LIMIT").is_err());

    assert!(parse_select("SELECT * FROM books LIMIT 1 trailing").is_err());

    // Hit: expected '*' or column list after SELECT
    assert!(parse_select("SELECT , FROM t").is_err());
    // Hit: expected parameter '?'
    assert!(parse_select("SELECT * FROM t WHERE x = y").is_err());
    // Hit: expected predicate
    assert!(parse_select("SELECT * FROM t WHERE )").is_err());
}
