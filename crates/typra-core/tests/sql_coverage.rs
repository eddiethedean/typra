use typra_core::sql::{parse_select, SqlColumns, SqlPredicate, SqlValue};

#[test]
fn parse_select_supports_lte_predicate() {
    let q = parse_select("select * from t where a <= ?").unwrap();
    assert!(matches!(q.columns, SqlColumns::Star));
    assert_eq!(q.collection, "t");
    assert_eq!(q.param_count, 1);

    let pred = q.predicate.unwrap();
    match pred {
        SqlPredicate::Lte { path, value } => {
            assert_eq!(path.0.len(), 1);
            assert_eq!(path.0[0], "a");
            assert_eq!(value, SqlValue::Param(0));
        }
        other => panic!("expected Lte predicate, got {other:?}"),
    }
}

#[test]
fn parse_select_rejects_integer_overflow_in_number_token() {
    // Force `usize` parse to fail in the lexer.
    let huge = "999999999999999999999999999999999999999999999999999999";
    let sql = format!("select * from t limit {huge}");
    assert!(parse_select(&sql).is_err());
}

#[test]
fn parse_select_supports_gt_without_equals() {
    let q = parse_select("select * from t where a > ?").unwrap();
    let pred = q.predicate.unwrap();
    assert!(matches!(pred, SqlPredicate::Gt { .. }));
}

#[test]
fn parse_select_parses_dotted_paths_and_comma_separated_columns() {
    let q = parse_select("select a.b, c from t").unwrap();
    match q.columns {
        SqlColumns::Paths(paths) => {
            assert_eq!(paths.len(), 2);
            assert_eq!(paths[0].0.len(), 2);
            assert_eq!(paths[0].0[0], "a");
            assert_eq!(paths[0].0[1], "b");
            assert_eq!(paths[1].0.len(), 1);
            assert_eq!(paths[1].0[0], "c");
        }
        other => panic!("expected Paths columns, got {other:?}"),
    }
}

#[test]
fn parse_select_order_by_accepts_explicit_asc_and_defaults_to_asc() {
    let q = parse_select("select * from t order by a asc").unwrap();
    assert!(q.order_by.is_some());

    let q = parse_select("select * from t order by a").unwrap();
    assert!(q.order_by.is_some());

    // Exercise the "direction token present but not asc/desc" branch by leaving the next keyword
    // (`LIMIT`) to be parsed by the next clause.
    let q = parse_select("select * from t order by a limit 1").unwrap();
    assert!(q.order_by.is_some());
    assert_eq!(q.limit, Some(1));
}

#[test]
fn parse_select_limit_accepts_identifier_number_and_rejects_trailing_tokens() {
    let q = parse_select("select * from t limit 10").unwrap();
    assert_eq!(q.limit, Some(10));

    assert!(parse_select("select * from t limit x").is_err());

    assert!(parse_select("select * from t foo").is_err());
}

#[test]
fn parse_select_rejects_misspelled_from_keyword_as_identifier() {
    // This specifically forces `expect_ident_kw("from")` to see an identifier token that doesn't
    // match the expected keyword (so the match-guard false branch is exercised).
    assert!(parse_select("select * form t").is_err());
}

