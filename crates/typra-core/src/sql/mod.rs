//! Minimal SQL adapter for DB-API (0.10.0+).
//!
//! This is intentionally small: a `SELECT` subset that maps onto the existing typed query AST.

mod lexer;
mod parser;

use crate::error::DbError;
use crate::query::OrderBy;
use crate::schema::FieldPath;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SqlSelect {
    pub columns: SqlColumns,
    pub collection: String,
    pub predicate: Option<SqlPredicate>,
    pub order_by: Option<OrderBy>,
    pub limit: Option<usize>,
    pub param_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlColumns {
    Star,
    Paths(Vec<FieldPath>),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlValue {
    Param(usize),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SqlPredicate {
    Eq { path: FieldPath, value: SqlValue },
    Lt { path: FieldPath, value: SqlValue },
    Lte { path: FieldPath, value: SqlValue },
    Gt { path: FieldPath, value: SqlValue },
    Gte { path: FieldPath, value: SqlValue },
    And(Vec<SqlPredicate>),
    Or(Vec<SqlPredicate>),
}

/// Parse a minimal `SELECT` statement into a structured form.
///
/// Notes:
/// - This accepts only parameter placeholders (`?`) for predicate values (no SQL literals yet).
/// - Keywords are ASCII case-insensitive.
pub fn parse_select(sql: &str) -> Result<SqlSelect, DbError> {
    parser::parse_select_tokens(lexer::lex(sql)?)
}

#[cfg(test)]
mod parse_select_token_tests {
    use super::parser::parse_select_tokens;
    use crate::error::DbError;
    use crate::sql::lexer::Tok;

    /// The lexer always emits digit runs as [`Tok::Number`], but `LIMIT` still accepts [`Tok::Ident`]
    /// so callers/tests can feed synthetic token streams and `usize`-parsable names stay valid.
    #[test]
    fn limit_accepts_ident_that_parses_as_usize() {
        let toks = vec![
            Tok::Ident("select".into()),
            Tok::Star,
            Tok::Ident("from".into()),
            Tok::Ident("t".into()),
            Tok::Ident("limit".into()),
            Tok::Ident("42".into()),
        ];
        let s = parse_select_tokens(toks).unwrap();
        assert_eq!(s.limit, Some(42));
    }

    #[test]
    fn parse_errors_when_where_clause_ends_after_path() {
        let toks = vec![
            Tok::Ident("select".into()),
            Tok::Star,
            Tok::Ident("from".into()),
            Tok::Ident("t".into()),
            Tok::Ident("where".into()),
            Tok::Ident("x".into()),
        ];
        let e = parse_select_tokens(toks).unwrap_err();
        assert!(matches!(e, DbError::Query(_)));
    }
}
