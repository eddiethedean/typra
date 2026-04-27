//! Minimal SQL adapter for DB-API (0.10.0+).
//!
//! This is intentionally small: a `SELECT` subset that maps onto the existing typed query AST.

use std::borrow::Cow;

use crate::error::{DbError, QueryError};
use crate::query::{OrderBy, OrderDirection};
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

#[derive(Debug, Clone, PartialEq, Eq)]
enum Tok {
    Ident(String),
    Number(usize),
    Star,
    Comma,
    Dot,
    LParen,
    RParen,
    QMark,
    Eq,
    Lt,
    Lte,
    Gt,
    Gte,
}

fn err(msg: impl Into<String>) -> DbError {
    DbError::Query(QueryError {
        message: msg.into(),
    })
}

fn is_ident_start(c: char) -> bool {
    c.is_ascii_alphabetic() || c == '_'
}
fn is_ident_cont(c: char) -> bool {
    c.is_ascii_alphanumeric() || c == '_'
}

fn lex(input: &str) -> Result<Vec<Tok>, DbError> {
    let mut out = Vec::new();
    let mut chars = input.chars().peekable();
    while let Some(c) = chars.peek().copied() {
        if c.is_whitespace() {
            chars.next();
            continue;
        }
        match c {
            _ if c.is_ascii_digit() => {
                let mut s = String::new();
                while let Some(n) = chars.peek().copied() {
                    if n.is_ascii_digit() {
                        s.push(chars.next().unwrap());
                    } else {
                        break;
                    }
                }
                let n = s
                    .parse::<usize>()
                    .map_err(|_| err("invalid integer literal"))?;
                out.push(Tok::Number(n));
            }
            '*' => {
                chars.next();
                out.push(Tok::Star);
            }
            ',' => {
                chars.next();
                out.push(Tok::Comma);
            }
            '.' => {
                chars.next();
                out.push(Tok::Dot);
            }
            '(' => {
                chars.next();
                out.push(Tok::LParen);
            }
            ')' => {
                chars.next();
                out.push(Tok::RParen);
            }
            '?' => {
                chars.next();
                out.push(Tok::QMark);
            }
            '=' => {
                chars.next();
                out.push(Tok::Eq);
            }
            '<' => {
                chars.next();
                if chars.peek().copied() == Some('=') {
                    chars.next();
                    out.push(Tok::Lte);
                } else {
                    out.push(Tok::Lt);
                }
            }
            '>' => {
                chars.next();
                if chars.peek().copied() == Some('=') {
                    chars.next();
                    out.push(Tok::Gte);
                } else {
                    out.push(Tok::Gt);
                }
            }
            _ if is_ident_start(c) => {
                let mut s = String::new();
                s.push(chars.next().unwrap());
                while let Some(n) = chars.peek().copied() {
                    if is_ident_cont(n) {
                        s.push(chars.next().unwrap());
                    } else {
                        break;
                    }
                }
                out.push(Tok::Ident(s));
            }
            _ => return Err(err(format!("unsupported character in SQL: {c:?}"))),
        }
    }
    Ok(out)
}

fn ident_eq(a: &str, b: &str) -> bool {
    a.eq_ignore_ascii_case(b)
}

struct P {
    toks: Vec<Tok>,
    pos: usize,
    param_next: usize,
}

impl P {
    fn new(toks: Vec<Tok>) -> Self {
        Self {
            toks,
            pos: 0,
            param_next: 0,
        }
    }

    fn peek(&self) -> Option<&Tok> {
        self.toks.get(self.pos)
    }

    fn bump(&mut self) -> Option<Tok> {
        let t = self.toks.get(self.pos).cloned();
        if t.is_some() {
            self.pos += 1;
        }
        t
    }

    fn expect_ident_kw(&mut self, kw: &str) -> Result<(), DbError> {
        match self.bump() {
            Some(Tok::Ident(s)) if ident_eq(&s, kw) => Ok(()),
            _ => Err(err(format!("expected keyword {kw:?}"))),
        }
    }

    fn take_ident(&mut self) -> Result<String, DbError> {
        match self.bump() {
            Some(Tok::Ident(s)) => Ok(s),
            _ => Err(err("expected identifier")),
        }
    }

    fn take_path(&mut self) -> Result<FieldPath, DbError> {
        let first = self.take_ident()?;
        let mut parts = vec![Cow::Owned(first)];
        while let Some(Tok::Dot) = self.peek() {
            self.bump();
            let seg = self.take_ident()?;
            parts.push(Cow::Owned(seg));
        }
        Ok(FieldPath(parts))
    }

    fn take_param(&mut self) -> Result<SqlValue, DbError> {
        match self.bump() {
            Some(Tok::QMark) => {
                let idx = self.param_next;
                self.param_next += 1;
                Ok(SqlValue::Param(idx))
            }
            _ => Err(err("expected parameter '?'")),
        }
    }

    fn take_cmp(&mut self, path: FieldPath) -> Result<SqlPredicate, DbError> {
        let op = self
            .bump()
            .ok_or_else(|| err("expected comparison operator"))?;
        let value = self.take_param()?;
        Ok(match op {
            Tok::Eq => SqlPredicate::Eq { path, value },
            Tok::Lt => SqlPredicate::Lt { path, value },
            Tok::Lte => SqlPredicate::Lte { path, value },
            Tok::Gt => SqlPredicate::Gt { path, value },
            Tok::Gte => SqlPredicate::Gte { path, value },
            _ => return Err(err("expected one of '=', '<', '<=', '>', '>='")),
        })
    }

    fn parse_primary(&mut self) -> Result<SqlPredicate, DbError> {
        match self.peek() {
            Some(Tok::LParen) => {
                self.bump();
                let inner = self.parse_or()?;
                match self.bump() {
                    Some(Tok::RParen) => Ok(inner),
                    _ => Err(err("expected ')'")),
                }
            }
            Some(Tok::Ident(_)) => {
                let path = self.take_path()?;
                self.take_cmp(path)
            }
            _ => Err(err("expected predicate")),
        }
    }

    fn parse_and(&mut self) -> Result<SqlPredicate, DbError> {
        let mut items = vec![self.parse_primary()?];
        while let Some(Tok::Ident(s)) = self.peek() {
            if !ident_eq(s, "and") {
                break;
            }
            self.bump();
            items.push(self.parse_primary()?);
        }
        Ok(if items.len() == 1 {
            items.remove(0)
        } else {
            SqlPredicate::And(items)
        })
    }

    fn parse_or(&mut self) -> Result<SqlPredicate, DbError> {
        let mut items = vec![self.parse_and()?];
        while let Some(Tok::Ident(s)) = self.peek() {
            if !ident_eq(s, "or") {
                break;
            }
            self.bump();
            items.push(self.parse_and()?);
        }
        Ok(if items.len() == 1 {
            items.remove(0)
        } else {
            SqlPredicate::Or(items)
        })
    }
}

/// Parse a minimal `SELECT` statement into a structured form.
///
/// Notes:
/// - This accepts only parameter placeholders (`?`) for predicate values (no SQL literals yet).
/// - Keywords are ASCII case-insensitive.
pub fn parse_select(sql: &str) -> Result<SqlSelect, DbError> {
    let toks = lex(sql)?;
    let mut p = P::new(toks);

    p.expect_ident_kw("select")?;
    let columns = match p.peek() {
        Some(Tok::Star) => {
            p.bump();
            SqlColumns::Star
        }
        Some(Tok::Ident(_)) => {
            let mut paths = Vec::new();
            paths.push(p.take_path()?);
            while let Some(Tok::Comma) = p.peek() {
                p.bump();
                paths.push(p.take_path()?);
            }
            SqlColumns::Paths(paths)
        }
        _ => return Err(err("expected '*' or column list after SELECT")),
    };

    p.expect_ident_kw("from")?;
    let collection = p.take_ident()?;

    let mut predicate = None;
    let mut order_by = None;
    let mut limit = None;

    if let Some(Tok::Ident(s)) = p.peek() {
        if ident_eq(s, "where") {
            p.bump();
            predicate = Some(p.parse_or()?);
        }
    }

    if let Some(Tok::Ident(s)) = p.peek() {
        if ident_eq(s, "order") {
            p.bump();
            p.expect_ident_kw("by")?;
            let path = p.take_path()?;
            let dir = match p.peek() {
                Some(Tok::Ident(s)) if ident_eq(s, "asc") => {
                    p.bump();
                    OrderDirection::Asc
                }
                Some(Tok::Ident(s)) if ident_eq(s, "desc") => {
                    p.bump();
                    OrderDirection::Desc
                }
                _ => OrderDirection::Asc,
            };
            order_by = Some(OrderBy {
                path,
                direction: dir,
            });
        }
    }

    if let Some(Tok::Ident(s)) = p.peek() {
        if ident_eq(s, "limit") {
            p.bump();
            let n = match p.bump() {
                Some(Tok::Number(n)) => n,
                _ => return Err(err("expected integer after LIMIT")),
            };
            limit = Some(n);
        }
    }

    if p.peek().is_some() {
        return Err(err("trailing tokens after SQL statement"));
    }

    Ok(SqlSelect {
        columns,
        collection,
        predicate,
        order_by,
        limit,
        param_count: p.param_next,
    })
}
