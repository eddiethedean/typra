//! Minimal typed query AST, planning, and execution (0.7.0).

mod ast;
mod operators;
mod planner;

pub use ast::{OrderBy, OrderDirection};
pub use ast::{Predicate, Query};
pub use planner::{
    execute_query, execute_query_iter, execute_query_iter_with_spill_path, explain_query,
    QueryRowIter,
};
