use crate::schema::{CollectionId, FieldPath};
use crate::ScalarValue;

#[derive(Debug, Clone, PartialEq)]
pub struct Query {
    pub collection: CollectionId,
    pub predicate: Option<Predicate>,
    pub limit: Option<usize>,
    pub order_by: Option<OrderBy>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderBy {
    pub path: FieldPath,
    pub direction: OrderDirection,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OrderDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Predicate {
    Eq { path: FieldPath, value: ScalarValue },
    Lt { path: FieldPath, value: ScalarValue },
    Lte { path: FieldPath, value: ScalarValue },
    Gt { path: FieldPath, value: ScalarValue },
    Gte { path: FieldPath, value: ScalarValue },
    And(Vec<Predicate>),
    Or(Vec<Predicate>),
}
