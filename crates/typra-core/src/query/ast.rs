use crate::schema::{CollectionId, FieldPath};
use crate::ScalarValue;

#[derive(Debug, Clone, PartialEq)]
pub struct Query {
    pub collection: CollectionId,
    pub predicate: Option<Predicate>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Predicate {
    Eq { path: FieldPath, value: ScalarValue },
    And(Vec<Predicate>),
}
