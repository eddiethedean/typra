use std::collections::hash_map::Iter as HashMapIter;
use std::collections::BTreeMap;

use crate::catalog::Catalog;
use crate::db::scalar_at_path;
use crate::error::{DbError, SchemaError};
use crate::index::IndexState;
use crate::record::RowValue;
use crate::schema::{CollectionId, IndexKind};
use crate::ScalarValue;

use super::ast::{OrderBy, OrderDirection};
use super::ast::{Predicate, Query};

#[derive(Debug, Clone, PartialEq)]
enum Plan {
    IndexLookup {
        collection_id: u32,
        index_name: String,
        kind: IndexKind,
        key: Vec<u8>,
        residual: Option<Predicate>,
        limit: Option<usize>,
        order_by: Option<OrderBy>,
    },
    CollectionScan {
        collection_id: u32,
        predicate: Option<Predicate>,
        limit: Option<usize>,
        order_by: Option<OrderBy>,
    },
}

pub fn explain_query(catalog: &Catalog, query: &Query) -> Result<String, DbError> {
    let col =
        catalog
            .get(query.collection)
            .ok_or(DbError::Schema(SchemaError::UnknownCollection {
                id: query.collection.0,
            }))?;
    let plan = plan_query(col.id, &col.indexes, query)?;
    Ok(match plan {
        Plan::IndexLookup {
            index_name,
            kind,
            residual,
            limit,
            order_by,
            ..
        } => {
            let mut s = String::new();
            s.push_str("Plan:\n");
            s.push_str(&format!(
                "  IndexLookup index={index_name:?} kind={kind:?}\n"
            ));
            if let Some(r) = residual {
                s.push_str(&format!("  ResidualFilter {r:?}\n"));
            }
            if let Some(n) = limit {
                s.push_str(&format!("  Limit {n}\n"));
            }
            if let Some(ob) = order_by {
                s.push_str(&format!("  OrderBy {:?} {:?}\n", ob.path, ob.direction));
            }
            s
        }
        Plan::CollectionScan {
            predicate,
            limit,
            order_by,
            ..
        } => {
            let mut s = String::new();
            s.push_str("Plan:\n");
            s.push_str("  CollectionScan\n");
            if let Some(p) = predicate {
                s.push_str(&format!("  Filter {p:?}\n"));
            }
            if let Some(n) = limit {
                s.push_str(&format!("  Limit {n}\n"));
            }
            if let Some(ob) = order_by {
                s.push_str(&format!("  OrderBy {:?} {:?}\n", ob.path, ob.direction));
            }
            s
        }
    })
}

pub fn execute_query(
    catalog: &Catalog,
    indexes: &IndexState,
    latest: &crate::db::LatestMap,
    query: &Query,
) -> Result<Vec<BTreeMap<String, RowValue>>, DbError> {
    let col =
        catalog
            .get(query.collection)
            .ok_or(DbError::Schema(SchemaError::UnknownCollection {
                id: query.collection.0,
            }))?;
    let plan = plan_query(col.id, &col.indexes, query)?;

    match plan {
        Plan::IndexLookup {
            collection_id,
            index_name,
            kind,
            key,
            residual,
            limit,
            order_by,
        } => {
            let mut out = Vec::new();
            let push_row = |out: &mut Vec<BTreeMap<String, RowValue>>, pk_key: Vec<u8>| {
                if let Some(row) = latest.get(&(collection_id, pk_key)).cloned() {
                    out.push(row);
                }
            };

            match kind {
                IndexKind::Unique => {
                    if let Some(pk) = indexes.unique_lookup(collection_id, &index_name, &key) {
                        push_row(&mut out, pk.to_vec());
                    }
                }
                IndexKind::NonUnique => {
                    if let Some(pks) = indexes.non_unique_lookup(collection_id, &index_name, &key) {
                        for pk in pks {
                            push_row(&mut out, pk);
                            if limit.map(|n| out.len() >= n).unwrap_or(false) {
                                break;
                            }
                        }
                    }
                }
            }

            if let Some(pred) = residual {
                out.retain(|row| eval_predicate(row, &pred));
            }
            apply_order_by_and_limit(&mut out, order_by.as_ref(), limit);
            Ok(out)
        }
        Plan::CollectionScan {
            collection_id,
            predicate,
            limit,
            order_by,
        } => {
            let mut out = Vec::new();
            for ((cid, _pk), row) in latest.iter() {
                if *cid != collection_id {
                    continue;
                }
                if let Some(ref p) = predicate {
                    if !eval_predicate(row, p) {
                        continue;
                    }
                }
                out.push(row.clone());
            }
            apply_order_by_and_limit(&mut out, order_by.as_ref(), limit);
            Ok(out)
        }
    }
}

/// Pull-based row iterator for simple queries (0.7 execution boundary).
///
/// This is **not** a full Volcano-style operator engine (no spill, joins, or async). It walks the
/// same plans as [`execute_query`] and yields owned rows one at a time.
pub struct QueryRowIter<'a> {
    state: QueryRowIterState<'a>,
}

enum QueryRowIterState<'a> {
    Vec {
        rows: Vec<BTreeMap<String, RowValue>>,
        pos: usize,
    },
    IndexUnique {
        latest: &'a crate::db::LatestMap,
        collection_id: u32,
        pk: Option<Vec<u8>>,
        residual: Option<Predicate>,
        emitted: bool,
    },
    IndexNonUnique {
        latest: &'a crate::db::LatestMap,
        collection_id: u32,
        pks: std::vec::IntoIter<Vec<u8>>,
        residual: Option<Predicate>,
        limit: Option<usize>,
        yielded: usize,
    },
    Scan {
        it: HashMapIter<'a, (u32, Vec<u8>), BTreeMap<String, RowValue>>,
        collection_id: u32,
        predicate: Option<Predicate>,
        limit: Option<usize>,
        yielded: usize,
    },
}

impl<'a> Iterator for QueryRowIter<'a> {
    type Item = Result<BTreeMap<String, RowValue>, DbError>;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.state {
            QueryRowIterState::Vec { rows, pos } => {
                if *pos >= rows.len() {
                    None
                } else {
                    let out = rows[*pos].clone();
                    *pos += 1;
                    Some(Ok(out))
                }
            }
            QueryRowIterState::IndexUnique {
                latest,
                collection_id,
                pk,
                residual,
                emitted,
            } => {
                if *emitted {
                    return None;
                }
                let Some(pk_key) = pk.take() else {
                    *emitted = true;
                    return None;
                };
                let Some(row) = latest.get(&(*collection_id, pk_key)).cloned() else {
                    *emitted = true;
                    return None;
                };
                if let Some(pred) = residual {
                    if !eval_predicate(&row, pred) {
                        *emitted = true;
                        return None;
                    }
                }
                *emitted = true;
                Some(Ok(row))
            }
            QueryRowIterState::IndexNonUnique {
                latest,
                collection_id,
                pks,
                residual,
                limit,
                yielded,
            } => {
                for pk_key in pks.by_ref() {
                    if let Some(n) = *limit {
                        if *yielded >= n {
                            return None;
                        }
                    }
                    let Some(row) = latest.get(&(*collection_id, pk_key)).cloned() else {
                        continue;
                    };
                    if let Some(pred) = residual {
                        if !eval_predicate(&row, pred) {
                            continue;
                        }
                    }
                    *yielded += 1;
                    return Some(Ok(row));
                }
                None
            }
            QueryRowIterState::Scan {
                it,
                collection_id,
                predicate,
                limit,
                yielded,
            } => {
                for (&(cid, _), row) in it.by_ref() {
                    if cid != *collection_id {
                        continue;
                    }
                    if let Some(ref p) = *predicate {
                        if !eval_predicate(row, p) {
                            continue;
                        }
                    }
                    if let Some(n) = *limit {
                        if *yielded >= n {
                            return None;
                        }
                    }
                    *yielded += 1;
                    return Some(Ok(row.clone()));
                }
                None
            }
        }
    }
}

/// Same planning and row sources as [`execute_query`], but as a lazy iterator.
pub fn execute_query_iter<'a>(
    catalog: &'a Catalog,
    indexes: &'a IndexState,
    latest: &'a crate::db::LatestMap,
    query: &Query,
) -> Result<QueryRowIter<'a>, DbError> {
    if query.order_by.is_some() {
        return Ok(QueryRowIter {
            state: QueryRowIterState::Vec {
                rows: execute_query(catalog, indexes, latest, query)?,
                pos: 0,
            },
        });
    }
    let col =
        catalog
            .get(query.collection)
            .ok_or(DbError::Schema(SchemaError::UnknownCollection {
                id: query.collection.0,
            }))?;
    let plan = plan_query(col.id, &col.indexes, query)?;
    let state = match plan {
        Plan::IndexLookup {
            collection_id,
            index_name,
            kind,
            key,
            residual,
            limit,
            ..
        } => match kind {
            IndexKind::Unique => {
                let pk = indexes
                    .unique_lookup(collection_id, &index_name, &key)
                    .map(|p| p.to_vec());
                QueryRowIterState::IndexUnique {
                    latest,
                    collection_id,
                    pk,
                    residual,
                    emitted: false,
                }
            }
            IndexKind::NonUnique => {
                let pks = indexes
                    .non_unique_lookup(collection_id, &index_name, &key)
                    .unwrap_or_default()
                    .into_iter();
                QueryRowIterState::IndexNonUnique {
                    latest,
                    collection_id,
                    pks,
                    residual,
                    limit,
                    yielded: 0,
                }
            }
        },
        Plan::CollectionScan {
            collection_id,
            predicate,
            limit,
            ..
        } => QueryRowIterState::Scan {
            it: latest.iter(),
            collection_id,
            predicate,
            limit,
            yielded: 0,
        },
    };
    Ok(QueryRowIter { state })
}

fn plan_query(
    collection: CollectionId,
    indexes: &[crate::schema::IndexDef],
    query: &Query,
) -> Result<Plan, DbError> {
    let Some(pred) = query.predicate.clone() else {
        return Ok(Plan::CollectionScan {
            collection_id: collection.0,
            predicate: None,
            limit: query.limit,
            order_by: query.order_by.clone(),
        });
    };

    let (best, residual) = match choose_index(indexes, &pred) {
        None => (None, Some(pred)),
        Some((idx, value, used_pred)) => {
            let residual = remove_used_predicate(pred, used_pred);
            (Some((idx, value)), residual)
        }
    };

    if let Some((idx, value)) = best {
        Ok(Plan::IndexLookup {
            collection_id: collection.0,
            index_name: idx.name.clone(),
            kind: idx.kind,
            key: value.canonical_key_bytes(),
            residual,
            limit: query.limit,
            order_by: query.order_by.clone(),
        })
    } else {
        Ok(Plan::CollectionScan {
            collection_id: collection.0,
            predicate: residual,
            limit: query.limit,
            order_by: query.order_by.clone(),
        })
    }
}

fn choose_index<'a>(
    indexes: &'a [crate::schema::IndexDef],
    pred: &Predicate,
) -> Option<(&'a crate::schema::IndexDef, ScalarValue, Predicate)> {
    match pred {
        Predicate::Eq { path, value } => indexes
            .iter()
            .find(|idx| &idx.path == path)
            .map(|idx| (idx, value.clone(), pred.clone())),
        Predicate::Lt { .. }
        | Predicate::Lte { .. }
        | Predicate::Gt { .. }
        | Predicate::Gte { .. }
        | Predicate::Or(_) => None,
        Predicate::And(items) => {
            // Prefer unique index predicates, else first indexed predicate.
            let mut best: Option<(&crate::schema::IndexDef, ScalarValue, Predicate)> = None;
            for p in items {
                if let Some((idx, v, used)) = choose_index(indexes, p) {
                    match best {
                        None => best = Some((idx, v, used)),
                        Some((best_idx, _, _)) => {
                            if best_idx.kind != IndexKind::Unique && idx.kind == IndexKind::Unique {
                                best = Some((idx, v, used));
                            }
                        }
                    }
                }
            }
            best
        }
    }
}

fn remove_used_predicate(pred: Predicate, used: Predicate) -> Option<Predicate> {
    if pred == used {
        return None;
    }
    match pred {
        Predicate::And(items) => {
            let mut out: Vec<Predicate> = items.into_iter().filter(|p| p != &used).collect();
            match out.len() {
                0 => None,
                1 => Some(out.remove(0)),
                _ => Some(Predicate::And(out)),
            }
        }
        _ => Some(pred),
    }
}

fn eval_predicate(row: &BTreeMap<String, RowValue>, pred: &Predicate) -> bool {
    match pred {
        Predicate::Eq { path, value } => scalar_at_path(row, path)
            .map(|s| &s == value)
            .unwrap_or(false),
        Predicate::Lt { path, value } => scalar_at_path(row, path)
            .and_then(|s| scalar_partial_cmp(&s, value))
            .map(|o| o.is_lt())
            .unwrap_or(false),
        Predicate::Lte { path, value } => scalar_at_path(row, path)
            .and_then(|s| scalar_partial_cmp(&s, value))
            .map(|o| o.is_lt() || o.is_eq())
            .unwrap_or(false),
        Predicate::Gt { path, value } => scalar_at_path(row, path)
            .and_then(|s| scalar_partial_cmp(&s, value))
            .map(|o| o.is_gt())
            .unwrap_or(false),
        Predicate::Gte { path, value } => scalar_at_path(row, path)
            .and_then(|s| scalar_partial_cmp(&s, value))
            .map(|o| o.is_gt() || o.is_eq())
            .unwrap_or(false),
        Predicate::And(items) => items.iter().all(|p| eval_predicate(row, p)),
        Predicate::Or(items) => items.iter().any(|p| eval_predicate(row, p)),
    }
}

fn apply_order_by_and_limit(
    rows: &mut Vec<BTreeMap<String, RowValue>>,
    order_by: Option<&OrderBy>,
    limit: Option<usize>,
) {
    if let Some(ob) = order_by {
        rows.sort_by(|a, b| {
            let av = scalar_at_path(a, &ob.path);
            let bv = scalar_at_path(b, &ob.path);
            let ord = match (av, bv) {
                (None, None) => std::cmp::Ordering::Equal,
                (None, Some(_)) => std::cmp::Ordering::Greater,
                (Some(_), None) => std::cmp::Ordering::Less,
                (Some(x), Some(y)) => {
                    scalar_partial_cmp(&x, &y).unwrap_or(std::cmp::Ordering::Equal)
                }
            };
            match ob.direction {
                OrderDirection::Asc => ord,
                OrderDirection::Desc => ord.reverse(),
            }
        });
    }
    if let Some(n) = limit {
        rows.truncate(n);
    }
}

fn scalar_partial_cmp(a: &ScalarValue, b: &ScalarValue) -> Option<std::cmp::Ordering> {
    use ScalarValue::*;
    match (a, b) {
        (Bool(x), Bool(y)) => Some(x.cmp(y)),
        (Int64(x), Int64(y)) => Some(x.cmp(y)),
        (Uint64(x), Uint64(y)) => Some(x.cmp(y)),
        (Float64(x), Float64(y)) => x.partial_cmp(y),
        (String(x), String(y)) => Some(x.cmp(y)),
        (Bytes(x), Bytes(y)) => Some(x.cmp(y)),
        (Uuid(x), Uuid(y)) => Some(x.cmp(y)),
        (Timestamp(x), Timestamp(y)) => Some(x.cmp(y)),
        _ => None,
    }
}
