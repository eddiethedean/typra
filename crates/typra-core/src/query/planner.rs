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
use super::operators::{LimitOp, RowKey, RowSource};

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
            match predicate {
                Some(p) => s.push_str(&format!("  Filter {p:?}\n")),
                None => {}
            }
            match limit {
                Some(n) => s.push_str(&format!("  Limit {n}\n")),
                None => {}
            }
            match order_by {
                Some(ob) => s.push_str(&format!("  OrderBy {:?} {:?}\n", ob.path, ob.direction)),
                None => {}
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
                match latest.get(&(collection_id, pk_key)).cloned() {
                    Some(row) => out.push(row),
                    None => {}
                }
            };

            match kind {
                IndexKind::Unique => {
                    match indexes.unique_lookup(collection_id, &index_name, &key) {
                        Some(pk) => push_row(&mut out, pk.to_vec()),
                        None => {}
                    }
                }
                IndexKind::NonUnique => {
                    match indexes.non_unique_lookup(collection_id, &index_name, &key) {
                        Some(pks) => {
                            for pk in pks {
                                push_row(&mut out, pk);
                                if limit.map(|n| out.len() >= n).unwrap_or(false) {
                                    break;
                                }
                            }
                        }
                        None => {}
                    }
                }
            }

            match residual {
                Some(pred) => out.retain(|row| eval_predicate(row, &pred)),
                None => {}
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
                match predicate.as_ref() {
                    Some(p) => {
                        if !eval_predicate(row, p) {
                            continue;
                        }
                    }
                    None => {}
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
/// This is **not** a full Volcano-style operator engine yet (no joins / async), but it does
/// establish an internal streaming operator boundary by yielding `(collection_id, pk_key)` and
/// materializing rows from `latest` at the edge.
pub struct QueryRowIter<'a> {
    state: QueryRowIterState<'a>,
}

enum QueryRowIterState<'a> {
    Vec {
        rows: Vec<BTreeMap<String, RowValue>>,
        pos: usize,
    },
    Source {
        latest: &'a crate::db::LatestMap,
        source: Box<dyn RowSource + 'a>,
    },
}

impl<'a> Iterator for QueryRowIter<'a> {
    type Item = Result<BTreeMap<String, RowValue>, DbError>;

    fn next(&mut self) -> Option<Self::Item> {
        match &mut self.state {
            QueryRowIterState::Vec { rows, pos } => {
                match (*pos).cmp(&rows.len()) {
                    std::cmp::Ordering::Less => {
                        let out = rows[*pos].clone();
                        *pos += 1;
                        Some(Ok(out))
                    }
                    std::cmp::Ordering::Equal | std::cmp::Ordering::Greater => None,
                }
            }
            QueryRowIterState::Source { latest, source } => loop {
                let rk = source.next_key()?;
                match rk {
                    Err(e) => return Some(Err(e)),
                    Ok((cid, pk_key)) => {
                        match latest.get(&(cid.0, pk_key)).cloned() {
                            Some(row) => return Some(Ok(row)),
                            None => {}
                        }
                    }
                }
            },
        }
    }
}

struct IndexUniqueSource<'a> {
    latest: &'a crate::db::LatestMap,
    collection_id: u32,
    pk: Option<Vec<u8>>,
    residual: Option<Predicate>,
    done: bool,
}

impl RowSource for IndexUniqueSource<'_> {
    fn next_key(&mut self) -> Option<Result<RowKey, DbError>> {
        match self.done {
            true => return None,
            false => {}
        }
        self.done = true;
        let pk_key = self.pk.take()?;
        let row = self.latest.get(&(self.collection_id, pk_key.clone()))?;
        match &self.residual {
            None => {}
            Some(pred) => match eval_predicate(row, pred) {
                true => {}
                false => return None,
            },
        };
        Some(Ok((CollectionId(self.collection_id), pk_key)))
    }
}

struct IndexNonUniqueSource<'a> {
    latest: &'a crate::db::LatestMap,
    collection_id: u32,
    pks: std::vec::IntoIter<Vec<u8>>,
    residual: Option<Predicate>,
}

impl RowSource for IndexNonUniqueSource<'_> {
    fn next_key(&mut self) -> Option<Result<RowKey, DbError>> {
        for pk_key in self.pks.by_ref() {
            let row = match self.latest.get(&(self.collection_id, pk_key.clone())) {
                Some(r) => r,
                None => continue,
            };
            match &self.residual {
                None => {}
                Some(pred) => match eval_predicate(row, pred) {
                    true => {}
                    false => continue,
                },
            }
            return Some(Ok((CollectionId(self.collection_id), pk_key)));
        }
        None
    }
}

struct ScanSource<'a> {
    it: HashMapIter<'a, (u32, Vec<u8>), BTreeMap<String, RowValue>>,
    collection_id: u32,
    predicate: Option<Predicate>,
}

impl RowSource for ScanSource<'_> {
    fn next_key(&mut self) -> Option<Result<RowKey, DbError>> {
        for (&(cid, ref pk_key), row) in self.it.by_ref() {
            if cid != self.collection_id {
                continue;
            }
            match &self.predicate {
                None => {}
                Some(p) => match eval_predicate(row, p) {
                    true => {}
                    false => continue,
                },
            };
            return Some(Ok((CollectionId(self.collection_id), pk_key.clone())));
        }
        None
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
    let mut source: Box<dyn RowSource + 'a> = match plan {
        Plan::IndexLookup {
            collection_id,
            index_name,
            kind,
            key,
            residual,
            ..
        } => match kind {
            IndexKind::Unique => {
                let pk = indexes
                    .unique_lookup(collection_id, &index_name, &key)
                    .map(|p| p.to_vec());
                Box::new(IndexUniqueSource {
                    latest,
                    collection_id,
                    pk,
                    residual,
                    done: false,
                })
            }
            IndexKind::NonUnique => {
                let pks = indexes
                    .non_unique_lookup(collection_id, &index_name, &key)
                    .unwrap_or_default()
                    .into_iter();
                Box::new(IndexNonUniqueSource {
                    latest,
                    collection_id,
                    pks,
                    residual,
                })
            }
        },
        Plan::CollectionScan {
            collection_id,
            predicate,
            ..
        } => Box::new(ScanSource {
            it: latest.iter(),
            collection_id,
            predicate,
        }),
    };

    if let Some(n) = query.limit {
        source = Box::new(LimitOp::new(source, n));
    }

    Ok(QueryRowIter {
        state: QueryRowIterState::Source { latest, source },
    })
}

/// Like [`execute_query_iter`], but when `q.order_by` is set this will attempt a bounded-memory
/// external sort by spilling ephemeral `Temp` segments to the underlying DB file.
///
/// If `db_path` is `None` (e.g. in-memory), this falls back to the in-memory sort path.
pub fn execute_query_iter_with_spill_path<'a>(
    catalog: &'a Catalog,
    indexes: &'a IndexState,
    latest: &'a crate::db::LatestMap,
    q: &Query,
    db_path: Option<&std::path::Path>,
) -> Result<QueryRowIter<'a>, DbError> {
    let order_by = match q.order_by.clone() {
        Some(ob) => ob,
        None => return execute_query_iter(catalog, indexes, latest, q),
    };

    // If we don't have a file path to spill into, fall back to the existing in-memory behavior.
    let path = match db_path {
        Some(p) => p,
        None => {
            return Ok(QueryRowIter {
                state: QueryRowIterState::Vec {
                    rows: execute_query(catalog, indexes, latest, q)?,
                    pos: 0,
                },
            });
        }
    };

    let col = catalog
        .get(q.collection)
        .ok_or(DbError::Schema(SchemaError::UnknownCollection {
            id: q.collection.0,
        }))?;
    let plan = plan_query(col.id, &col.indexes, q)?;

    let base: Box<dyn RowSource + 'a> = match plan.clone() {
        Plan::IndexLookup {
            collection_id,
            index_name,
            kind,
            key,
            residual,
            ..
        } => match kind {
            IndexKind::Unique => Box::new(IndexUniqueSource {
                latest,
                collection_id,
                pk: indexes
                    .unique_lookup(collection_id, &index_name, &key)
                    .map(|p| p.to_vec()),
                residual,
                done: false,
            }),
            IndexKind::NonUnique => Box::new(IndexNonUniqueSource {
                latest,
                collection_id,
                pks: indexes
                    .non_unique_lookup(collection_id, &index_name, &key)
                    .unwrap_or_default()
                    .into_iter(),
                residual,
            }),
        },
        Plan::CollectionScan {
            collection_id,
            predicate,
            ..
        } => Box::new(ScanSource {
            it: latest.iter(),
            collection_id,
            predicate,
        }),
    };

    // Build a sorted key source (potentially spilling to Temp segments).
    let spill_file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .create(false)
        .open(path)
        .map_err(DbError::Io)?;
    let spill_store = crate::storage::FileStore::new(spill_file);
    let spill = crate::spill::TempSpillFile::new(spill_store)?;

    let sort_source = Box::new(ExternalSortSource::new(
        spill, latest, base, col.id.0, order_by,
    )?);

    let mut source: Box<dyn RowSource + 'a> = sort_source;
    match q.limit {
        Some(n) => source = Box::new(LimitOp::new(source, n)),
        None => {}
    }

    Ok(QueryRowIter {
        state: QueryRowIterState::Source { latest, source },
    })
}

#[derive(Clone)]
struct SortItem {
    // `none_flag`: 0 for Some, 1 for None (so None sorts last on ascending).
    none_flag: u8,
    sort_key: Vec<u8>,
    key: RowKey,
}

fn sort_item_for(
    latest: &crate::db::LatestMap,
    key: &RowKey,
    order_by: &OrderBy,
) -> Option<SortItem> {
    let (cid, pk) = key;
    let row = latest.get(&(cid.0, pk.clone()))?;
    let (none_flag, sort_key) = match scalar_at_path(row, &order_by.path) {
        None => (1u8, Vec::new()),
        Some(s) => (0u8, scalar_sort_key_bytes(&s)),
    };
    Some(SortItem {
        none_flag,
        sort_key,
        key: (CollectionId(cid.0), pk.clone()),
    })
}

fn scalar_sort_key_bytes(s: &ScalarValue) -> Vec<u8> {
    match s {
        ScalarValue::Bool(b) => {
            let v = match b {
                true => 1,
                false => 0,
            };
            vec![0, v]
        }
        ScalarValue::Int64(v) => {
            let u = (*v as u64) ^ 0x8000_0000_0000_0000u64;
            let mut out = vec![1];
            out.extend_from_slice(&u.to_be_bytes());
            out
        }
        ScalarValue::Uint64(v) => {
            let mut out = vec![2];
            out.extend_from_slice(&v.to_be_bytes());
            out
        }
        ScalarValue::Float64(v) => {
            let mut bits = v.to_bits();
            match (bits & (1u64 << 63)) != 0 {
                true => bits = !bits,
                false => bits ^= 1u64 << 63,
            }
            let mut out = vec![3];
            out.extend_from_slice(&bits.to_be_bytes());
            out
        }
        ScalarValue::String(st) => {
            let mut out = vec![4];
            out.extend_from_slice(st.as_bytes());
            out
        }
        ScalarValue::Bytes(b) => {
            let mut out = vec![5];
            out.extend_from_slice(b);
            out
        }
        ScalarValue::Uuid(u) => {
            let mut out = vec![6];
            out.extend_from_slice(u);
            out
        }
        ScalarValue::Timestamp(t) => {
            let u = (*t as u64) ^ 0x8000_0000_0000_0000u64;
            let mut out = vec![7];
            out.extend_from_slice(&u.to_be_bytes());
            out
        }
    }
}

fn cmp_sort_item(a: &SortItem, b: &SortItem, dir: OrderDirection) -> std::cmp::Ordering {
    let ord = a
        .none_flag
        .cmp(&b.none_flag)
        .then_with(|| a.sort_key.cmp(&b.sort_key))
        .then_with(|| a.key.1.cmp(&b.key.1));
    match dir {
        OrderDirection::Asc => ord,
        OrderDirection::Desc => ord.reverse(),
    }
}

// Simple external sort: sort fixed-size runs, spill each run as one Temp segment,
// then k-way merge those runs.
struct ExternalSortSource<'a> {
    _spill: crate::spill::TempSpillFile<crate::storage::FileStore>,
    collection_id: u32,
    dir: OrderDirection,
    heap: std::collections::BinaryHeap<HeapItem>,
    runs: Vec<RunReader>,
    _latest: &'a crate::db::LatestMap,
}

#[derive(Clone)]
struct RunMeta {
    offset: u64,
    payload_len: u64,
}

struct RunReader {
    buf: Vec<u8>,
    pos: usize,
}

impl RunReader {
    fn new(buf: Vec<u8>) -> Self {
        Self { buf, pos: 0 }
    }

    fn next_item(&mut self) -> Option<(u8, Vec<u8>, Vec<u8>)> {
        fn read_u32(buf: &[u8], pos: &mut usize) -> Option<u32> {
            let b = buf.get(*pos..*pos + 4)?;
            *pos += 4;
            Some(u32::from_le_bytes([b[0], b[1], b[2], b[3]]))
        }
        let none_flag = *self.buf.get(self.pos)?;
        self.pos += 1;
        let key_len = read_u32(&self.buf, &mut self.pos)? as usize;
        let key = self.buf.get(self.pos..self.pos + key_len)?.to_vec();
        self.pos += key_len;
        let pk_len = read_u32(&self.buf, &mut self.pos)? as usize;
        let pk = self.buf.get(self.pos..self.pos + pk_len)?.to_vec();
        self.pos += pk_len;
        Some((none_flag, key, pk))
    }
}

#[derive(Clone)]
struct HeapItem {
    run_idx: usize,
    none_flag: u8,
    sort_key: Vec<u8>,
    pk: Vec<u8>,
    dir: OrderDirection,
}

impl PartialEq for HeapItem {
    fn eq(&self, other: &Self) -> bool {
        (self.none_flag, &self.sort_key, &self.pk) == (other.none_flag, &other.sort_key, &other.pk)
    }
}
impl Eq for HeapItem {}

impl PartialOrd for HeapItem {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for HeapItem {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // BinaryHeap is max-heap; invert to get min-heap behavior.
        let a = SortItem {
            none_flag: self.none_flag,
            sort_key: self.sort_key.clone(),
            key: (CollectionId(0), self.pk.clone()),
        };
        let b = SortItem {
            none_flag: other.none_flag,
            sort_key: other.sort_key.clone(),
            key: (CollectionId(0), other.pk.clone()),
        };
        cmp_sort_item(&a, &b, self.dir).reverse()
    }
}

impl<'a> ExternalSortSource<'a> {
    fn new(
        mut spill: crate::spill::TempSpillFile<crate::storage::FileStore>,
        latest: &'a crate::db::LatestMap,
        mut input: Box<dyn RowSource + 'a>,
        collection_id: u32,
        order_by: OrderBy,
    ) -> Result<Self, DbError> {
        const RUN_KEYS: usize = 2048;

        let dir = order_by.direction;
        let mut runs_meta: Vec<RunMeta> = Vec::new();
        let mut run: Vec<SortItem> = Vec::with_capacity(RUN_KEYS);

        while let Some(rk) = input.next_key() {
            let rk = rk?;
            match sort_item_for(latest, &rk, &order_by) {
                Some(item) => run.push(item),
                None => {}
            }
            if run.len() >= RUN_KEYS {
                run.sort_by(|a, b| cmp_sort_item(a, b, dir));
                let payload = encode_run(&run, dir);
                let off = spill.append_temp_segment(&payload)?;
                runs_meta.push(RunMeta {
                    offset: off,
                    payload_len: payload.len() as u64,
                });
                run.clear();
            }
        }

        match run.is_empty() {
            true => {}
            false => {
                run.sort_by(|a, b| cmp_sort_item(a, b, dir));
                let payload = encode_run(&run, dir);
                let off = spill.append_temp_segment(&payload)?;
                runs_meta.push(RunMeta {
                    offset: off,
                    payload_len: payload.len() as u64,
                });
            }
        }

        // Load run buffers and seed heap.
        let mut runs: Vec<RunReader> = Vec::new();
        let mut heap = std::collections::BinaryHeap::new();
        for (i, m) in runs_meta.into_iter().enumerate() {
            let buf = spill.read_temp_payload(m.offset, m.payload_len)?;
            let mut rr = RunReader::new(buf);
            match rr.next_item() {
                Some((none_flag, sort_key, pk)) => heap.push(HeapItem {
                    run_idx: i,
                    none_flag,
                    sort_key,
                    pk: pk.clone(),
                    dir,
                }),
                None => {}
            }
            runs.push(rr);
        }

        Ok(Self {
            _spill: spill,
            collection_id,
            dir,
            heap,
            runs,
            _latest: latest,
        })
    }
}

fn encode_run(run: &[SortItem], _dir: OrderDirection) -> Vec<u8> {
    let mut out = Vec::new();
    for it in run {
        out.push(it.none_flag);
        out.extend_from_slice(&(it.sort_key.len() as u32).to_le_bytes());
        out.extend_from_slice(&it.sort_key);
        out.extend_from_slice(&(it.key.1.len() as u32).to_le_bytes());
        out.extend_from_slice(&it.key.1);
    }
    out
}

impl RowSource for ExternalSortSource<'_> {
    fn next_key(&mut self) -> Option<Result<RowKey, DbError>> {
        let top = self.heap.pop()?;
        let run_idx = top.run_idx;
        // refill from same run
        match self.runs[run_idx].next_item() {
            Some((none_flag, sort_key, pk)) => {
                self.heap.push(HeapItem {
                    run_idx,
                    none_flag,
                    sort_key,
                    pk: pk.clone(),
                    dir: self.dir,
                });
            }
            None => {}
        }
        Some(Ok((CollectionId(self.collection_id), top.pk)))
    }
}

fn plan_query(
    collection: CollectionId,
    indexes: &[crate::schema::IndexDef],
    query: &Query,
) -> Result<Plan, DbError> {
    let pred = match query.predicate.clone() {
        Some(p) => p,
        None => {
            return Ok(Plan::CollectionScan {
                collection_id: collection.0,
                predicate: None,
                limit: query.limit,
                order_by: query.order_by.clone(),
            });
        }
    };

    let (best, residual) = match choose_index(indexes, &pred) {
        None => (None, Some(pred)),
        Some((idx, value, used_pred)) => {
            let residual = remove_used_predicate(pred, used_pred);
            (Some((idx, value)), residual)
        }
    };

    match best {
        Some((idx, value)) => Ok(Plan::IndexLookup {
            collection_id: collection.0,
            index_name: idx.name.clone(),
            kind: idx.kind,
            key: value.canonical_key_bytes(),
            residual,
            limit: query.limit,
            order_by: query.order_by.clone(),
        }),
        None => Ok(Plan::CollectionScan {
            collection_id: collection.0,
            predicate: residual,
            limit: query.limit,
            order_by: query.order_by.clone(),
        }),
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
                match choose_index(indexes, p) {
                    None => {}
                    Some((idx, v, used)) => match best {
                        None => best = Some((idx, v, used)),
                        Some((best_idx, _, _)) => {
                            match (best_idx.kind, idx.kind) {
                                // Never downgrade away from a unique choice.
                                (IndexKind::Unique, _) => {}
                                // Upgrade any non-unique best to a unique candidate.
                                (_, IndexKind::Unique) => best = Some((idx, v, used)),
                                // Otherwise keep the first indexed predicate.
                                _ => {}
                            };
                        }
                    },
                }
            }
            best
        }
    }
}

fn remove_used_predicate(pred: Predicate, used: Predicate) -> Option<Predicate> {
    match pred == used {
        true => return None,
        false => {}
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
        Predicate::Eq { path, value } => match scalar_at_path(row, path) {
            Some(s) => &s == value,
            None => false,
        },
        Predicate::Lt { path, value } => match scalar_at_path(row, path) {
            Some(s) => match scalar_partial_cmp(&s, value) {
                Some(o) => o.is_lt(),
                None => false,
            },
            None => false,
        },
        Predicate::Lte { path, value } => match scalar_at_path(row, path) {
            Some(s) => match scalar_partial_cmp(&s, value) {
                Some(o) => o.is_lt() || o.is_eq(),
                None => false,
            },
            None => false,
        },
        Predicate::Gt { path, value } => match scalar_at_path(row, path) {
            Some(s) => match scalar_partial_cmp(&s, value) {
                Some(o) => o.is_gt(),
                None => false,
            },
            None => false,
        },
        Predicate::Gte { path, value } => match scalar_at_path(row, path) {
            Some(s) => match scalar_partial_cmp(&s, value) {
                Some(o) => o.is_gt() || o.is_eq(),
                None => false,
            },
            None => false,
        },
        Predicate::And(items) => items.iter().all(|p| eval_predicate(row, p)),
        Predicate::Or(items) => items.iter().any(|p| eval_predicate(row, p)),
    }
}

fn apply_order_by_and_limit(
    rows: &mut Vec<BTreeMap<String, RowValue>>,
    order_by: Option<&OrderBy>,
    limit: Option<usize>,
) {
    match order_by {
        Some(ob) => rows.sort_by(|a, b| {
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
        }),
        None => {}
    }
    match limit {
        Some(n) => rows.truncate(n),
        None => {}
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::{CatalogRecordWire, Catalog as MemCatalog};
    use crate::index::{IndexEntry, IndexOp, IndexState};
    use crate::record::RowValue;
    use crate::schema::{CollectionId, FieldDef, FieldPath, IndexDef, IndexKind, Type};
    use std::borrow::Cow;
    use std::collections::{BTreeMap, HashMap};

    fn field(name: &str, ty: Type) -> FieldDef {
        FieldDef {
            path: FieldPath(vec![Cow::Owned(name.to_string())]),
            ty,
            constraints: vec![],
        }
    }

    fn build_catalog_with_indexes(
        collection_id: u32,
        fields: Vec<FieldDef>,
        indexes: Vec<IndexDef>,
        primary: &str,
    ) -> MemCatalog {
        let mut catalog = MemCatalog::default();
        catalog
            .apply_record(CatalogRecordWire::CreateCollection {
                collection_id,
                name: "t".to_string(),
                schema_version: 1,
                fields,
                indexes,
                primary_field: Some(primary.to_string()),
            })
            .unwrap();
        catalog
    }

    #[test]
    fn execute_query_index_lookup_skips_missing_latest_row() {
        let catalog = build_catalog_with_indexes(
            1,
            vec![field("id", Type::String), field("x", Type::String)],
            vec![IndexDef {
                name: "x_uq".to_string(),
                path: FieldPath(vec![Cow::Owned("x".to_string())]),
                kind: IndexKind::Unique,
            }],
            "id",
        );

        let mut indexes = IndexState::default();
        indexes
            .apply(IndexEntry {
                collection_id: 1,
                index_name: "x_uq".to_string(),
                kind: IndexKind::Unique,
                op: IndexOp::Insert,
                index_key: ScalarValue::String("v".to_string()).canonical_key_bytes(),
                pk_key: b"missing_pk".to_vec(),
            })
            .unwrap();

        let latest: crate::db::LatestMap = HashMap::new();
        let q = Query {
            collection: CollectionId(1),
            predicate: Some(Predicate::Eq {
                path: FieldPath(vec![Cow::Owned("x".to_string())]),
                value: ScalarValue::String("v".to_string()),
            }),
            limit: None,
            order_by: None,
        };
        let rows = execute_query(&catalog, &indexes, &latest, &q).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn execute_query_index_lookup_pushes_existing_latest_row() {
        let catalog = build_catalog_with_indexes(
            1,
            vec![field("id", Type::String), field("x", Type::String)],
            vec![IndexDef {
                name: "x_uq".to_string(),
                path: FieldPath(vec![Cow::Owned("x".to_string())]),
                kind: IndexKind::Unique,
            }],
            "id",
        );

        let pk_key = b"pk".to_vec();
        let mut indexes = IndexState::default();
        indexes
            .apply(IndexEntry {
                collection_id: 1,
                index_name: "x_uq".to_string(),
                kind: IndexKind::Unique,
                op: IndexOp::Insert,
                index_key: ScalarValue::String("v".to_string()).canonical_key_bytes(),
                pk_key: pk_key.clone(),
            })
            .unwrap();

        let mut latest: crate::db::LatestMap = HashMap::new();
        latest.insert(
            (1, pk_key.clone()),
            BTreeMap::from([
                ("id".to_string(), RowValue::String("pk".to_string())),
                ("x".to_string(), RowValue::String("v".to_string())),
            ]),
        );

        let q = Query {
            collection: CollectionId(1),
            predicate: Some(Predicate::Eq {
                path: FieldPath(vec![Cow::Owned("x".to_string())]),
                value: ScalarValue::String("v".to_string()),
            }),
            limit: None,
            order_by: None,
        };
        let rows = execute_query(&catalog, &indexes, &latest, &q).unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn execute_query_collection_scan_filters_out_non_matches() {
        let catalog = build_catalog_with_indexes(
            1,
            vec![field("id", Type::String), field("y", Type::Int64)],
            vec![],
            "id",
        );
        let indexes = IndexState::default();
        let mut latest: crate::db::LatestMap = HashMap::new();
        latest.insert(
            (1, b"a".to_vec()),
            BTreeMap::from([
                ("id".to_string(), RowValue::String("a".to_string())),
                ("y".to_string(), RowValue::Int64(1)),
            ]),
        );

        let q = Query {
            collection: CollectionId(1),
            predicate: Some(Predicate::Eq {
                path: FieldPath(vec![Cow::Owned("y".to_string())]),
                value: ScalarValue::Int64(2),
            }),
            limit: None,
            order_by: None,
        };
        let rows = execute_query(&catalog, &indexes, &latest, &q).unwrap();
        assert!(rows.is_empty());
    }

    #[test]
    fn execute_query_collection_scan_includes_matches() {
        let catalog = build_catalog_with_indexes(
            1,
            vec![field("id", Type::String), field("y", Type::Int64)],
            vec![],
            "id",
        );
        let indexes = IndexState::default();
        let mut latest: crate::db::LatestMap = HashMap::new();
        latest.insert(
            (1, b"a".to_vec()),
            BTreeMap::from([
                ("id".to_string(), RowValue::String("a".to_string())),
                ("y".to_string(), RowValue::Int64(2)),
            ]),
        );
        let q = Query {
            collection: CollectionId(1),
            predicate: Some(Predicate::Eq {
                path: FieldPath(vec![Cow::Owned("y".to_string())]),
                value: ScalarValue::Int64(2),
            }),
            limit: None,
            order_by: None,
        };
        let rows = execute_query(&catalog, &indexes, &latest, &q).unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn query_row_iter_vec_returns_none_when_empty() {
        let it = QueryRowIter {
            state: QueryRowIterState::Vec {
                rows: Vec::new(),
                pos: 0,
            },
        };
        assert!(it.into_iter().next().is_none());
    }

    #[test]
    fn query_row_iter_vec_advances_then_none() {
        let catalog = build_catalog_with_indexes(
            1,
            vec![field("id", Type::String)],
            vec![],
            "id",
        );
        let indexes = IndexState::default();
        let mut latest: crate::db::LatestMap = HashMap::new();
        latest.insert(
            (1, b"a".to_vec()),
            BTreeMap::from([("id".to_string(), RowValue::String("a".to_string()))]),
        );
        let q = Query {
            collection: CollectionId(1),
            predicate: None,
            limit: None,
            order_by: Some(OrderBy {
                path: FieldPath(vec![Cow::Owned("id".to_string())]),
                direction: OrderDirection::Asc,
            }),
        };
        let mut it = execute_query_iter(&catalog, &indexes, &latest, &q).unwrap();
        assert!(it.next().is_some());
        assert!(it.next().is_none());
    }

    #[test]
    fn query_row_iter_source_skips_missing_latest_and_finishes() {
        struct OneKeyThenNone {
            yielded: bool,
        }
        impl RowSource for OneKeyThenNone {
            fn next_key(&mut self) -> Option<Result<RowKey, DbError>> {
                match self.yielded {
                    true => None,
                    false => {
                        self.yielded = true;
                        Some(Ok((CollectionId(1), b"missing".to_vec())))
                    }
                }
            }
        }

        let latest: crate::db::LatestMap = HashMap::new();
        let mut it = QueryRowIter {
            state: QueryRowIterState::Source {
                latest: &latest,
                source: Box::new(OneKeyThenNone { yielded: false }),
            },
        };
        assert!(it.next().is_none());
    }

    #[test]
    fn query_row_iter_source_returns_row_when_present() {
        struct OneKeyThenNone {
            yielded: bool,
            key: Vec<u8>,
        }
        impl RowSource for OneKeyThenNone {
            fn next_key(&mut self) -> Option<Result<RowKey, DbError>> {
                match self.yielded {
                    true => None,
                    false => {
                        self.yielded = true;
                        Some(Ok((CollectionId(1), self.key.clone())))
                    }
                }
            }
        }
        let mut latest: crate::db::LatestMap = HashMap::new();
        latest.insert(
            (1, b"a".to_vec()),
            BTreeMap::from([("id".to_string(), RowValue::String("a".to_string()))]),
        );
        let mut it = QueryRowIter {
            state: QueryRowIterState::Source {
                latest: &latest,
                source: Box::new(OneKeyThenNone {
                    yielded: false,
                    key: b"a".to_vec(),
                }),
            },
        };
        assert!(it.next().unwrap().is_ok());
        assert!(it.next().is_none());
    }

    #[test]
    fn index_unique_source_residual_predicate_filters_row() {
        let mut latest: crate::db::LatestMap = HashMap::new();
        latest.insert(
            (1, b"a".to_vec()),
            BTreeMap::from([
                ("id".to_string(), RowValue::String("a".to_string())),
                ("x".to_string(), RowValue::Int64(2)),
            ]),
        );
        let mut src = IndexUniqueSource {
            latest: &latest,
            collection_id: 1,
            pk: Some(b"a".to_vec()),
            residual: Some(Predicate::Eq {
                path: FieldPath(vec![Cow::Owned("x".to_string())]),
                value: ScalarValue::Int64(1),
            }),
            done: false,
        };
        assert!(src.next_key().is_none());
    }

    #[test]
    fn index_unique_source_yields_when_no_residual() {
        let mut latest: crate::db::LatestMap = HashMap::new();
        latest.insert(
            (1, b"a".to_vec()),
            BTreeMap::from([("id".to_string(), RowValue::String("a".to_string()))]),
        );
        let mut src = IndexUniqueSource {
            latest: &latest,
            collection_id: 1,
            pk: Some(b"a".to_vec()),
            residual: None,
            done: false,
        };
        assert!(src.next_key().unwrap().is_ok());
        assert!(src.next_key().is_none());
    }

    #[test]
    fn index_non_unique_source_skips_missing_rows_and_residual_non_matches() {
        let mut latest: crate::db::LatestMap = HashMap::new();
        latest.insert(
            (1, b"present".to_vec()),
            BTreeMap::from([
                ("id".to_string(), RowValue::String("present".to_string())),
                ("x".to_string(), RowValue::Int64(2)),
            ]),
        );
        let mut src = IndexNonUniqueSource {
            latest: &latest,
            collection_id: 1,
            pks: vec![b"missing".to_vec(), b"present".to_vec()].into_iter(),
            residual: Some(Predicate::Eq {
                path: FieldPath(vec![Cow::Owned("x".to_string())]),
                value: ScalarValue::Int64(1),
            }),
        };
        assert!(src.next_key().is_none());
    }

    #[test]
    fn index_non_unique_source_yields_when_residual_matches() {
        let mut latest: crate::db::LatestMap = HashMap::new();
        latest.insert(
            (1, b"present".to_vec()),
            BTreeMap::from([
                ("id".to_string(), RowValue::String("present".to_string())),
                ("x".to_string(), RowValue::Int64(1)),
            ]),
        );
        let mut src = IndexNonUniqueSource {
            latest: &latest,
            collection_id: 1,
            pks: vec![b"present".to_vec()].into_iter(),
            residual: Some(Predicate::Eq {
                path: FieldPath(vec![Cow::Owned("x".to_string())]),
                value: ScalarValue::Int64(1),
            }),
        };
        assert!(src.next_key().unwrap().is_ok());
        assert!(src.next_key().is_none());
    }

    #[test]
    fn scalar_sort_key_bytes_covers_float64_sign_branches() {
        let _neg = scalar_sort_key_bytes(&ScalarValue::Float64(-1.0));
        let _pos = scalar_sort_key_bytes(&ScalarValue::Float64(1.0));
    }

    #[test]
    fn scalar_sort_key_bytes_covers_bool_true_false() {
        let _t = scalar_sort_key_bytes(&ScalarValue::Bool(true));
        let _f = scalar_sort_key_bytes(&ScalarValue::Bool(false));
    }

    #[test]
    fn choose_index_prefers_unique_predicate_in_and() {
        let indexes = vec![
            IndexDef {
                name: "a".to_string(),
                path: FieldPath(vec![Cow::Owned("a".to_string())]),
                kind: IndexKind::NonUnique,
            },
            IndexDef {
                name: "b".to_string(),
                path: FieldPath(vec![Cow::Owned("b".to_string())]),
                kind: IndexKind::Unique,
            },
        ];
        let pred = Predicate::And(vec![
            Predicate::Eq {
                path: FieldPath(vec![Cow::Owned("a".to_string())]),
                value: ScalarValue::Int64(1),
            },
            Predicate::Eq {
                path: FieldPath(vec![Cow::Owned("b".to_string())]),
                value: ScalarValue::Int64(2),
            },
        ]);
        let (idx, _v, _used) = choose_index(&indexes, &pred).unwrap();
        assert_eq!(idx.name, "b");
        assert_eq!(idx.kind, IndexKind::Unique);
    }

    #[test]
    fn choose_index_does_not_downgrade_unique_in_and() {
        let indexes = vec![
            IndexDef {
                name: "a".to_string(),
                path: FieldPath(vec![Cow::Owned("a".to_string())]),
                kind: IndexKind::Unique,
            },
            IndexDef {
                name: "b".to_string(),
                path: FieldPath(vec![Cow::Owned("b".to_string())]),
                kind: IndexKind::NonUnique,
            },
        ];
        let pred = Predicate::And(vec![
            Predicate::Eq {
                path: FieldPath(vec![Cow::Owned("a".to_string())]),
                value: ScalarValue::Int64(1),
            },
            Predicate::Eq {
                path: FieldPath(vec![Cow::Owned("b".to_string())]),
                value: ScalarValue::Int64(2),
            },
        ]);
        let (idx, _v, _used) = choose_index(&indexes, &pred).unwrap();
        assert_eq!(idx.name, "a");
        assert_eq!(idx.kind, IndexKind::Unique);
    }

    #[test]
    fn choose_index_does_not_upgrade_when_both_non_unique() {
        let indexes = vec![
            IndexDef {
                name: "a".to_string(),
                path: FieldPath(vec![Cow::Owned("a".to_string())]),
                kind: IndexKind::NonUnique,
            },
            IndexDef {
                name: "b".to_string(),
                path: FieldPath(vec![Cow::Owned("b".to_string())]),
                kind: IndexKind::NonUnique,
            },
        ];
        let pred = Predicate::And(vec![
            Predicate::Eq {
                path: FieldPath(vec![Cow::Owned("a".to_string())]),
                value: ScalarValue::Int64(1),
            },
            Predicate::Eq {
                path: FieldPath(vec![Cow::Owned("b".to_string())]),
                value: ScalarValue::Int64(2),
            },
        ]);
        let (idx, _v, _used) = choose_index(&indexes, &pred).unwrap();
        assert_eq!(idx.name, "a");
        assert_eq!(idx.kind, IndexKind::NonUnique);
    }

    #[test]
    fn eval_predicate_covers_lte_and_gte_eq_paths() {
        let row = BTreeMap::from([("x".to_string(), RowValue::Int64(2))]);
        assert!(eval_predicate(
            &row,
            &Predicate::Lte {
                path: FieldPath(vec![Cow::Owned("x".to_string())]),
                value: ScalarValue::Int64(2),
            }
        ));
        assert!(eval_predicate(
            &row,
            &Predicate::Gte {
                path: FieldPath(vec![Cow::Owned("x".to_string())]),
                value: ScalarValue::Int64(2),
            }
        ));
    }

    #[test]
    fn external_sort_source_spills_multiple_runs_and_refills_heap() {
        use crate::spill::TempSpillFile;
        use crate::storage::FileStore;

        // Build latest with enough rows to trigger the RUN_KEYS spill threshold.
        let mut latest: crate::db::LatestMap = HashMap::new();
        for i in 0..2050u32 {
            latest.insert(
                (1, i.to_le_bytes().to_vec()),
                BTreeMap::from([
                    ("id".to_string(), RowValue::Uint64(i as u64)),
                    ("x".to_string(), RowValue::Uint64(i as u64)),
                ]),
            );
        }

        struct Keys {
            i: u32,
            end: u32,
        }
        impl RowSource for Keys {
            fn next_key(&mut self) -> Option<Result<RowKey, DbError>> {
                if self.i >= self.end {
                    return None;
                }
                let pk = self.i.to_le_bytes().to_vec();
                self.i += 1;
                Some(Ok((CollectionId(1), pk)))
            }
        }

        let tmp = tempfile::NamedTempFile::new().unwrap();
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(tmp.path())
            .unwrap();
        let spill = TempSpillFile::new(FileStore::new(f)).unwrap();

        let ob = OrderBy {
            path: FieldPath(vec![Cow::Owned("x".to_string())]),
            direction: OrderDirection::Asc,
        };

        let mut src = ExternalSortSource::new(
            spill,
            &latest,
            Box::new(Keys { i: 0, end: 2050 }),
            1,
            ob,
        )
        .unwrap();

        // Pull one key; this should pop from heap and attempt a refill.
        let _ = src.next_key().unwrap().unwrap();
    }

    #[test]
    fn external_sort_source_new_with_empty_input_yields_none() {
        use crate::spill::TempSpillFile;
        use crate::storage::FileStore;

        let latest: crate::db::LatestMap = HashMap::new();
        struct Empty;
        impl RowSource for Empty {
            fn next_key(&mut self) -> Option<Result<RowKey, DbError>> {
                None
            }
        }

        let tmp = tempfile::NamedTempFile::new().unwrap();
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(tmp.path())
            .unwrap();
        let spill = TempSpillFile::new(FileStore::new(f)).unwrap();

        let ob = OrderBy {
            path: FieldPath(vec![Cow::Owned("x".to_string())]),
            direction: OrderDirection::Asc,
        };

        let mut src = ExternalSortSource::new(spill, &latest, Box::new(Empty), 1, ob).unwrap();
        assert!(src.next_key().is_none());
    }

    #[test]
    fn external_sort_source_ignores_keys_missing_latest_rows() {
        use crate::spill::TempSpillFile;
        use crate::storage::FileStore;

        let latest: crate::db::LatestMap = HashMap::new();
        struct OneMissing {
            done: bool,
        }
        impl RowSource for OneMissing {
            fn next_key(&mut self) -> Option<Result<RowKey, DbError>> {
                match self.done {
                    true => None,
                    false => {
                        self.done = true;
                        Some(Ok((CollectionId(1), b"missing".to_vec())))
                    }
                }
            }
        }

        let tmp = tempfile::NamedTempFile::new().unwrap();
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(tmp.path())
            .unwrap();
        let spill = TempSpillFile::new(FileStore::new(f)).unwrap();
        let ob = OrderBy {
            path: FieldPath(vec![Cow::Owned("x".to_string())]),
            direction: OrderDirection::Asc,
        };

        let mut src =
            ExternalSortSource::new(spill, &latest, Box::new(OneMissing { done: false }), 1, ob)
                .unwrap();
        assert!(src.next_key().is_none());
    }

    #[test]
    fn external_sort_source_next_key_no_refill_when_run_exhausted() {
        use crate::spill::TempSpillFile;
        use crate::storage::FileStore;

        // Single row => single run with a single item; after pop, refill returns None.
        let mut latest: crate::db::LatestMap = HashMap::new();
        latest.insert(
            (1, b"a".to_vec()),
            BTreeMap::from([
                ("id".to_string(), RowValue::String("a".to_string())),
                ("x".to_string(), RowValue::Uint64(1)),
            ]),
        );

        struct One {
            done: bool,
        }
        impl RowSource for One {
            fn next_key(&mut self) -> Option<Result<RowKey, DbError>> {
                match self.done {
                    true => None,
                    false => {
                        self.done = true;
                        Some(Ok((CollectionId(1), b"a".to_vec())))
                    }
                }
            }
        }

        let tmp = tempfile::NamedTempFile::new().unwrap();
        let f = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(tmp.path())
            .unwrap();
        let spill = TempSpillFile::new(FileStore::new(f)).unwrap();
        let ob = OrderBy {
            path: FieldPath(vec![Cow::Owned("x".to_string())]),
            direction: OrderDirection::Asc,
        };

        let mut src = ExternalSortSource::new(
            spill,
            &latest,
            Box::new(One { done: false }),
            1,
            ob,
        )
        .unwrap();
        assert!(src.next_key().unwrap().is_ok());
        assert!(src.next_key().is_none());
    }
}
