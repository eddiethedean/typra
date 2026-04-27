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
                if let Some(row) = latest.get(&(collection_id, pk_key)).cloned() {
                    out.push(row);
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
                if *pos >= rows.len() {
                    None
                } else {
                    let out = rows[*pos].clone();
                    *pos += 1;
                    Some(Ok(out))
                }
            }
            QueryRowIterState::Source { latest, source } => loop {
                let rk = source.next_key()?;
                match rk {
                    Err(e) => return Some(Err(e)),
                    Ok((cid, pk_key)) => {
                        if let Some(row) = latest.get(&(cid.0, pk_key)).cloned() {
                            return Some(Ok(row));
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
            Some(pred) => {
                if !eval_predicate(row, pred) {
                    return None;
                }
            }
            None => {}
        }
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
            let Some(row) = self.latest.get(&(self.collection_id, pk_key.clone())) else {
                continue;
            };
            if let Some(pred) = &self.residual {
                if !eval_predicate(row, pred) {
                    continue;
                }
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
                Some(p) => {
                    if !eval_predicate(row, p) {
                        continue;
                    }
                }
                None => {}
            }
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
        ScalarValue::Bool(b) => vec![0, if *b { 1 } else { 0 }],
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
            if bits & (1u64 << 63) != 0 {
                bits = !bits;
            } else {
                bits ^= 1u64 << 63;
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
            if let Some(item) = sort_item_for(latest, &rk, &order_by) {
                run.push(item);
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

        if !run.is_empty() {
            run.sort_by(|a, b| cmp_sort_item(a, b, dir));
            let payload = encode_run(&run, dir);
            let off = spill.append_temp_segment(&payload)?;
            runs_meta.push(RunMeta {
                offset: off,
                payload_len: payload.len() as u64,
            });
        }

        // Load run buffers and seed heap.
        let mut runs: Vec<RunReader> = Vec::new();
        let mut heap = std::collections::BinaryHeap::new();
        for (i, m) in runs_meta.into_iter().enumerate() {
            let buf = spill.read_temp_payload(m.offset, m.payload_len)?;
            let mut rr = RunReader::new(buf);
            if let Some((none_flag, sort_key, pk)) = rr.next_item() {
                heap.push(HeapItem {
                    run_idx: i,
                    none_flag,
                    sort_key,
                    pk: pk.clone(),
                    dir,
                });
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
        if let Some((none_flag, sort_key, pk)) = self.runs[run_idx].next_item() {
            self.heap.push(HeapItem {
                run_idx,
                none_flag,
                sort_key,
                pk: pk.clone(),
                dir: self.dir,
            });
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
                            let should_upgrade =
                                best_idx.kind != IndexKind::Unique && idx.kind == IndexKind::Unique;
                            if should_upgrade {
                                best = Some((idx, v, used));
                            }
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
