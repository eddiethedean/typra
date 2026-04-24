use std::collections::HashMap;

use crate::db::scalar_at_path;
use crate::error::{DbError, QueryError};
use crate::record::RowValue;
use crate::schema::FieldPath;
use crate::spill::TempSpillFile;
use crate::storage::{FileStore, Store};
use crate::ScalarValue;

fn qerr(msg: impl Into<String>) -> DbError {
    DbError::Query(QueryError {
        message: msg.into(),
    })
}

#[derive(Clone, Debug)]
struct AggVal {
    count: u64,
    sum: i64,
}

#[derive(Clone, Debug)]
struct SpillSeg {
    offset: u64,
    payload_len: u64,
    partition: u8,
}

fn part_for_i64(k: i64) -> u8 {
    // Simple stable hash -> partition in [0, 63].
    let x = (k as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    (x & 63) as u8
}

fn encode_partition_entries(entries: &[(i64, AggVal)]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for (k, v) in entries {
        out.extend_from_slice(&k.to_le_bytes());
        out.extend_from_slice(&v.count.to_le_bytes());
        out.extend_from_slice(&v.sum.to_le_bytes());
    }
    out
}

fn decode_partition_entries(buf: &[u8]) -> Result<Vec<(i64, AggVal)>, DbError> {
    if buf.len() < 4 {
        return Err(qerr("spill segment truncated"));
    }
    let n = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    let mut pos = 4usize;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        if pos + 8 + 8 + 8 > buf.len() {
            return Err(qerr("spill segment truncated"));
        }
        let k = i64::from_le_bytes(buf[pos..pos + 8].try_into().unwrap());
        pos += 8;
        let count = u64::from_le_bytes(buf[pos..pos + 8].try_into().unwrap());
        pos += 8;
        let sum = i64::from_le_bytes(buf[pos..pos + 8].try_into().unwrap());
        pos += 8;
        out.push((k, AggVal { count, sum }));
    }
    Ok(out)
}

fn flush_map_to_spill<S: Store>(
    map: &mut HashMap<i64, AggVal>,
    spill: &mut TempSpillFile<S>,
    spill_segs: &mut Vec<SpillSeg>,
) -> Result<(), DbError> {
    if map.is_empty() {
        return Ok(());
    }

    // Partition entries so merge can be bounded per partition.
    let mut parts: [Vec<(i64, AggVal)>; 64] = std::array::from_fn(|_| Vec::new());
    for (k, v) in map.drain() {
        parts[part_for_i64(k) as usize].push((k, v));
    }

    for (p, entries) in parts.into_iter().enumerate() {
        if entries.is_empty() {
            continue;
        }
        let payload = encode_partition_entries(&entries);
        let off = spill.append_temp_segment(&payload)?;
        spill_segs.push(SpillSeg {
            offset: off,
            payload_len: payload.len() as u64,
            partition: p as u8,
        });
    }

    Ok(())
}

/// Spillable aggregation (v0): group-by one `int64` field and compute `COUNT` + `SUM(int64)`.
///
/// - Uses a bounded in-memory hashmap (by number of groups).
/// - When the map exceeds `max_groups_in_mem`, it spills partial aggregates to `Temp` segments.
/// - Merge phase reads partitions one at a time to bound memory.
pub fn spillable_group_count_sum_i64<I>(
    rows: I,
    group_by: &FieldPath,
    sum_field: &FieldPath,
    max_groups_in_mem: usize,
    mut spill: Option<&mut TempSpillFile<FileStore>>,
) -> Result<Vec<(i64, u64, i64)>, DbError>
where
    I: Iterator<Item = Result<std::collections::BTreeMap<String, RowValue>, DbError>>,
{
    if max_groups_in_mem == 0 {
        return Err(qerr("max_groups_in_mem must be > 0"));
    }

    let mut map: HashMap<i64, AggVal> = HashMap::new();
    let mut spill_segs: Vec<SpillSeg> = Vec::new();

    for r in rows {
        let r = r?;
        let Some(ScalarValue::Int64(g)) = scalar_at_path(&r, group_by) else {
            continue;
        };
        let Some(ScalarValue::Int64(v)) = scalar_at_path(&r, sum_field) else {
            continue;
        };
        let e = map.entry(g).or_insert(AggVal { count: 0, sum: 0 });
        e.count += 1;
        e.sum = e.sum.wrapping_add(v);

        if map.len() > max_groups_in_mem {
            let Some(ref mut spill) = spill else {
                return Err(qerr(
                    "aggregation exceeded memory budget but no spill store was provided",
                ));
            };
            flush_map_to_spill(&mut map, spill, &mut spill_segs)?;
        }
    }

    if let Some(ref mut spill) = spill {
        flush_map_to_spill(&mut map, spill, &mut spill_segs)?;
    }

    // If we never spilled, we can return directly.
    if spill_segs.is_empty() {
        let mut out: Vec<_> = map.into_iter().map(|(k, v)| (k, v.count, v.sum)).collect();
        out.sort_by_key(|(k, _, _)| *k);
        return Ok(out);
    }

    let Some(spill) = spill else {
        return Err(qerr(
            "internal: spill segments exist but spill store missing",
        ));
    };

    // Merge partitions one at a time to bound memory.
    let mut by_part: [Vec<SpillSeg>; 64] = std::array::from_fn(|_| Vec::new());
    for s in spill_segs {
        by_part[s.partition as usize].push(s);
    }

    let mut out: Vec<(i64, u64, i64)> = Vec::new();
    for segs in by_part {
        if segs.is_empty() {
            continue;
        }
        let mut part_map: HashMap<i64, AggVal> = HashMap::new();
        for s in segs {
            let buf = spill.read_temp_payload(s.offset, s.payload_len)?;
            for (k, v) in decode_partition_entries(&buf)? {
                let e = part_map.entry(k).or_insert(AggVal { count: 0, sum: 0 });
                e.count += v.count;
                e.sum = e.sum.wrapping_add(v.sum);
            }
        }
        for (k, v) in part_map {
            out.push((k, v.count, v.sum));
        }
    }
    out.sort_by_key(|(k, _, _)| *k);
    Ok(out)
}
