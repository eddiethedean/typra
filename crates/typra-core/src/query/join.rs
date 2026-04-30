use std::collections::HashMap;

use crate::db::scalar_at_path;
use crate::error::{DbError, QueryError};
use crate::record::RowValue;
use crate::schema::FieldPath;
use crate::spill::TempSpillFile;
use crate::storage::Store;
use crate::ScalarValue;

fn qerr(msg: impl Into<String>) -> DbError {
    DbError::Query(QueryError {
        message: msg.into(),
    })
}

#[derive(Clone, Debug)]
struct SpillSeg {
    offset: u64,
    payload_len: u64,
    partition: u8,
}

fn part_for_i64(k: i64) -> u8 {
    let x = (k as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    (x & 63) as u8
}

fn encode_entries(entries: &[(i64, u64)]) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend_from_slice(&(entries.len() as u32).to_le_bytes());
    for (k, c) in entries {
        out.extend_from_slice(&k.to_le_bytes());
        out.extend_from_slice(&c.to_le_bytes());
    }
    out
}

fn decode_entries(buf: &[u8]) -> Result<Vec<(i64, u64)>, DbError> {
    if buf.len() < 4 {
        return Err(qerr("spill segment truncated"));
    }
    let n = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]) as usize;
    let mut pos = 4usize;
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        if pos + 16 > buf.len() {
            return Err(qerr("spill segment truncated"));
        }
        let k = i64::from_le_bytes(buf[pos..pos + 8].try_into().unwrap());
        pos += 8;
        let c = u64::from_le_bytes(buf[pos..pos + 8].try_into().unwrap());
        pos += 8;
        out.push((k, c));
    }
    Ok(out)
}

fn flush_counts_to_spill<S: Store>(
    counts: &mut HashMap<i64, u64>,
    spill: &mut TempSpillFile<S>,
    segs: &mut Vec<SpillSeg>,
) -> Result<(), DbError> {
    if counts.is_empty() {
        return Ok(());
    }
    let mut parts: [Vec<(i64, u64)>; 64] = std::array::from_fn(|_| Vec::new());
    for (k, c) in counts.drain() {
        parts[part_for_i64(k) as usize].push((k, c));
    }
    for (p, entries) in parts.into_iter().enumerate() {
        if entries.is_empty() {
            continue;
        }
        let payload = encode_entries(&entries);
        let off = spill.append_temp_segment(&payload)?;
        segs.push(SpillSeg {
            offset: off,
            payload_len: payload.len() as u64,
            partition: p as u8,
        });
    }
    Ok(())
}

/// Minimal spill-capable join foundation (v0): equi-join **match count** on one `int64` key.
///
/// This is intentionally small and internal. It proves:
/// - a join-shaped operator boundary,
/// - spill partitioning to `Temp` segments,
/// - a bounded merge phase.
pub fn spillable_hash_join_match_count_i64<I1, I2, S: Store>(
    left_rows: I1,
    right_rows: I2,
    left_on: &FieldPath,
    right_on: &FieldPath,
    max_keys_in_mem: usize,
    mut spill: Option<&mut TempSpillFile<S>>,
) -> Result<u64, DbError>
where
    I1: Iterator<Item = Result<std::collections::BTreeMap<String, RowValue>, DbError>>,
    I2: Iterator<Item = Result<std::collections::BTreeMap<String, RowValue>, DbError>>,
{
    if max_keys_in_mem == 0 {
        return Err(qerr("max_keys_in_mem must be > 0"));
    }

    // Right side key multiplicities (v0: materialized).
    let mut right_counts: HashMap<i64, u64> = HashMap::new();
    for r in right_rows {
        let r = r?;
        let Some(ScalarValue::Int64(k)) = scalar_at_path(&r, right_on) else {
            continue;
        };
        *right_counts.entry(k).or_insert(0) += 1;
    }

    let mut left_counts: HashMap<i64, u64> = HashMap::new();
    let mut segs: Vec<SpillSeg> = Vec::new();

    for r in left_rows {
        let r = r?;
        let Some(ScalarValue::Int64(k)) = scalar_at_path(&r, left_on) else {
            continue;
        };
        *left_counts.entry(k).or_insert(0) += 1;
        if left_counts.len() > max_keys_in_mem {
            let Some(ref mut spill) = spill else {
                return Err(qerr(
                    "join exceeded memory budget but no spill store was provided",
                ));
            };
            flush_counts_to_spill(&mut left_counts, spill, &mut segs)?;
        }
    }

    if let Some(ref mut spill) = spill {
        flush_counts_to_spill(&mut left_counts, spill, &mut segs)?;
    }

    // No spill path.
    if segs.is_empty() {
        let mut total = 0u64;
        for (k, lc) in left_counts {
            if let Some(rc) = right_counts.get(&k) {
                total = total.wrapping_add(lc.wrapping_mul(*rc));
            }
        }
        return Ok(total);
    }

    let spill = spill.expect("internal: spill segments exist but spill store missing");

    // Merge each partition and compute matches.
    let mut by_part: [Vec<SpillSeg>; 64] = std::array::from_fn(|_| Vec::new());
    for s in segs {
        by_part[s.partition as usize].push(s);
    }

    let mut total = 0u64;
    for segs in by_part {
        if segs.is_empty() {
            continue;
        }
        let mut part_counts: HashMap<i64, u64> = HashMap::new();
        for s in segs {
            let buf = spill.read_temp_payload(s.offset, s.payload_len)?;
            for (k, c) in decode_entries(&buf)? {
                *part_counts.entry(k).or_insert(0) += c;
            }
        }
        for (k, lc) in part_counts {
            if let Some(rc) = right_counts.get(&k) {
                total = total.wrapping_add(lc.wrapping_mul(*rc));
            }
        }
    }

    Ok(total)
}

#[cfg(test)]
mod tests {
    include!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/unit/src_query_join_tests.rs"
    ));
}
