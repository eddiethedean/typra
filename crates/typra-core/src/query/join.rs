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
    use super::*;
    use crate::spill::TempSpillFile;
    use crate::storage::VecStore;
    use std::collections::BTreeMap;

    fn fp(s: &'static str) -> FieldPath {
        FieldPath::new([std::borrow::Cow::Borrowed(s)]).unwrap()
    }

    fn row(k: i64) -> BTreeMap<String, RowValue> {
        let mut m = BTreeMap::new();
        m.insert("k".to_string(), RowValue::Int64(k));
        m
    }

    fn row_missing() -> BTreeMap<String, RowValue> {
        BTreeMap::new()
    }

    #[test]
    fn decode_entries_truncated_errors() {
        let err = decode_entries(&[]).unwrap_err();
        assert!(matches!(err, DbError::Query(_)));

        let mut buf = Vec::new();
        buf.extend_from_slice(&1u32.to_le_bytes());
        buf.extend_from_slice(&0i64.to_le_bytes());
        let err = decode_entries(&buf).unwrap_err();
        assert!(matches!(err, DbError::Query(_)));
    }

    #[test]
    fn join_rejects_zero_budget_and_requires_spill_store_when_over_budget() {
        let err = spillable_hash_join_match_count_i64::<_, _, VecStore>(
            std::iter::once(Ok(row(1))),
            std::iter::once(Ok(row(1))),
            &fp("k"),
            &fp("k"),
            0,
            None,
        )
        .unwrap_err();
        assert!(matches!(err, DbError::Query(_)));

        // Over budget with no spill store: 2 distinct keys with budget 1.
        let left = vec![Ok(row(1)), Ok(row(2))].into_iter();
        let right = vec![Ok(row(1))].into_iter();
        let err = spillable_hash_join_match_count_i64::<_, _, VecStore>(
            left,
            right,
            &fp("k"),
            &fp("k"),
            1,
            None,
        )
        .unwrap_err();
        assert!(matches!(err, DbError::Query(_)));
    }

    #[test]
    fn join_no_spill_path_counts_matches() {
        let left = vec![Ok(row_missing()), Ok(row(1)), Ok(row(1)), Ok(row(2))].into_iter();
        let right = vec![Ok(row_missing()), Ok(row(1)), Ok(row(3)), Ok(row(1))].into_iter();
        let total = spillable_hash_join_match_count_i64::<_, _, VecStore>(
            left,
            right,
            &fp("k"),
            &fp("k"),
            10,
            None,
        )
        .unwrap();
        // left(1)=2, right(1)=2 => 4 matches; key 2 has 0 matches.
        assert_eq!(total, 4);
    }

    #[test]
    fn join_spills_and_merges_partitions() {
        let left = vec![Ok(row(1)), Ok(row(2)), Ok(row(1)), Ok(row(3))].into_iter();
        let right = vec![Ok(row(1)), Ok(row(1)), Ok(row(2))].into_iter();

        let base = VecStore::new();
        let mut spill = TempSpillFile::new(base).unwrap();
        let total = spillable_hash_join_match_count_i64(
            left,
            right,
            &fp("k"),
            &fp("k"),
            1,
            Some(&mut spill),
        )
        .unwrap();

        // left counts: 1->2,2->1,3->1 ; right: 1->2,2->1 => 2*2 + 1*1 = 5
        assert_eq!(total, 5);
    }
}
