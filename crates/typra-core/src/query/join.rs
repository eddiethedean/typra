use std::collections::HashMap;

use crate::db::scalar_at_path;
use crate::error::{DbError, QueryError};
use crate::record::RowValue;
use crate::schema::FieldPath;
use crate::spill::TempSpillFile;
use crate::storage::FileStore;
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

fn flush_counts_to_spill(
    counts: &mut HashMap<i64, u64>,
    spill: &mut TempSpillFile<FileStore>,
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
pub fn spillable_hash_join_match_count_i64<I1, I2>(
    left_rows: I1,
    right_rows: I2,
    left_on: &FieldPath,
    right_on: &FieldPath,
    max_keys_in_mem: usize,
    mut spill: Option<&mut TempSpillFile<FileStore>>,
) -> Result<u64, DbError>
where
    I1: Iterator<Item = Result<std::collections::BTreeMap<String, RowValue>, DbError>>,
    I2: Iterator<Item = Result<std::collections::BTreeMap<String, RowValue>, DbError>>,
{
    match max_keys_in_mem {
        0 => return Err(qerr("max_keys_in_mem must be > 0")),
        _ => {}
    }

    // Right side key multiplicities (v0: materialized).
    let mut right_counts: HashMap<i64, u64> = HashMap::new();
    for r in right_rows {
        let r = r?;
        let k = match scalar_at_path(&r, right_on) {
            Some(ScalarValue::Int64(k)) => k,
            _ => continue,
        };
        *right_counts.entry(k).or_insert(0) += 1;
    }

    let mut left_counts: HashMap<i64, u64> = HashMap::new();
    let mut segs: Vec<SpillSeg> = Vec::new();

    for r in left_rows {
        let r = r?;
        let k = match scalar_at_path(&r, left_on) {
            Some(ScalarValue::Int64(k)) => k,
            _ => continue,
        };
        *left_counts.entry(k).or_insert(0) += 1;
        match left_counts.len().cmp(&max_keys_in_mem) {
            std::cmp::Ordering::Greater => {
                let spill_ref = spill.as_deref_mut().ok_or_else(|| {
                    qerr("join exceeded memory budget but no spill store was provided")
                })?;
                flush_counts_to_spill(&mut left_counts, spill_ref, &mut segs)?;
            }
            _ => {}
        }
    }

    match spill.as_deref_mut() {
        Some(spill_ref) => flush_counts_to_spill(&mut left_counts, spill_ref, &mut segs)?,
        None => {}
    }

    // No spill path.
    match segs.is_empty() {
        true => {
            let mut total = 0u64;
            for (k, lc) in left_counts {
                match right_counts.get(&k) {
                    Some(rc) => total = total.wrapping_add(lc.wrapping_mul(*rc)),
                    None => {}
                }
            }
            return Ok(total);
        }
        false => {}
    }

    let spill = spill.expect("spill segments exist but spill store missing");

    // Merge each partition and compute matches.
    let mut by_part: [Vec<SpillSeg>; 64] = std::array::from_fn(|_| Vec::new());
    for s in segs {
        by_part[s.partition as usize].push(s);
    }

    let mut total = 0u64;
    for segs in by_part {
        match segs.is_empty() {
            true => continue,
            false => {}
        }
        let mut part_counts: HashMap<i64, u64> = HashMap::new();
        for s in segs {
            let buf = spill.read_temp_payload(s.offset, s.payload_len)?;
            for (k, c) in decode_entries(&buf)? {
                *part_counts.entry(k).or_insert(0) += c;
            }
        }
        for (k, lc) in part_counts {
            match right_counts.get(&k) {
                Some(rc) => total = total.wrapping_add(lc.wrapping_mul(*rc)),
                None => {}
            }
        }
    }

    Ok(total)
}

#[cfg(test)]
mod tests {
    use std::borrow::Cow;
    use super::*;
    use std::collections::BTreeMap;

    fn fp(parts: &[&str]) -> FieldPath {
        FieldPath(parts.iter().map(|s| Cow::Owned(s.to_string())).collect())
    }

    fn row_i64(k: &str, v: i64) -> BTreeMap<String, RowValue> {
        let mut m = BTreeMap::new();
        m.insert(k.to_string(), RowValue::Int64(v));
        m
    }

    #[test]
    fn decode_entries_rejects_truncated_buffers() {
        assert!(decode_entries(&[]).is_err());
        assert!(decode_entries(&[0, 0, 0]).is_err());
        let mut buf = Vec::new();
        buf.extend_from_slice(&1u32.to_le_bytes());
        buf.extend_from_slice(&0i64.to_le_bytes());
        assert!(decode_entries(&buf).is_err());
    }

    #[test]
    fn decode_entries_accepts_valid_buffer() {
        let entries = vec![(9i64, 7u64), (-1i64, 3u64)];
        let buf = encode_entries(&entries);
        let got = decode_entries(&buf).unwrap();
        assert_eq!(got, entries);
    }

    #[test]
    fn flush_counts_to_spill_noop_when_empty() {
        use crate::storage::FileStore;

        let mut counts: HashMap<i64, u64> = HashMap::new();
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("spill.typra");
        std::fs::write(&path, []).unwrap();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(&path)
            .unwrap();
        let mut spill = TempSpillFile::new(FileStore::new(file)).unwrap();
        let mut segs = Vec::new();
        flush_counts_to_spill(&mut counts, &mut spill, &mut segs).unwrap();
        assert!(segs.is_empty());
    }

    #[test]
    fn flush_counts_to_spill_writes_segments_and_exercises_empty_partitions() {
        use crate::storage::FileStore;

        let mut counts: HashMap<i64, u64> = HashMap::new();
        counts.insert(1, 2);
        counts.insert(2, 3);
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("spill.typra");
        std::fs::write(&path, []).unwrap();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(&path)
            .unwrap();
        let mut spill = TempSpillFile::new(FileStore::new(file)).unwrap();
        let mut segs = Vec::new();
        flush_counts_to_spill(&mut counts, &mut spill, &mut segs).unwrap();
        assert!(!segs.is_empty());
    }

    #[test]
    fn spillable_join_rejects_zero_budget() {
        let left = std::iter::empty::<Result<BTreeMap<String, RowValue>, DbError>>();
        let right = std::iter::empty::<Result<BTreeMap<String, RowValue>, DbError>>();
        assert!(
            spillable_hash_join_match_count_i64(left, right, &fp(&["k"]), &fp(&["k"]), 0, None)
                .is_err()
        );
    }

    #[test]
    fn spillable_join_errors_when_exceeds_budget_without_spill() {
        let right = vec![Ok(row_i64("k", 1))].into_iter();
        let left = vec![Ok(row_i64("k", 1)), Ok(row_i64("k", 2))].into_iter();
        assert!(
            spillable_hash_join_match_count_i64(left, right, &fp(&["k"]), &fp(&["k"]), 1, None)
                .is_err()
        );
    }

    #[test]
    fn spillable_join_no_spill_path_counts_matches() {
        let right = vec![
            Ok(row_i64("k", 1)),
            Ok(row_i64("k", 1)),
            Ok(row_i64("k", 2)),
        ]
        .into_iter();
        let left = vec![Ok(row_i64("k", 1)), Ok(row_i64("k", 2)), Ok(row_i64("k", 2))]
            .into_iter();
        let total =
            spillable_hash_join_match_count_i64(left, right, &fp(&["k"]), &fp(&["k"]), 10, None)
                .unwrap();
        // left counts: 1->1,2->2; right counts: 1->2,2->1 => matches = 1*2 + 2*1 = 4
        assert_eq!(total, 4);
    }

    #[test]
    fn spillable_join_no_spill_path_skips_missing_right_key_arm() {
        // Coverage-motivated: ensure `right_counts.get(&k) == None` is exercised in the no-spill
        // merge loop.
        let right = vec![Ok(row_i64("k", 1))].into_iter();
        let left = vec![Ok(row_i64("k", 1)), Ok(row_i64("k", 999))].into_iter();
        let total =
            spillable_hash_join_match_count_i64(left, right, &fp(&["k"]), &fp(&["k"]), 10, None)
                .unwrap();
        // Only the shared key contributes.
        assert_eq!(total, 1);
    }

    #[test]
    fn spillable_join_spill_merge_skips_missing_right_key_arm() {
        // Coverage-motivated: ensure `right_counts.get(&k) == None` is exercised in the spill
        // merge loop (per-partition reduce).
        use crate::storage::FileStore;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("spill.typra");
        std::fs::write(&path, []).unwrap();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(&path)
            .unwrap();
        let mut spill = TempSpillFile::new(FileStore::new(file)).unwrap();

        // Right side has no usable join keys => empty right_counts.
        let right = std::iter::empty::<Result<BTreeMap<String, RowValue>, DbError>>();
        // Left side has >budget unique keys => forces spill segments.
        let left = vec![Ok(row_i64("k", 1)), Ok(row_i64("k", 2))].into_iter();

        let total = spillable_hash_join_match_count_i64(
            left,
            right,
            &fp(&["k"]),
            &fp(&["k"]),
            1, // force spill
            Some(&mut spill),
        )
        .unwrap();
        assert_eq!(total, 0);
    }

    #[test]
    fn spillable_join_spills_and_merges_with_file_store_and_skips_missing_keys() {
        use crate::storage::FileStore;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("spill.typra");
        std::fs::write(&path, []).unwrap();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(&path)
            .unwrap();
        let mut spill = TempSpillFile::new(FileStore::new(file)).unwrap();

        let right = (0..50i64).map(|i| {
            if i == 0 {
                // wrong type => skipped
                let mut r = BTreeMap::new();
                r.insert("k".to_string(), RowValue::String("nope".to_string()));
                Ok(r)
            } else {
                Ok(row_i64("k", i % 7))
            }
        });
        let left = (0..200i64).map(|i| {
            if i == 0 {
                // missing key => skipped
                Ok(BTreeMap::new())
            } else {
                Ok(row_i64("k", i % 7))
            }
        });

        let total = spillable_hash_join_match_count_i64(
            left,
            right,
            &fp(&["k"]),
            &fp(&["k"]),
            1, // force spill
            Some(&mut spill),
        )
        .unwrap();
        assert!(total > 0);
    }

    #[test]
    fn spillable_join_with_spill_store_but_no_spill_occurs() {
        use crate::storage::FileStore;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("spill.typra");
        std::fs::write(&path, []).unwrap();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(&path)
            .unwrap();
        let mut spill = TempSpillFile::new(FileStore::new(file)).unwrap();

        let right = vec![Ok(row_i64("k", 1))].into_iter();
        let left = vec![Ok(row_i64("k", 1)), Ok(row_i64("k", 1))].into_iter();
        let total = spillable_hash_join_match_count_i64(
            left,
            right,
            &fp(&["k"]),
            &fp(&["k"]),
            100,
            Some(&mut spill),
        )
        .unwrap();
        assert_eq!(total, 2);
    }

    #[test]
    fn spillable_join_two_keys_forces_spill_path() {
        use crate::storage::FileStore;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("spill.typra");
        std::fs::write(&path, []).unwrap();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(&path)
            .unwrap();
        let mut spill = TempSpillFile::new(FileStore::new(file)).unwrap();

        // Right counts: k=1 twice, k=2 once
        let right = vec![Ok(row_i64("k", 1)), Ok(row_i64("k", 1)), Ok(row_i64("k", 2))].into_iter();
        // Left counts exceed budget 1: k=1 then k=2 triggers spill
        let left = vec![Ok(row_i64("k", 1)), Ok(row_i64("k", 2))].into_iter();

        let total = spillable_hash_join_match_count_i64(
            left,
            right,
            &fp(&["k"]),
            &fp(&["k"]),
            1,
            Some(&mut spill),
        )
        .unwrap();
        // matches: left(1)=1 * right(1)=2 + left(2)=1 * right(2)=1 => 3
        assert_eq!(total, 3);
    }

    #[test]
    fn spillable_join_spill_merge_has_empty_partitions() {
        use crate::storage::FileStore;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("spill.typra");
        std::fs::write(&path, []).unwrap();
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(false)
            .open(&path)
            .unwrap();
        let mut spill = TempSpillFile::new(FileStore::new(file)).unwrap();

        // Use keys that all hash to the same partition (k % 64 == 0),
        // so the merge loop definitely sees many empty partitions.
        let right = vec![Ok(row_i64("k", 0)), Ok(row_i64("k", 64))].into_iter();
        let left = vec![Ok(row_i64("k", 0)), Ok(row_i64("k", 64))].into_iter();

        let total = spillable_hash_join_match_count_i64(
            left,
            right,
            &fp(&["k"]),
            &fp(&["k"]),
            1, // force spill
            Some(&mut spill),
        )
        .unwrap();
        assert_eq!(total, 2);
    }

    #[test]
    fn spillable_join_len_equal_to_budget_does_not_spill() {
        // Specifically exercise the `left_counts.len() > max_keys_in_mem` false path when
        // `left_counts.len() == max_keys_in_mem`.
        let right = vec![Ok(row_i64("k", 1))].into_iter();
        let left = vec![Ok(row_i64("k", 1)), Ok(row_i64("k", 1)), Ok(row_i64("k", 1))].into_iter();

        let total =
            spillable_hash_join_match_count_i64(left, right, &fp(&["k"]), &fp(&["k"]), 1, None)
                .unwrap();
        assert_eq!(total, 3);
    }
}
