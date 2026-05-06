use std::collections::BTreeMap;

use crate::record::RowValue;

pub(crate) fn merge_non_pk_into_full_map(
    full_map: &mut BTreeMap<String, RowValue>,
    parts: &[String],
    v: &RowValue,
) {
    // Build nested objects for multi-segment field paths.
    let mut cur: &mut RowValue = full_map
        .entry(parts[0].clone())
        .or_insert_with(|| RowValue::Object(BTreeMap::new()));
    for seg in parts.iter().skip(1).take(parts.len().saturating_sub(2)) {
        if !matches!(cur, RowValue::Object(_)) {
            *cur = RowValue::Object(BTreeMap::new());
        }
        if let RowValue::Object(m) = cur {
            cur = m
                .entry(seg.clone())
                .or_insert_with(|| RowValue::Object(BTreeMap::new()));
        }
    }
    if let RowValue::Object(m) = cur {
        m.insert(parts.last().unwrap().clone(), v.clone());
    }
}

