use std::collections::BTreeMap;

use crate::record::RowValue;

fn object_map_mut(v: &mut RowValue) -> &mut BTreeMap<String, RowValue> {
    if !matches!(v, RowValue::Object(_)) {
        *v = RowValue::Object(BTreeMap::new());
    }
    let RowValue::Object(m) = v else {
        unreachable!("merge_non_pk_into_full_map: value was coerced to Object");
    };
    m
}

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
        cur = object_map_mut(cur)
            .entry(seg.clone())
            .or_insert_with(|| RowValue::Object(BTreeMap::new()));
    }
    if let RowValue::Object(m) = cur {
        m.insert(parts.last().unwrap().clone(), v.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::merge_non_pk_into_full_map;
    use crate::record::RowValue;
    use std::collections::BTreeMap;

    #[test]
    fn merge_turns_scalar_prefix_into_object_for_deeper_path() {
        let mut m = BTreeMap::new();
        m.insert("a".into(), RowValue::Int64(7));
        merge_non_pk_into_full_map(
            &mut m,
            &["a".into(), "b".into(), "c".into()],
            &RowValue::String("leaf".into()),
        );
        let inner = m.get("a").unwrap().as_object_map().unwrap();
        let mid = inner.get("b").unwrap().as_object_map().unwrap();
        assert_eq!(mid.get("c"), Some(&RowValue::String("leaf".into())));
    }

    #[test]
    fn merge_three_segments_overwrites_leaf() {
        let mut m = BTreeMap::new();
        merge_non_pk_into_full_map(
            &mut m,
            &["x".into(), "y".into(), "z".into()],
            &RowValue::Bool(true),
        );
        merge_non_pk_into_full_map(
            &mut m,
            &["x".into(), "y".into(), "z".into()],
            &RowValue::Bool(false),
        );
        let x = m.get("x").unwrap().as_object_map().unwrap();
        let y = x.get("y").unwrap().as_object_map().unwrap();
        assert_eq!(y.get("z"), Some(&RowValue::Bool(false)));
    }
}

