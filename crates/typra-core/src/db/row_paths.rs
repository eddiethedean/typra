use std::collections::{BTreeMap, HashSet};

use crate::error::{DbError, SchemaError};
use crate::record::RowValue;
use crate::schema::{FieldDef, FieldPath};

pub(crate) fn validate_unknown_fields_for_multiseg_schema(
    fields: &[FieldDef],
    pk_name: &str,
    row: &BTreeMap<String, RowValue>,
) -> Result<(), DbError> {
    fn walk_row(out: &mut Vec<Vec<String>>, prefix: &mut Vec<String>, v: &RowValue) {
        match v {
            RowValue::Object(map) => {
                for (k, child) in map {
                    prefix.push(k.clone());
                    walk_row(out, prefix, child);
                    prefix.pop();
                }
            }
            // Lists/enums/scalars/None are treated as leaves at this path.
            _ => out.push(prefix.clone()),
        }
    }

    let mut leaf_paths: Vec<Vec<String>> = Vec::new();
    for (k, v) in row {
        if k == pk_name {
            continue;
        }
        let mut prefix = vec![k.clone()];
        walk_row(&mut leaf_paths, &mut prefix, v);
    }

    // Allowed leaf paths are exactly the schema field defs (excluding PK).
    let mut allowed: HashSet<Vec<String>> = HashSet::new();
    for f in fields {
        if f.path.0.len() == 1 && f.path.0[0] == pk_name {
            continue;
        }
        allowed.insert(field_path_to_vec(&f.path));
    }

    for p in leaf_paths {
        if !allowed.contains(&p) {
            return Err(DbError::Schema(SchemaError::RowUnknownField { name: p.join(".") }));
        }
    }
    Ok(())
}

fn field_path_to_vec(fp: &FieldPath) -> Vec<String> {
    fp.0.iter().map(|s| s.as_ref().to_string()).collect()
}

