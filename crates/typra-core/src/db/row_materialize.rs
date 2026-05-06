use std::collections::BTreeMap;

use crate::error::{DbError, SchemaError};
use crate::record::RowValue;
use crate::schema::FieldDef;
use crate::validation;

pub(crate) fn row_value_at_path(
    row: &BTreeMap<String, RowValue>,
    path: &[std::borrow::Cow<'static, str>],
) -> Option<RowValue> {
    let mut cur = row.get(
        path.first()
            .expect("catalog field paths are validated as non-empty")
            .as_ref(),
    )?;
    for seg in path.iter().skip(1) {
        cur = cur.as_object_map()?.get(seg.as_ref())?;
    }
    Some(cur.clone())
}

pub(crate) fn build_non_pk_values_in_schema_order(
    row: &BTreeMap<String, RowValue>,
    non_pk_defs: &[&FieldDef],
) -> Result<Vec<(FieldDef, RowValue)>, DbError> {
    let mut non_pk: Vec<(FieldDef, RowValue)> = Vec::with_capacity(non_pk_defs.len());
    for def in non_pk_defs {
        let v = match row_value_at_path(row, &def.path.0) {
            Some(x) => x,
            None if validation::allows_absent_root(&def.ty) => RowValue::None,
            None => {
                return Err(DbError::Schema(SchemaError::RowMissingField {
                    name: def
                        .path
                        .0
                        .iter()
                        .map(|s| s.as_ref())
                        .collect::<Vec<_>>()
                        .join("."),
                }));
            }
        };
        non_pk.push(((*def).clone(), v));
    }
    Ok(non_pk)
}

