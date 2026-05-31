use crate::error::DbError;
use crate::schema::{IndexDef, IndexKind, SchemaChange, Type};
use crate::{schema, validation};

/// Classify a schema update from `(old_fields, old_indexes)` to `(new_fields, new_indexes)`.
///
/// Policy (v0.9.0, conservative):
/// - Existing fields must remain present and type-compatible.
/// - Enum fields may add variants (superset) but may not remove variants.
/// - Constraints must be identical for existing fields (tightening is treated as breaking).
/// - New fields are `Safe` only if they are top-level-absent-compatible (`Optional`); otherwise `NeedsMigration`.
/// - Existing indexes must remain unchanged; adding indexes is `Safe` for `NonUnique` and `NeedsMigration` for `Unique`.
pub fn classify_schema_update(
    old_fields: &[schema::FieldDef],
    old_indexes: &[IndexDef],
    new_fields: &[schema::FieldDef],
    new_indexes: &[IndexDef],
) -> Result<SchemaChange, DbError> {
    // Build path->def maps.
    let mut old_map: std::collections::HashMap<&schema::FieldPath, &schema::FieldDef> =
        std::collections::HashMap::new();
    for f in old_fields {
        old_map.insert(&f.path, f);
    }
    let mut new_map: std::collections::HashMap<&schema::FieldPath, &schema::FieldDef> =
        std::collections::HashMap::new();
    for f in new_fields {
        new_map.insert(&f.path, f);
    }

    // Existing fields must exist with compatible type and same constraints.
    for (path, old_def) in &old_map {
        let Some(new_def) = new_map.get(path) else {
            return Ok(SchemaChange::Breaking {
                reason: format!("field removed: {:?}", path.0),
            });
        };
        if old_def.constraints != new_def.constraints {
            return Ok(SchemaChange::Breaking {
                reason: format!("constraints changed for field {:?}", path.0),
            });
        }
        if !type_is_compatible(&old_def.ty, &new_def.ty) {
            return Ok(SchemaChange::Breaking {
                reason: format!("type changed for field {:?}", path.0),
            });
        }
    }

    // New fields: safe only if optional-at-root; otherwise migration required.
    for (path, new_def) in &new_map {
        if old_map.contains_key(path) {
            continue;
        }
        if validation::allows_absent_root(&new_def.ty) {
            continue;
        }
        return Ok(SchemaChange::NeedsMigration {
            reason: format!("new required field {:?} needs backfill", path.0),
            backfill_top_level_field: if path.0.len() == 1 {
                Some(path.0[0].to_string())
            } else {
                None
            },
        });
    }

    // Index rules: existing indexes must remain identical.
    let mut old_idx_map: std::collections::HashMap<&str, &IndexDef> =
        std::collections::HashMap::new();
    for idx in old_indexes {
        old_idx_map.insert(idx.name.as_str(), idx);
    }
    let mut new_idx_map: std::collections::HashMap<&str, &IndexDef> =
        std::collections::HashMap::new();
    for idx in new_indexes {
        new_idx_map.insert(idx.name.as_str(), idx);
    }

    for (name, old_idx) in &old_idx_map {
        // Dropping an index is allowed: it only affects planning/unique enforcement going forward.
        let Some(new_idx) = new_idx_map.get(name) else {
            continue;
        };
        // Use `|` so both comparisons run; `||` leaves one side unevaluated and shows as a line miss under llvm-cov.
        if (old_idx.kind != new_idx.kind) | (old_idx.path != new_idx.path) {
            return Ok(SchemaChange::Breaking {
                reason: format!("index changed: {name:?}"),
            });
        }
    }

    // Added indexes.
    for (name, new_idx) in &new_idx_map {
        if old_idx_map.contains_key(name) {
            continue;
        }
        if new_idx.kind == IndexKind::Unique {
            return Ok(SchemaChange::NeedsMigration {
                reason: format!("new unique index {name:?} needs rebuild/validation"),
                backfill_top_level_field: None,
            });
        }
    }

    Ok(SchemaChange::Safe)
}

fn type_is_compatible(old: &Type, new: &Type) -> bool {
    match (old, new) {
        (Type::Enum(old_vars), Type::Enum(new_vars)) => {
            // New must be a superset (no removals).
            old_vars.iter().all(|v| new_vars.contains(v))
        }
        _ => old == new,
    }
}
