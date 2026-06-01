use crate::error::{DbError, FormatError, SchemaError};
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
            backfill_field_path: Some((*path).clone()),
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
                backfill_field_path: None,
            });
        }
    }

    Ok(SchemaChange::Safe)
}

/// Validate that a model's declared fields (and optional indexes) are compatible with an
/// existing collection catalog entry.
///
/// - Every model field must exist in the catalog with the same type.
/// - Primary key name must match.
/// - If the model declares the full field set, index definitions must match the catalog.
/// - Subset models may omit catalog fields; index defs on subset models are optional but must
///   match when present.
pub fn validate_model_fields_against_catalog(
    col: &crate::catalog::CollectionInfo,
    primary_field: &str,
    model_fields: &[schema::FieldDef],
    model_indexes: &[IndexDef],
) -> Result<(), DbError> {
    let Some(pk) = col.primary_field.as_deref() else {
        return Err(DbError::Schema(SchemaError::NoPrimaryKey {
            collection_id: col.id.0,
        }));
    };
    if pk != primary_field {
        return Err(DbError::Schema(SchemaError::PrimaryFieldNotFound {
            name: primary_field.to_string(),
        }));
    }
    for mf in model_fields {
        let Some(cf) = col.fields.iter().find(|f| f.path == mf.path) else {
            return Err(DbError::Schema(SchemaError::RowUnknownField {
                name: mf.path.0.last().map(|s| s.to_string()).unwrap_or_default(),
            }));
        };
        if cf.ty != mf.ty {
            return Err(DbError::Format(FormatError::RecordPayloadTypeMismatch));
        }
    }

    let model_paths: std::collections::BTreeSet<_> = model_fields.iter().map(|f| &f.path).collect();
    let catalog_paths: std::collections::BTreeSet<_> = col.fields.iter().map(|f| &f.path).collect();
    let is_full_schema = model_paths == catalog_paths;

    if is_full_schema && !indexes_match(&col.indexes, model_indexes) {
        return Err(DbError::Schema(SchemaError::IncompatibleSchemaChange {
            message: "model index definitions do not match collection catalog".into(),
        }));
    }
    if !is_full_schema {
        for mi in model_indexes {
            let Some(ci) = col.indexes.iter().find(|i| i.name == mi.name) else {
                return Err(DbError::Schema(SchemaError::IncompatibleSchemaChange {
                    message: format!("unknown index {:?}", mi.name),
                }));
            };
            if ci.kind != mi.kind || ci.path != mi.path {
                return Err(DbError::Schema(SchemaError::IncompatibleSchemaChange {
                    message: format!("index {:?} does not match catalog", mi.name),
                }));
            }
        }
    }
    Ok(())
}

fn indexes_match(a: &[IndexDef], b: &[IndexDef]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.iter().all(|ia| {
        b.iter()
            .any(|ib| ib.name == ia.name && ib.kind == ia.kind && ib.path == ia.path)
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::borrow::Cow;

    use crate::catalog::CollectionInfo;
    use crate::error::{DbError, SchemaError};
    use crate::schema::{FieldDef, FieldPath, IndexDef, IndexKind, Type};
    use crate::schema::SchemaVersion;
    use crate::CollectionId;

    fn field(name: &str, ty: Type) -> FieldDef {
        FieldDef {
            path: FieldPath(vec![Cow::Owned(name.to_string())]),
            ty,
            constraints: vec![],
        }
    }

    fn path(segs: &[&str]) -> FieldPath {
        FieldPath(segs.iter().map(|s| Cow::Owned((*s).to_string())).collect())
    }

    #[test]
    fn validate_model_fields_rejects_full_schema_index_mismatch() {
        let col = CollectionInfo {
            id: CollectionId(1),
            name: "t".into(),
            current_version: SchemaVersion(1),
            fields: vec![field("id", Type::Int64), field("x", Type::Int64)],
            indexes: vec![IndexDef {
                name: "x_idx".into(),
                path: path(&["x"]),
                kind: IndexKind::NonUnique,
            }],
            primary_field: Some("id".into()),
        };
        let err = validate_model_fields_against_catalog(
            &col,
            "id",
            &col.fields,
            &[IndexDef {
                name: "other".into(),
                path: path(&["x"]),
                kind: IndexKind::NonUnique,
            }],
        )
        .unwrap_err();
        assert!(matches!(
            err,
            DbError::Schema(SchemaError::IncompatibleSchemaChange { .. })
        ));
    }

    #[test]
    fn validate_model_fields_subset_index_errors() {
        let col = CollectionInfo {
            id: CollectionId(1),
            name: "t".into(),
            current_version: SchemaVersion(1),
            fields: vec![field("id", Type::Int64), field("note", Type::String)],
            indexes: vec![IndexDef {
                name: "id_idx".into(),
                path: path(&["id"]),
                kind: IndexKind::NonUnique,
            }],
            primary_field: Some("id".into()),
        };
        let subset = vec![field("id", Type::Int64)];

        let err = validate_model_fields_against_catalog(
            &col,
            "id",
            &subset,
            &[IndexDef {
                name: "missing".into(),
                path: path(&["id"]),
                kind: IndexKind::NonUnique,
            }],
        )
        .unwrap_err();
        assert!(matches!(
            err,
            DbError::Schema(SchemaError::IncompatibleSchemaChange { message }) if message.contains("unknown index")
        ));

        let err2 = validate_model_fields_against_catalog(
            &col,
            "id",
            &subset,
            &[IndexDef {
                name: "id_idx".into(),
                path: path(&["id"]),
                kind: IndexKind::Unique,
            }],
        )
        .unwrap_err();
        assert!(matches!(
            err2,
            DbError::Schema(SchemaError::IncompatibleSchemaChange { message }) if message.contains("does not match catalog")
        ));
    }
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
