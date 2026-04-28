//! Collection identity, field paths, logical [`Type`] values, and the [`DbModel`] marker trait.

use std::borrow::Cow;

use crate::error::{DbError, SchemaError};
use crate::validation;

/// Stable numeric id for a registered collection (assigned at create time, starting at `1`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct CollectionId(pub u32);

/// Monotonic schema version for one collection (starts at `1` on create; bumps on each new version).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SchemaVersion(pub u32);

/// Dot-style path segments for a field (v1 rows use single-segment top-level names only).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct FieldPath(pub Vec<Cow<'static, str>>);

impl FieldPath {
    /// Build a path from non-empty UTF-8 segments (rejects empty paths or empty segments).
    pub fn new(parts: impl IntoIterator<Item = Cow<'static, str>>) -> Result<Self, DbError> {
        let parts: Vec<Cow<'static, str>> = parts.into_iter().collect();
        if parts.is_empty() || parts.iter().any(|p| p.is_empty()) {
            return Err(DbError::Schema(SchemaError::InvalidFieldPath));
        }
        Ok(Self(parts))
    }
}

pub(crate) fn validate_field_defs(fields: &[FieldDef]) -> Result<(), DbError> {
    // Basic path validation (in case callers constructed `FieldPath` directly).
    for f in fields {
        if f.path.0.is_empty() || f.path.0.iter().any(|s| s.is_empty()) {
            return Err(DbError::Schema(SchemaError::InvalidFieldPath));
        }
    }

    // Duplicates.
    let mut seen: std::collections::HashSet<&FieldPath> = std::collections::HashSet::new();
    for f in fields {
        if !seen.insert(&f.path) {
            return Err(DbError::Schema(SchemaError::InvalidFieldPath));
        }
    }

    // Parent/child conflicts (e.g. `a` and `a.b`).
    for (i, a) in fields.iter().enumerate() {
        for b in fields.iter().skip(i + 1) {
            let pa = &a.path.0;
            let pb = &b.path.0;
            let min = pa.len().min(pb.len());
            if pa.len() != pb.len() && pa[..min] == pb[..min] {
                return Err(DbError::Schema(SchemaError::InvalidFieldPath));
            }
        }
    }

    Ok(())
}

/// Logical type of a field in the catalog (mirrors encoding in record payloads where supported).
#[derive(Debug, Clone, PartialEq)]
pub enum Type {
    /// Boolean.
    Bool,
    /// Signed 64-bit integer.
    Int64,
    /// Unsigned 64-bit integer.
    Uint64,
    /// IEEE-754 double.
    Float64,
    /// UTF-8 string.
    String,
    /// Raw bytes.
    Bytes,
    /// 16-byte UUID (canonical record encoding uses tagged bytes).
    Uuid,
    /// Signed epoch milliseconds (or engine-defined timestamp unit).
    Timestamp,
    /// Value may be absent (`None`).
    Optional(Box<Type>),
    /// Homogeneous list.
    List(Box<Type>),
    /// Fixed set of nested fields (struct-like).
    Object(Vec<FieldDef>),
    /// Tagged union of string variants.
    Enum(Vec<String>),
}

/// Declarative constraint on a field (0.6+). Evaluated on insert after type checks.
#[derive(Debug, Clone, PartialEq)]
pub enum Constraint {
    /// Minimum inclusive for signed integers (`Int64`).
    MinI64(i64),
    /// Maximum inclusive for signed integers (`Int64`).
    MaxI64(i64),
    /// Minimum inclusive for unsigned integers (`Uint64`).
    MinU64(u64),
    /// Maximum inclusive for unsigned integers (`Uint64`).
    MaxU64(u64),
    /// Minimum inclusive for floats (`Float64`).
    MinF64(f64),
    /// Maximum inclusive for floats (`Float64`).
    MaxF64(f64),
    /// Minimum UTF-8 byte length (`String`) or element count (`List`).
    MinLength(u64),
    /// Maximum UTF-8 byte length (`String`) or element count (`List`).
    MaxLength(u64),
    /// Rust regex syntax (applied to `String`).
    Regex(String),
    /// Loose email shape check (`String`).
    Email,
    /// `http`/`https` URL prefix check (`String`).
    Url,
    /// Non-empty string, bytes, or list.
    NonEmpty,
}

/// One field’s path, type, and optional constraints within a collection schema.
#[derive(Debug, Clone, PartialEq)]
pub struct FieldDef {
    pub path: FieldPath,
    pub ty: Type,
    pub constraints: Vec<Constraint>,
}

impl FieldDef {
    pub fn new(path: FieldPath, ty: Type) -> Self {
        Self {
            path,
            ty,
            constraints: Vec::new(),
        }
    }
}

/// Kind of secondary index.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexKind {
    /// Enforces a uniqueness constraint: one primary key per indexed value.
    Unique,
    /// Non-unique index: many primary keys per indexed value.
    NonUnique,
}

/// Secondary index definition for one collection schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexDef {
    /// Stable identifier within a collection schema (e.g. `"email_unique"`).
    pub name: String,
    /// Field path whose scalar value is indexed (may be nested, e.g. `["profile","timezone"]`).
    pub path: FieldPath,
    pub kind: IndexKind,
}

/// High-level description of a collection (name, version, fields); used by tooling and derives.
#[derive(Debug, Clone, PartialEq)]
pub struct CollectionSchema {
    pub name: String,
    pub version: SchemaVersion,
    pub fields: Vec<FieldDef>,
    pub id: Option<CollectionId>,
}

/// Marker trait for Rust types that map to Typra collection records.
///
/// Implement via `#[derive(DbModel)]` from the optional `typra-derive` crate (re-exported by the
/// `typra` facade when the **`derive`** feature is enabled).
pub trait DbModel {
    fn collection_name() -> &'static str;
    fn fields() -> Vec<FieldDef>;
    fn primary_field() -> &'static str;
    fn indexes() -> Vec<IndexDef> {
        Vec::new()
    }
}

/// Compatibility classification for a proposed schema update.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaChange {
    /// Update is safe to apply without rewriting existing data.
    Safe,
    /// Update is supported, but existing data must be rewritten/backfilled first.
    NeedsMigration { reason: String },
    /// Update is not supported/safe and should be rejected by default.
    Breaking { reason: String },
}

/// Classify a schema update from `(old_fields, old_indexes)` to `(new_fields, new_indexes)`.
///
/// Policy (v0.9.0, conservative):
/// - Existing fields must remain present and type-compatible.
/// - Enum fields may add variants (superset) but may not remove variants.
/// - Constraints must be identical for existing fields (tightening is treated as breaking).
/// - New fields are `Safe` only if they are top-level-absent-compatible (`Optional`); otherwise `NeedsMigration`.
/// - Existing indexes must remain unchanged; adding indexes is `Safe` for `NonUnique` and `NeedsMigration` for `Unique`.
pub fn classify_schema_update(
    old_fields: &[FieldDef],
    old_indexes: &[IndexDef],
    new_fields: &[FieldDef],
    new_indexes: &[IndexDef],
) -> Result<SchemaChange, DbError> {
    // Build path->def maps.
    let mut old_map: std::collections::HashMap<&FieldPath, &FieldDef> =
        std::collections::HashMap::new();
    for f in old_fields {
        old_map.insert(&f.path, f);
    }
    let mut new_map: std::collections::HashMap<&FieldPath, &FieldDef> =
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
        if old_idx.kind != new_idx.kind || old_idx.path != new_idx.path {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::borrow::Cow;

    fn fp(parts: &[&'static str]) -> FieldPath {
        FieldPath::new(parts.iter().copied().map(Cow::Borrowed)).unwrap()
    }

    #[test]
    fn validate_field_defs_rejects_empty_duplicate_and_parent_child_conflict() {
        // Empty path via direct construction (bypasses FieldPath::new).
        let bad = FieldDef::new(FieldPath(vec![]), Type::Int64);
        assert!(matches!(
            validate_field_defs(&[bad]),
            Err(DbError::Schema(SchemaError::InvalidFieldPath))
        ));

        // Duplicate.
        let a1 = FieldDef::new(fp(&["a"]), Type::Int64);
        let a2 = FieldDef::new(fp(&["a"]), Type::Int64);
        assert!(matches!(
            validate_field_defs(&[a1, a2]),
            Err(DbError::Schema(SchemaError::InvalidFieldPath))
        ));

        // Parent/child conflict.
        let p = FieldDef::new(fp(&["a"]), Type::Int64);
        let c = FieldDef::new(fp(&["a", "b"]), Type::Int64);
        assert!(matches!(
            validate_field_defs(&[p, c]),
            Err(DbError::Schema(SchemaError::InvalidFieldPath))
        ));
    }

    #[test]
    fn classify_schema_update_hits_breaking_and_migration_paths() {
        // Constraints changed => Breaking.
        let mut old = FieldDef::new(fp(&["x"]), Type::Int64);
        old.constraints = vec![Constraint::MinI64(0)];
        let old_fields = vec![old.clone()];
        let old_indexes: Vec<IndexDef> = vec![];

        let mut new = FieldDef::new(fp(&["x"]), Type::Int64);
        new.constraints = vec![Constraint::MinI64(1)];
        let ch = classify_schema_update(&old_fields, &old_indexes, &[new], &[]).unwrap();
        assert!(matches!(ch, SchemaChange::Breaking { .. }));

        // Type changed => Breaking.
        let ch = classify_schema_update(
            &[FieldDef::new(fp(&["x"]), Type::Int64)],
            &[],
            &[FieldDef::new(fp(&["x"]), Type::Uint64)],
            &[],
        )
        .unwrap();
        assert!(matches!(ch, SchemaChange::Breaking { .. }));

        // New required field (non-optional) => NeedsMigration.
        let ch = classify_schema_update(
            &[FieldDef::new(fp(&["x"]), Type::Int64)],
            &[],
            &[
                FieldDef::new(fp(&["x"]), Type::Int64),
                FieldDef::new(fp(&["y"]), Type::Int64),
            ],
            &[],
        )
        .unwrap();
        assert!(matches!(ch, SchemaChange::NeedsMigration { .. }));

        // Index changed => Breaking.
        let old_idx = IndexDef {
            name: "i".to_string(),
            path: fp(&["x"]),
            kind: IndexKind::Unique,
        };
        let new_idx = IndexDef {
            name: "i".to_string(),
            path: fp(&["x"]),
            kind: IndexKind::NonUnique,
        };
        let ch = classify_schema_update(
            &[FieldDef::new(fp(&["x"]), Type::Int64)],
            &[old_idx],
            &[FieldDef::new(fp(&["x"]), Type::Int64)],
            &[new_idx],
        )
        .unwrap();
        assert!(matches!(ch, SchemaChange::Breaking { .. }));
    }
}
