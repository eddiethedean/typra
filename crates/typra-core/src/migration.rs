//! Migration planning and helpers.
//!
//! Typra keeps schema evolution conservative by default. When a proposed schema change requires
//! rewriting existing data (e.g. adding a required field), callers can plan and then execute a
//! migration using helpers provided here.

use crate::schema::SchemaChange;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MigrationStep {
    /// Backfill a missing top-level field for all existing rows in a collection.
    BackfillTopLevelField { field: String },
    /// Rebuild index entries for a collection (typically after adding a new index definition).
    RebuildIndexes,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationPlan {
    pub change: SchemaChange,
    pub steps: Vec<MigrationStep>,
}
