// SPDX-License-Identifier: Apache-2.0

//! Collection and column metadata types for query planning.

use super::query::EngineType;
use crate::types_expr::SqlDataType;

/// Metadata about a collection for query planning.
#[derive(Debug, Clone)]
pub struct CollectionInfo {
    pub name: String,
    pub engine: EngineType,
    pub columns: Vec<ColumnInfo>,
    pub primary_key: Option<String>,
    pub has_auto_tier: bool,
    /// Secondary indexes available for planner rewrites. Populated by the
    /// catalog adapter from `StoredCollection.indexes`. `Building` entries
    /// are included so the planner can see them but MUST be skipped when
    /// choosing an index lookup — only `Ready` indexes back query rewrites.
    pub indexes: Vec<IndexSpec>,
    /// When `true`, this collection stores every write as an immutable
    /// version keyed by `system_from_ms`. Enables `FOR SYSTEM_TIME AS OF`
    /// and `FOR VALID_TIME` queries. Only meaningful for document engines
    /// today; other engines ignore this flag.
    pub bitemporal: bool,
    /// Primary engine hint from the catalog.
    pub primary: nodedb_types::PrimaryEngine,
    /// Vector-primary configuration. `Some` only when
    /// `primary == PrimaryEngine::Vector`.
    pub vector_primary: Option<nodedb_types::VectorPrimaryConfig>,
    /// How this collection's rows are distributed across vShards.
    ///
    /// Authoritative per-collection partition metadata. Future routing layers
    /// read this instead of inferring distribution from engine type.
    pub partition_strategy: nodedb_types::PartitionStrategy,
}

/// Secondary index metadata surfaced to the SQL planner.
#[derive(Debug, Clone)]
pub struct IndexSpec {
    pub name: String,
    /// Canonical field path (`$.email`, `$.user.name`, or plain column name
    /// for strict documents — the catalog layer stores them uniformly).
    pub field: String,
    pub unique: bool,
    pub case_insensitive: bool,
    /// Build state. Only `Ready` indexes drive query rewrites.
    pub state: IndexState,
    /// Partial-index predicate as raw SQL text (`WHERE <expr>` body
    /// without the keyword), or `None` for full indexes. The planner
    /// uses this to reject rewrites whose WHERE clause doesn't entail
    /// the predicate — matching against such a partial index would
    /// omit rows the index didn't cover.
    pub predicate: Option<String>,
}

/// Planner-facing index state. Mirrors the catalog variant but lives here
/// so the SQL crate doesn't depend on `nodedb` internals.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IndexState {
    Building,
    Ready,
}

/// Metadata about a single column.
#[derive(Debug, Clone)]
pub struct ColumnInfo {
    pub name: String,
    pub data_type: SqlDataType,
    pub nullable: bool,
    pub is_primary_key: bool,
    /// Default value expression (e.g. "UUID_V7", "ULID", "NANOID(10)", "0", "'active'").
    pub default: Option<String>,
    /// Raw type string as stored in the catalog (e.g. `"JSON"`, `"TEXT"`, `"FLOAT64"`).
    /// `None` for columns synthesized by the planner (e.g. auto-injected `id`).
    /// Columnar INSERT converters use this to reconstruct the exact `ColumnType`
    /// so JSON / Geometry / UUID columns are not incorrectly inferred as String.
    pub raw_type: Option<String>,
}
