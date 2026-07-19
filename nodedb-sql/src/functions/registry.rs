// SPDX-License-Identifier: Apache-2.0

//! Built-in function registry for SQL planning.
//!
//! Tracks known functions, their categories, arg specs, return types,
//! and whether they trigger special engine routing (e.g., vector_distance
//! → VectorSearch).

use nodedb_types::columnar::ColumnType;

use super::builtins::builtin_functions;

/// Semantic version for tracking when a function was introduced.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Version {
    pub major: u8,
    pub minor: u8,
    pub patch: u8,
}

impl Version {
    pub const fn new(major: u8, minor: u8, patch: u8) -> Self {
        Self {
            major,
            minor,
            patch,
        }
    }
}

/// Specification for a single function argument position.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArgTypeSpec {
    /// Argument name, for documentation and error messages.
    pub name: &'static str,
    /// Accepted column types. Empty slice means any type is accepted (wildcard).
    pub accepted: &'static [ColumnType],
    /// If true on the last argument, this argument may repeat zero or more times.
    pub variadic: bool,
}

/// Function category.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FunctionCategory {
    Scalar,
    Aggregate,
    Window,
}

/// Whether a function triggers special engine routing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SearchTrigger {
    None,
    VectorSearch,
    MultiVectorSearch,
    /// Sparse-vector inverted-index search: `sparse_score(field, '{dim: weight, ...}')`
    /// in ORDER BY routes to `SqlPlan::SparseSearch` and lowers to
    /// `VectorOp::SparseSearch` (dot-product top-k over the sparse index).
    SparseSearch,
    TextSearch,
    HybridSearch,
    TextMatch,
    SpatialDWithin,
    SpatialContains,
    SpatialIntersects,
    SpatialWithin,
    TimeBucket,
    /// Array engine table-valued read: `ARRAY_SLICE(name, slice_obj, attrs?, limit?)`.
    ArraySlice,
    /// Array engine table-valued read: `ARRAY_PROJECT(name, attrs)`.
    ArrayProject,
    /// Array engine table-valued aggregate:
    /// `ARRAY_AGG(name, attr, reducer, group_by_dim?)`.
    ArrayAgg,
    /// Array engine table-valued elementwise:
    /// `ARRAY_ELEMENTWISE(left, right, op, attr)`.
    ArrayElementwise,
    /// Array engine maintenance scalar (returns BOOL): `ARRAY_FLUSH(name)`.
    ArrayFlush,
    /// Array engine maintenance scalar (returns BOOL): `ARRAY_COMPACT(name)`.
    ArrayCompact,
    /// Planner-intercepted graph-distance marker for three-source RRF fusion.
    /// `graph_score(node_id_col, seed_id, depth => N, label => 'edge_label')`
    /// is never evaluated per-row; the hybrid planner extracts its arguments
    /// and builds a graph BFS spec in the physical plan.
    GraphSearch,
}

/// Metadata about a known function.
#[derive(Debug, Clone)]
pub struct FunctionMeta {
    pub name: &'static str,
    pub category: FunctionCategory,
    pub min_args: usize,
    pub max_args: usize,
    pub search_trigger: SearchTrigger,
    /// Static return type, when known at plan time. `None` means the type
    /// is context-dependent or unknown (resolved at runtime).
    pub return_type: Option<ColumnType>,
    /// Per-position argument type specifications.
    pub arg_types: &'static [ArgTypeSpec],
    /// Version in which this function was introduced.
    pub since: Version,
}

/// The function registry.
pub struct FunctionRegistry {
    functions: Vec<FunctionMeta>,
}

impl FunctionRegistry {
    /// Create the default registry with all built-in functions.
    pub fn new() -> Self {
        Self {
            functions: builtin_functions(),
        }
    }

    /// Look up a function by name (case-insensitive).
    pub fn lookup(&self, name: &str) -> Option<&FunctionMeta> {
        let lower = name.to_lowercase();
        self.functions.iter().find(|f| f.name == lower)
    }

    /// Check if a function triggers special search routing.
    pub fn search_trigger(&self, name: &str) -> SearchTrigger {
        self.lookup(name)
            .map(|f| f.search_trigger)
            .unwrap_or(SearchTrigger::None)
    }

    /// Check if a function is an aggregate.
    pub fn is_aggregate(&self, name: &str) -> bool {
        self.lookup(name)
            .is_some_and(|f| f.category == FunctionCategory::Aggregate)
    }

    /// Check if a function is a window function.
    pub fn is_window(&self, name: &str) -> bool {
        self.lookup(name)
            .is_some_and(|f| f.category == FunctionCategory::Window)
    }
}

impl Default for FunctionRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lookup_builtin() {
        let reg = FunctionRegistry::new();
        assert!(reg.is_aggregate("COUNT"));
        assert!(reg.is_aggregate("sum"));
        assert!(!reg.is_aggregate("vector_distance"));
        assert!(reg.is_window("row_number"));
        assert_eq!(
            reg.search_trigger("vector_distance"),
            SearchTrigger::VectorSearch
        );
        assert_eq!(
            reg.search_trigger("st_dwithin"),
            SearchTrigger::SpatialDWithin
        );
        assert_eq!(reg.search_trigger("time_bucket"), SearchTrigger::TimeBucket);
    }

    #[test]
    fn chunk_text_renamed_to_ndb_chunk_text() {
        let reg = FunctionRegistry::new();
        assert!(
            reg.lookup("ndb_chunk_text").is_some(),
            "ndb_chunk_text must be registered"
        );
        assert!(
            reg.lookup("chunk_text").is_none(),
            "old name chunk_text must not exist"
        );
    }

    #[test]
    fn unimplemented_functions_removed() {
        let reg = FunctionRegistry::new();
        assert!(reg.lookup("currency").is_none());
        assert!(reg.lookup("distribute").is_none());
        assert!(reg.lookup("allocate").is_none());
        assert!(reg.lookup("resolve_permission").is_none());
        assert!(reg.lookup("convert_currency").is_none());
    }

    #[test]
    fn all_builtins_have_since_set() {
        let reg = FunctionRegistry::new();
        let v0_1_0 = Version::new(0, 1, 0);
        for f in &reg.functions {
            assert_eq!(
                f.since, v0_1_0,
                "function '{}' must have since = Version::new(0, 1, 0)",
                f.name
            );
        }
    }

    #[test]
    fn all_builtins_arg_counts_consistent() {
        let reg = FunctionRegistry::new();
        for f in &reg.functions {
            let n = f.arg_types.len();
            let last_variadic = f.arg_types.last().is_some_and(|a| a.variadic);
            if last_variadic {
                // min_args must be <= arg_types.len()
                assert!(
                    f.min_args <= n,
                    "function '{}': min_args ({}) > arg_types.len() ({}) with variadic last arg",
                    f.name,
                    f.min_args,
                    n
                );
            } else {
                // min_args <= n <= max_args
                assert!(
                    f.min_args <= n,
                    "function '{}': min_args ({}) > arg_types.len() ({})",
                    f.name,
                    f.min_args,
                    n
                );
                assert!(
                    n <= f.max_args,
                    "function '{}': arg_types.len() ({}) > max_args ({})",
                    f.name,
                    n,
                    f.max_args
                );
            }
        }
    }

    #[test]
    fn return_type_spot_checks() {
        let reg = FunctionRegistry::new();
        assert_eq!(
            reg.lookup("now").and_then(|f| f.return_type),
            Some(ColumnType::Timestamptz),
            "now() must return Timestamptz"
        );
        assert_eq!(
            reg.lookup("count").and_then(|f| f.return_type),
            Some(ColumnType::Int64),
            "count must return Int64"
        );
        assert_eq!(
            reg.lookup("doc_exists").and_then(|f| f.return_type),
            Some(ColumnType::Bool),
            "doc_exists must return Bool"
        );
        assert_eq!(
            reg.lookup("st_contains").and_then(|f| f.return_type),
            Some(ColumnType::Bool),
            "st_contains must return Bool"
        );
        assert_eq!(
            reg.lookup("pg_fts_match").and_then(|f| f.return_type),
            Some(ColumnType::Bool),
            "pg_fts_match must return Bool"
        );
        assert_eq!(
            reg.lookup("lower").and_then(|f| f.return_type),
            Some(ColumnType::String),
            "lower must return String"
        );
    }
}
