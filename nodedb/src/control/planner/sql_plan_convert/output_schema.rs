// SPDX-License-Identifier: BUSL-1.1

//! Derives the planner-authoritative [`OutputSchema`] from a compiled
//! `SqlPlan` list, for later threading into response shaping.
//!
//! Purely additive: nothing in this module is consumed by any existing
//! call site yet. The single call site that invokes [`build_output_schema`]
//! today discards the result.

use std::collections::HashMap;

use nodedb_sql::catalog::SqlCatalog;
use nodedb_sql::types::SqlPlan;
use nodedb_sql::types::query::Projection;
use nodedb_sql::types_expr::SqlExpr;

use crate::control::server::response_shape::schema::{
    OutputColumn, OutputSchema, sql_data_type_to_ddl_col_type,
};
use crate::control::server::response_shape::types::DdlColType;

/// Maps one `Projection` entry to an `OutputColumn`, given a map of bare
/// column name -> resolved wire type for the collection in scope.
///
/// Mirrors `response_shape::project::expr_column_names`'s derivation rule:
/// for a qualified `table.column` reference, `lookup_key` keeps the full
/// dot-joined form (the join executor prefixes every key with its source
/// collection name) while `display_name` is the last segment. For a bare
/// column both are identical.
///
/// `Projection::Star` / `Projection::QualifiedStar` have no single concrete
/// column and return `None`; the caller sets `is_star` instead.
fn projection_to_column(
    p: &Projection,
    types: &HashMap<String, DdlColType>,
) -> Option<OutputColumn> {
    match p {
        Projection::Column(qname) => {
            let display_name = qname
                .rsplit('.')
                .next()
                .map(str::to_string)
                .unwrap_or_else(|| qname.clone());
            let ty = types
                .get(&display_name)
                .copied()
                .unwrap_or(DdlColType::Text);
            Some(OutputColumn {
                display_name,
                lookup_key: qname.clone(),
                ty,
            })
        }
        Projection::Computed { alias, .. } => Some(OutputColumn {
            display_name: alias.clone(),
            lookup_key: alias.clone(),
            ty: DdlColType::Text,
        }),
        Projection::Star | Projection::QualifiedStar(_) => None,
    }
}

/// Builds a `HashMap` of bare column name -> resolved wire type for
/// `collection`, via a best-effort catalog lookup. Returns an empty map
/// (never an error) when the lookup fails or the collection is unknown —
/// callers fall back to `DdlColType::Text` for every column in that case.
fn column_types_for<C: SqlCatalog>(
    catalog: &C,
    database_id: nodedb_types::DatabaseId,
    collection: &str,
) -> HashMap<String, DdlColType> {
    match catalog.get_collection(database_id, collection) {
        Ok(Some(info)) => info
            .columns
            .iter()
            .map(|c| (c.name.clone(), sql_data_type_to_ddl_col_type(&c.data_type)))
            .collect(),
        _ => HashMap::new(),
    }
}

/// Derives an `OutputColumn` for one GROUP BY key expression.
///
/// Mirrors the `Projection::Column` naming rule: a bare/qualified column
/// name's `display_name` is its last dot segment while `lookup_key` keeps
/// the full qualified form; any other expression shape falls back to
/// `DdlColType::Text` with no resolvable name, using the group index as a
/// stable placeholder lookup key.
fn group_by_key_column(expr: &SqlExpr, index: usize) -> OutputColumn {
    match expr {
        SqlExpr::Column { table, name } => {
            let lookup_key = match table {
                Some(t) => format!("{t}.{name}"),
                None => name.clone(),
            };
            OutputColumn {
                display_name: name.clone(),
                lookup_key,
                ty: DdlColType::Text,
            }
        }
        _ => {
            let placeholder = format!("group_{index}");
            OutputColumn {
                display_name: placeholder.clone(),
                lookup_key: placeholder,
                ty: DdlColType::Text,
            }
        }
    }
}

/// Maps a projection list to an `OutputSchema` fragment using `types`.
fn schema_from_projection(
    projection: &[Projection],
    types: &HashMap<String, DdlColType>,
) -> OutputSchema {
    let mut columns = Vec::with_capacity(projection.len());
    let mut is_star = false;
    for p in projection {
        match projection_to_column(p, types) {
            Some(col) => columns.push(col),
            None => is_star = true,
        }
    }
    OutputSchema { columns, is_star }
}

/// Derives the planner-authoritative output schema of a compiled plan list.
///
/// Only the plan variants that carry a resolvable projection against a
/// single named collection are handled directly; other plan variants are
/// handled by later units in this effort and fall back to an empty schema.
pub fn build_output_schema<C: SqlCatalog>(
    plans: &[SqlPlan],
    catalog: &C,
    database_id: nodedb_types::DatabaseId,
) -> OutputSchema {
    let Some(plan) = plans.first() else {
        return OutputSchema {
            columns: Vec::new(),
            is_star: false,
        };
    };

    match plan {
        SqlPlan::Scan {
            collection,
            projection,
            ..
        }
        | SqlPlan::DocumentIndexLookup {
            collection,
            projection,
            ..
        }
        | SqlPlan::SpatialScan {
            collection,
            projection,
            ..
        }
        | SqlPlan::TimeseriesScan {
            collection,
            projection,
            ..
        } => {
            let types = column_types_for(catalog, database_id, collection);
            schema_from_projection(projection, &types)
        }
        SqlPlan::Join { projection, .. } => {
            // A join has no single source collection; column types default
            // to `Text` for every projected field rather than picking one
            // side arbitrarily.
            let types = HashMap::new();
            schema_from_projection(projection, &types)
        }
        SqlPlan::ConstantResult { columns, .. } => OutputSchema {
            columns: columns
                .iter()
                .map(|c| OutputColumn {
                    display_name: c.clone(),
                    lookup_key: c.clone(),
                    ty: DdlColType::Text,
                })
                .collect(),
            is_star: false,
        },
        SqlPlan::Aggregate {
            group_by,
            aggregates,
            ..
        } => {
            let mut columns = Vec::with_capacity(group_by.len() + aggregates.len());
            for (index, key) in group_by.iter().enumerate() {
                columns.push(group_by_key_column(key, index));
            }
            for agg in aggregates {
                // `AggregateExpr::alias` is always populated by the planner:
                // either the user's explicit alias, or (for unnamed
                // projections) the lowercased unparsed expression text —
                // e.g. `count(*)` — matching
                // `response_shape::project::expr_column_names`'s own
                // lowercasing of non-column expressions. So the alias is
                // already the canonical name; no separate derivation needed.
                let ty = if agg.function.eq_ignore_ascii_case("count") {
                    DdlColType::Int8
                } else {
                    DdlColType::Text
                };
                columns.push(OutputColumn {
                    display_name: agg.alias.clone(),
                    lookup_key: agg.alias.clone(),
                    ty,
                });
            }
            OutputSchema {
                columns,
                is_star: false,
            }
        }
        // Set operations take their column names/types from the first
        // (left) branch, matching standard SQL set-op semantics.
        SqlPlan::Union { inputs, .. } => match inputs.first() {
            Some(first) => build_output_schema(std::slice::from_ref(first), catalog, database_id),
            None => OutputSchema::default(),
        },
        SqlPlan::Intersect { left, .. } | SqlPlan::Except { left, .. } => {
            build_output_schema(std::slice::from_ref(left.as_ref()), catalog, database_id)
        }
        SqlPlan::RecursiveValue { columns, .. } => OutputSchema {
            columns: columns
                .iter()
                .map(|name| OutputColumn {
                    display_name: name.clone(),
                    lookup_key: name.clone(),
                    ty: DdlColType::Text,
                })
                .collect(),
            is_star: false,
        },
        // The outer query determines the final projected shape; the CTE
        // definitions themselves are only inputs to it.
        SqlPlan::Cte { outer, .. } => {
            build_output_schema(std::slice::from_ref(outer.as_ref()), catalog, database_id)
        }
        SqlPlan::LateralTopK { projection, .. } | SqlPlan::LateralLoop { projection, .. } => {
            // No single source collection spans both outer and inner rows;
            // default every projected field to `Text` rather than picking
            // one side's catalog arbitrarily.
            let types = HashMap::new();
            schema_from_projection(projection, &types)
        }
        SqlPlan::ArraySlice {
            attr_projection, ..
        }
        | SqlPlan::ArrayProject {
            attr_projection, ..
        } => OutputSchema {
            columns: attr_projection
                .iter()
                .map(|name| OutputColumn {
                    display_name: name.clone(),
                    lookup_key: name.clone(),
                    ty: DdlColType::Text,
                })
                .collect(),
            is_star: false,
        },
        // `Merge` carries no output projection (it reports affected-row
        // counts, optionally with `RETURNING`, which isn't a static column
        // list on the plan) and `ArrayAgg` / `ArrayElementwise` compute a
        // single synthesized value column with no name carried on the plan
        // either; both are left to a later unit rather than guessed here.
        //
        // Other plan variants (VectorSearch, HybridSearch, Match, etc.) are
        // handled by later units in this effort.
        _ => OutputSchema {
            columns: Vec::new(),
            is_star: false,
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bare_column_uses_matching_type_from_map() {
        let mut types = HashMap::new();
        types.insert("foo".to_string(), DdlColType::Int8);
        let p = Projection::Column("foo".to_string());
        let col = projection_to_column(&p, &types).expect("Some for Column");
        assert_eq!(col.lookup_key, "foo");
        assert_eq!(col.display_name, "foo");
        assert_eq!(col.ty, DdlColType::Int8);
    }

    #[test]
    fn qualified_column_display_is_last_segment() {
        let types = HashMap::new();
        let p = Projection::Column("t.bar".to_string());
        let col = projection_to_column(&p, &types).expect("Some for Column");
        assert_eq!(col.lookup_key, "t.bar");
        assert_eq!(col.display_name, "bar");
        assert_eq!(col.ty, DdlColType::Text);
    }

    #[test]
    fn computed_uses_alias_for_both_and_defaults_to_text() {
        let types = HashMap::new();
        let p = Projection::Computed {
            expr: nodedb_sql::types_expr::SqlExpr::Wildcard,
            alias: "total".to_string(),
        };
        let col = projection_to_column(&p, &types).expect("Some for Computed");
        assert_eq!(col.lookup_key, "total");
        assert_eq!(col.display_name, "total");
        assert_eq!(col.ty, DdlColType::Text);
    }

    #[test]
    fn star_returns_none() {
        let types = HashMap::new();
        assert!(projection_to_column(&Projection::Star, &types).is_none());
        assert!(
            projection_to_column(&Projection::QualifiedStar("t".to_string()), &types).is_none()
        );
    }

    /// Catalog stub whose `get_collection` is never called by the
    /// `ConstantResult` branch under test; only required to satisfy the
    /// generic `SqlCatalog` bound on `build_output_schema`.
    struct NoCatalog;

    impl SqlCatalog for NoCatalog {
        fn get_collection(
            &self,
            _database_id: nodedb_types::DatabaseId,
            _name: &str,
        ) -> Result<Option<nodedb_sql::types::CollectionInfo>, nodedb_sql::catalog::SqlCatalogError>
        {
            Ok(None)
        }
    }

    #[test]
    fn constant_result_columns_map_to_text_output_columns() {
        let plans = vec![SqlPlan::ConstantResult {
            columns: vec!["a".to_string(), "b".to_string()],
            values: vec![],
        }];
        let schema = build_output_schema(&plans, &NoCatalog, nodedb_types::DatabaseId::DEFAULT);
        assert_eq!(schema.columns.len(), 2);
        assert_eq!(schema.columns[0].display_name, "a");
        assert_eq!(schema.columns[0].lookup_key, "a");
        assert_eq!(schema.columns[1].display_name, "b");
        assert!(!schema.is_star);
    }

    /// Minimal `Scan` plan against `collection`, used only to exercise
    /// recursion (Union/Intersect/Except/Cte); the catalog is `NoCatalog`
    /// so every column falls back to `DdlColType::Text`.
    fn scan_plan(collection: &str, projection: Vec<Projection>) -> SqlPlan {
        SqlPlan::Scan {
            collection: collection.to_string(),
            alias: None,
            engine: nodedb_sql::types::query::EngineType::DocumentSchemaless,
            filters: Vec::new(),
            projection,
            sort_keys: Vec::new(),
            limit: None,
            offset: 0,
            distinct: false,
            window_functions: Vec::new(),
            temporal: nodedb_sql::temporal::TemporalScope::default(),
        }
    }

    #[test]
    fn aggregate_outputs_group_keys_then_aggregates_in_order() {
        use nodedb_sql::types::query::AggregateExpr;

        let plans = vec![SqlPlan::Aggregate {
            input: Box::new(scan_plan("orders", vec![])),
            group_by: vec![SqlExpr::Column {
                table: None,
                name: "status".to_string(),
            }],
            aggregates: vec![
                AggregateExpr {
                    function: "sum".to_string(),
                    args: vec![SqlExpr::Column {
                        table: None,
                        name: "x".to_string(),
                    }],
                    alias: "total".to_string(),
                    distinct: false,
                    grouping_col_index: None,
                },
                AggregateExpr {
                    function: "count".to_string(),
                    args: vec![SqlExpr::Wildcard],
                    alias: "count(*)".to_string(),
                    distinct: false,
                    grouping_col_index: None,
                },
            ],
            having: Vec::new(),
            limit: 0,
            grouping_sets: None,
            sort_keys: Vec::new(),
        }];

        let schema = build_output_schema(&plans, &NoCatalog, nodedb_types::DatabaseId::DEFAULT);
        assert_eq!(schema.columns.len(), 3);
        assert_eq!(schema.columns[0].display_name, "status");
        assert_eq!(schema.columns[0].lookup_key, "status");
        assert_eq!(schema.columns[1].display_name, "total");
        assert_eq!(schema.columns[1].lookup_key, "total");
        assert_eq!(schema.columns[1].ty, DdlColType::Text);
        assert_eq!(schema.columns[2].display_name, "count(*)");
        assert_eq!(schema.columns[2].lookup_key, "count(*)");
        assert_eq!(schema.columns[2].ty, DdlColType::Int8);
        assert!(!schema.is_star);
    }

    #[test]
    fn union_takes_schema_from_first_input() {
        let plans = vec![SqlPlan::Union {
            inputs: vec![
                scan_plan("a", vec![Projection::Column("id".to_string())]),
                scan_plan("b", vec![Projection::Column("other".to_string())]),
            ],
            distinct: false,
        }];
        let schema = build_output_schema(&plans, &NoCatalog, nodedb_types::DatabaseId::DEFAULT);
        assert_eq!(schema.columns.len(), 1);
        assert_eq!(schema.columns[0].display_name, "id");
    }

    #[test]
    fn recursive_value_columns_map_to_text_output_columns() {
        let plans = vec![SqlPlan::RecursiveValue {
            cte_name: "c".to_string(),
            columns: vec!["n".to_string()],
            init_exprs: vec!["1".to_string()],
            step_exprs: vec!["n + 1".to_string()],
            condition: None,
            max_depth: 100,
            distinct: false,
        }];
        let schema = build_output_schema(&plans, &NoCatalog, nodedb_types::DatabaseId::DEFAULT);
        assert_eq!(schema.columns.len(), 1);
        assert_eq!(schema.columns[0].display_name, "n");
        assert_eq!(schema.columns[0].lookup_key, "n");
        assert_eq!(schema.columns[0].ty, DdlColType::Text);
        assert!(!schema.is_star);
    }
}
