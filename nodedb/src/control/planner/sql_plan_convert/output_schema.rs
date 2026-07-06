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
use nodedb_sql::types::query::{AggOutputSlot, Projection};
use nodedb_sql::types_expr::SqlExpr;

use crate::control::server::response_shape::schema::{
    OutputColumn, OutputSchema, sql_data_type_to_ddl_col_type,
};
use crate::control::server::response_shape::types::DdlColType;

/// Maps one `Projection` entry to an `OutputColumn`, given a map of bare
/// column name -> resolved wire type for the collection in scope.
///
/// This is the authoritative derivation rule (see also `schema_from_projection`
/// in this module): for a qualified `table.column` reference, `lookup_key` keeps the full
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
        Projection::Computed { expr, alias } => {
            // For an aliased column reference (`o.id AS oid`) the Data Plane
            // keys the value by the underlying column, not the alias — so the
            // lookup_key must be the qualified expression (matching the join
            // executor's prefixed keys) while the alias is only the display
            // name. A genuine computed expression (`price * qty AS total`) is
            // emitted by the executor under its alias, so that stays the key.
            let lookup_key = match expr {
                SqlExpr::Column {
                    table: Some(t),
                    name,
                } => format!("{t}.{name}"),
                SqlExpr::Column { table: None, name } => name.clone(),
                _ => alias.clone(),
            };
            Some(OutputColumn {
                display_name: alias.clone(),
                lookup_key,
                ty: DdlColType::Text,
            })
        }
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
/// The `display_name` is the SELECT-list output name: the explicit alias when
/// the projection aliased the key (`SELECT k AS label ... GROUP BY k` yields
/// output column `label`, matching Postgres), otherwise the key's own column
/// name. The `lookup_key` always stays the raw grouped column name (the key
/// the aggregate executor emits the value under), so `project_row` still finds
/// the value.
///
/// The output type is `DdlColType::Text`: the pgwire SELECT encoder is
/// text-format only, so advertising a non-TEXT type OID without matching typed
/// value encoding would break the extended-query protocol. (Typed aggregate /
/// group-key reporting is deferred to a dedicated pgwire value-encoding unit.)
///
/// A non-`Column` GROUP BY key (a computed expression) uses the alias when
/// present, else a stable `group_{index}` placeholder (also used as the lookup
/// key).
fn group_by_key_column(expr: &SqlExpr, index: usize, alias: Option<&str>) -> OutputColumn {
    match expr {
        SqlExpr::Column { table, name } => {
            let lookup_key = match table {
                Some(t) => format!("{t}.{name}"),
                None => name.clone(),
            };
            let display_name = alias.map(str::to_string).unwrap_or_else(|| name.clone());
            OutputColumn {
                display_name,
                lookup_key,
                ty: DdlColType::Text,
            }
        }
        _ => {
            let placeholder = format!("group_{index}");
            let display_name = alias
                .map(str::to_string)
                .unwrap_or_else(|| placeholder.clone());
            OutputColumn {
                display_name,
                lookup_key: placeholder,
                ty: DdlColType::Text,
            }
        }
    }
}

/// Returns the collection's columns in declared catalog order, mapped to
/// `OutputColumn`s (`display_name` = `lookup_key` = column name). Returns an
/// empty `Vec` when the catalog/collection lookup fails or the collection has
/// no declared columns (e.g. a schemaless collection) — so a schemaless
/// `SELECT *` still yields empty columns, deriving its shape from the rows.
fn ordered_columns_for<C: SqlCatalog>(
    catalog: &C,
    database_id: nodedb_types::DatabaseId,
    collection: &str,
) -> Vec<OutputColumn> {
    match catalog.get_collection(database_id, collection) {
        Ok(Some(info)) => info
            .columns
            .iter()
            .map(|c| OutputColumn {
                display_name: c.name.clone(),
                lookup_key: c.name.clone(),
                ty: sql_data_type_to_ddl_col_type(&c.data_type),
            })
            .collect(),
        _ => Vec::new(),
    }
}

/// Maps a projection list to an `OutputSchema` fragment using `types`.
///
/// A `Star` / `QualifiedStar` in the projection sets `is_star` and expands
/// into `ordered_cols` (the collection's catalog columns in declared order),
/// appending only entries not already produced by a named projection. When
/// the projection has no star, `ordered_cols` is ignored and behavior is the
/// named-columns-only, `is_star=false` case.
fn schema_from_projection(
    projection: &[Projection],
    types: &HashMap<String, DdlColType>,
    ordered_cols: &[OutputColumn],
) -> OutputSchema {
    let mut columns = Vec::with_capacity(projection.len());
    let mut is_star = false;
    for p in projection {
        match projection_to_column(p, types) {
            Some(col) => columns.push(col),
            None => {
                is_star = true;
                for oc in ordered_cols {
                    if !columns.iter().any(|c| c.lookup_key == oc.lookup_key) {
                        columns.push(oc.clone());
                    }
                }
            }
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
        }
        | SqlPlan::PointGet {
            collection,
            projection,
            ..
        }
        | SqlPlan::RangeScan {
            collection,
            projection,
            ..
        }
        | SqlPlan::RecursiveScan {
            collection,
            projection,
            ..
        }
        | SqlPlan::VectorSearch {
            collection,
            projection,
            ..
        }
        | SqlPlan::MultiVectorSearch {
            collection,
            projection,
            ..
        }
        | SqlPlan::TextSearch {
            collection,
            projection,
            ..
        }
        | SqlPlan::HybridSearch {
            collection,
            projection,
            ..
        }
        | SqlPlan::HybridSearchTriple {
            collection,
            projection,
            ..
        } => {
            let types = column_types_for(catalog, database_id, collection);
            let ordered_cols = ordered_columns_for(catalog, database_id, collection);
            schema_from_projection(projection, &types, &ordered_cols)
        }
        SqlPlan::Join { projection, .. } => {
            // A join has no single source collection; column types default
            // to `Text` for every projected field rather than picking one
            // side arbitrarily. A star here has no single catalog to expand
            // against, so no ordered columns are supplied.
            let types = HashMap::new();
            schema_from_projection(projection, &types, &[])
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
            group_by_aliases,
            output_order,
            aggregates,
            ..
        } => {
            // Derives the `OutputColumn` for one GROUP BY key: `group_by_aliases`
            // is parallel to `group_by` when populated, but may be empty when the
            // plan was built without a projection in scope — treat a
            // missing/`None` entry as "no alias".
            let key_column = |index: usize| {
                group_by.get(index).map(|key| {
                    let alias = group_by_aliases.get(index).and_then(|a| a.as_deref());
                    group_by_key_column(key, index, alias)
                })
            };
            // Derives the `OutputColumn` for one aggregate. `AggregateExpr::alias`
            // is always populated by the planner: either the user's explicit
            // alias, or (for unnamed projections) the lowercased unparsed
            // expression text — e.g. `count(*)` — matching this module's own
            // lowercasing of non-column expressions. So the alias is already the
            // canonical name; no separate derivation needed. All aggregate result
            // values ship as TEXT (the pgwire SELECT encoder is text-format only;
            // typed OIDs are deferred to a dedicated value-encoding unit).
            let agg_column = |index: usize| {
                aggregates.get(index).map(|agg| OutputColumn {
                    display_name: agg.alias.clone(),
                    lookup_key: agg.alias.clone(),
                    ty: DdlColType::Text,
                })
            };
            let mut columns = Vec::with_capacity(group_by.len() + aggregates.len());
            if output_order.is_empty() {
                // Built without a projection in scope: fall back to
                // group-keys-first, then aggregates.
                for index in 0..group_by.len() {
                    columns.extend(key_column(index));
                }
                for index in 0..aggregates.len() {
                    columns.extend(agg_column(index));
                }
            } else {
                // Emit columns in the recorded SELECT-list order.
                for slot in output_order {
                    match slot {
                        AggOutputSlot::GroupKey(index) => columns.extend(key_column(*index)),
                        AggOutputSlot::Aggregate(index) => columns.extend(agg_column(*index)),
                    }
                }
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
            // one side's catalog arbitrarily. A star has no single catalog to
            // expand against, so no ordered columns are supplied.
            let types = HashMap::new();
            schema_from_projection(projection, &types, &[])
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
        // Writes / DDL: no output rows, nothing to shape.
        SqlPlan::Insert { .. }
        | SqlPlan::KvInsert { .. }
        | SqlPlan::Upsert { .. }
        | SqlPlan::Update { .. }
        | SqlPlan::UpdateFrom { .. }
        | SqlPlan::Delete { .. }
        | SqlPlan::Truncate { .. }
        | SqlPlan::TimeseriesIngest { .. }
        | SqlPlan::InsertSelect { .. }
        | SqlPlan::CreateArray { .. }
        | SqlPlan::DropArray { .. }
        | SqlPlan::AlterArray { .. }
        | SqlPlan::InsertArray { .. }
        | SqlPlan::DeleteArray { .. }
        | SqlPlan::VectorPrimaryInsert { .. }
        | SqlPlan::CreateIndex { .. }
        | SqlPlan::DropIndex { .. }
        | SqlPlan::ArrayFlush { .. }
        | SqlPlan::ArrayCompact { .. } => OutputSchema::default(),
        // `Merge` (with or without RETURNING) and `Update`/`UpdateFrom` with
        // `returning: true` are shaped downstream via
        // `PlanKind::ReturningRows` -> `shape_returning_rows`, which reads
        // column names/values directly out of the response payload
        // (`RowsPayload` msgpack) and never consults `OutputSchema`. An
        // empty schema here is therefore correct, not a placeholder.
        SqlPlan::Merge { .. } => OutputSchema::default(),
        // `ArrayAgg` / `ArrayElementwise` compile to `ArrayOp::Aggregate` /
        // `ArrayOp::Elementwise`, which `describe_plan` classifies as
        // `PlanKind::MultiRow`. `MultiRow` responses are shaped by
        // `shape_generic_rows` -> `shape_decoded_rows`, which derives column
        // names from the decoded JSON payload itself, not from
        // `OutputSchema`. An empty schema here is therefore correct.
        SqlPlan::ArrayAgg { .. } | SqlPlan::ArrayElementwise { .. } => OutputSchema::default(),
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
            // `SELECT status AS state ...` — the group-key output name is the
            // SELECT-list alias, while the value lookup key stays the raw
            // grouped column name.
            group_by_aliases: vec![Some("state".to_string())],
            // Empty output_order exercises the group-keys-first fallback.
            output_order: Vec::new(),
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
        // Group-key display_name is the SELECT-list alias; lookup_key stays
        // the raw grouped column name (the executor's emitted value key).
        assert_eq!(schema.columns[0].display_name, "state");
        assert_eq!(schema.columns[0].lookup_key, "status");
        assert_eq!(schema.columns[1].display_name, "total");
        assert_eq!(schema.columns[1].lookup_key, "total");
        // Aggregate result columns ship as TEXT (pgwire SELECT encoder is
        // text-format only; typed OIDs deferred to a value-encoding unit).
        assert_eq!(schema.columns[1].ty, DdlColType::Text);
        assert_eq!(schema.columns[2].display_name, "count(*)");
        assert_eq!(schema.columns[2].lookup_key, "count(*)");
        assert_eq!(schema.columns[2].ty, DdlColType::Text);
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

    /// Shared projection for the leaf-variant tests below: a bare `id`
    /// column plus a computed `dist` alias.
    fn id_and_dist_projection() -> Vec<Projection> {
        vec![
            Projection::Column("id".to_string()),
            Projection::Computed {
                expr: SqlExpr::Wildcard,
                alias: "dist".to_string(),
            },
        ]
    }

    fn assert_id_and_dist_schema(schema: &OutputSchema) {
        assert_eq!(schema.columns.len(), 2);
        assert_eq!(schema.columns[0].display_name, "id");
        assert_eq!(schema.columns[0].lookup_key, "id");
        assert_eq!(schema.columns[0].ty, DdlColType::Text);
        assert_eq!(schema.columns[1].display_name, "dist");
        assert_eq!(schema.columns[1].lookup_key, "dist");
        assert_eq!(schema.columns[1].ty, DdlColType::Text);
        assert!(!schema.is_star);
    }

    #[test]
    fn point_get_uses_its_own_projection() {
        let plans = vec![SqlPlan::PointGet {
            collection: "users".to_string(),
            alias: None,
            engine: nodedb_sql::types::query::EngineType::DocumentSchemaless,
            key_column: "id".to_string(),
            key_value: nodedb_sql::types_expr::SqlValue::Null,
            projection: id_and_dist_projection(),
        }];
        let schema = build_output_schema(&plans, &NoCatalog, nodedb_types::DatabaseId::DEFAULT);
        assert_id_and_dist_schema(&schema);
    }

    #[test]
    fn vector_search_uses_its_own_projection() {
        let plans = vec![SqlPlan::VectorSearch {
            collection: "docs".to_string(),
            field: "embedding".to_string(),
            query_vector: vec![0.0, 1.0],
            top_k: 10,
            ef_search: 64,
            metric: nodedb_sql::types::DistanceMetric::L2,
            filters: Vec::new(),
            array_prefilter: None,
            ann_options: nodedb_sql::types::VectorAnnOptions::default(),
            skip_payload_fetch: false,
            payload_filters: Vec::new(),
            projection: id_and_dist_projection(),
        }];
        let schema = build_output_schema(&plans, &NoCatalog, nodedb_types::DatabaseId::DEFAULT);
        assert_id_and_dist_schema(&schema);
    }

    #[test]
    fn hybrid_search_uses_its_own_projection() {
        let plans = vec![SqlPlan::HybridSearch {
            collection: "docs".to_string(),
            query_vector: vec![0.0, 1.0],
            query_text: "hello".to_string(),
            top_k: 10,
            ef_search: 64,
            vector_weight: 0.5,
            fuzzy: false,
            score_alias: None,
            projection: id_and_dist_projection(),
        }];
        let schema = build_output_schema(&plans, &NoCatalog, nodedb_types::DatabaseId::DEFAULT);
        assert_id_and_dist_schema(&schema);
    }

    #[test]
    fn text_search_uses_its_own_projection() {
        let plans = vec![SqlPlan::TextSearch {
            collection: "docs".to_string(),
            query: nodedb_sql::types::FtsQuery::Plain {
                text: "hello".to_string(),
                fuzzy: false,
            },
            top_k: 10,
            filters: Vec::new(),
            score_alias: None,
            projection: id_and_dist_projection(),
        }];
        let schema = build_output_schema(&plans, &NoCatalog, nodedb_types::DatabaseId::DEFAULT);
        assert_id_and_dist_schema(&schema);
    }
}
