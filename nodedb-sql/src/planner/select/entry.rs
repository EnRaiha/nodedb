// SPDX-License-Identifier: Apache-2.0

//! Top-level query entry: CTE handling and UNION dispatch. ORDER BY and
//! search-trigger detection live in `order_by.rs`; LIMIT / OFFSET application
//! lives in `limit.rs`.

use nodedb_types::DatabaseId;
use sqlparser::ast::{Query, SetExpr};

use super::limit::apply_limit;
use super::order_by::{apply_order_by, try_hybrid_from_projection};
use super::query_tail::QueryTail;
use super::select_stmt::plan_select;
use crate::error::{Result, SqlError};
use crate::functions::registry::FunctionRegistry;
use crate::reserved::check_ast_identifier;
use crate::temporal::TemporalScope;
use crate::types::{Projection, SqlExpr, *};

/// Returns `true` when every projection item is either:
/// - a plain column reference to the surrogate/PK column (`id` or `document_id`), or
/// - a `vector_distance(...)` function call (any alias).
///
/// Anything else — a payload field, `*`, or an unrecognised expression — returns `false`.
fn is_pure_vector_projection(projection: &[Projection]) -> bool {
    if projection.is_empty() {
        return false;
    }
    for item in projection {
        match item {
            Projection::Column(name) => {
                let lower = name.to_ascii_lowercase();
                if lower != "id" && lower != "document_id" {
                    return false;
                }
            }
            Projection::Computed { expr, .. } => {
                // Accept any of the three vector distance function names.
                let SqlExpr::Function { name, .. } = expr else {
                    return false;
                };
                if !name.eq_ignore_ascii_case("vector_distance")
                    && !name.eq_ignore_ascii_case("vector_cosine_distance")
                    && !name.eq_ignore_ascii_case("vector_neg_inner_product")
                {
                    return false;
                }
            }
            Projection::Star | Projection::QualifiedStar(_) => return false,
        }
    }
    true
}

/// Plan a SELECT query.
pub fn plan_query(
    query: &Query,
    catalog: &dyn SqlCatalog,
    functions: &FunctionRegistry,
    temporal: TemporalScope,
) -> Result<SqlPlan> {
    // Handle CTEs (WITH clause).
    if let Some(with) = &query.with
        && with.recursive
    {
        return crate::planner::cte::plan_recursive_cte(query, catalog, functions, temporal);
    }
    // Non-recursive CTEs: plan each CTE subquery and the outer query.
    if let Some(with) = &query.with
        && !with.cte_tables.is_empty()
    {
        let inner_query = Query {
            with: None,
            body: query.body.clone(),
            order_by: query.order_by.clone(),
            limit_clause: query.limit_clause.clone(),
            fetch: query.fetch.clone(),
            locks: query.locks.clone(),
            for_clause: query.for_clause.clone(),
            settings: query.settings.clone(),
            format_clause: query.format_clause.clone(),
            pipe_operators: query.pipe_operators.clone(),
        };

        // Plan each CTE subquery.
        let mut definitions = Vec::new();
        let mut cte_names = Vec::new();
        for cte in &with.cte_tables {
            let name = check_ast_identifier(&cte.alias.name)?;
            for column in &cte.alias.columns {
                check_ast_identifier(&column.name)?;
            }
            let cte_plan = plan_query(&cte.query, catalog, functions, temporal)?;
            definitions.push((name.clone(), cte_plan));
            cte_names.push(name);
        }

        // Build CTE-aware catalog so the outer query can reference CTE names.
        let cte_catalog = CteCatalog {
            inner: catalog,
            cte_names,
        };
        let outer = plan_query(&inner_query, &cte_catalog, functions, temporal)?;

        return Ok(SqlPlan::Cte {
            definitions,
            outer: Box::new(outer),
        });
    }

    // Handle UNION.
    match &*query.body {
        SetExpr::Select(select) => {
            // ORDER BY / LIMIT belong to the query, not to its SELECT body,
            // but the scan planner needs them to pick an access path that can
            // honour them — so they travel down with the SELECT.
            let tail = QueryTail {
                order_by: query.order_by.as_ref(),
                limit_clause: &query.limit_clause,
            };
            let mut plan = plan_select(select, catalog, functions, temporal, &tail)?;
            // Snapshot the projection before ORDER BY transforms the plan,
            // in case `apply_order_by` converts a Scan into VectorSearch.
            let pre_order_by_projection: Option<Vec<Projection>> = match &plan {
                SqlPlan::Scan { projection, .. } => Some(projection.clone()),
                _ => None,
            };
            let pre_order_by_collection: Option<String> = match &plan {
                SqlPlan::Scan { collection, .. } => Some(collection.clone()),
                _ => None,
            };
            if let Some(order_by) = &query.order_by {
                plan = apply_order_by(&plan, order_by, functions, &select.projection)?;
            }
            // Fall back to a SELECT-projection scan for hybrid-search and
            // text-search triggers. The `SELECT id, rrf_score(...) AS score
            // FROM c WHERE ... LIMIT N` shape has no ORDER BY, so
            // `apply_order_by` cannot fire. The same applies to
            // `SELECT id, bm25_score(field, term) FROM c ORDER BY id` where
            // ORDER BY does not contain a search trigger.
            //
            // Also fires when the plan is already `TextSearch` (set by the
            // WHERE `text_match(...)` path) and the SELECT list additionally
            // contains `bm25_score(...)` — in that case we attach the
            // `score_alias` so the executor knows to inject the score column.
            //
            // `apply_order_by` may have wrapped a search plan in a
            // post-processing tail to carry the sort, so the upgrade inspects
            // the body and is re-wrapped in place — otherwise the score column
            // the SELECT list asked for would never be attached.
            let upgrade = {
                let leaf = match &plan {
                    SqlPlan::Subquery { input, .. } => input.as_ref(),
                    other => other,
                };
                if matches!(leaf, SqlPlan::Scan { .. } | SqlPlan::TextSearch { .. }) {
                    try_hybrid_from_projection(leaf, &select.projection, functions)?
                } else {
                    None
                }
            };
            if let Some(upgraded_leaf) = upgrade {
                plan = match plan {
                    SqlPlan::Subquery {
                        filters,
                        projection,
                        sort_keys,
                        offset,
                        distinct,
                        limit,
                        ..
                    } => SqlPlan::Subquery {
                        input: Box::new(upgraded_leaf),
                        filters,
                        projection,
                        sort_keys,
                        offset,
                        distinct,
                        limit,
                    },
                    _ => upgraded_leaf,
                };
            }
            // After ORDER BY: if we now have a VectorSearch, check whether
            // the collection is vector-primary and the projection is
            // payload-free. If so, set `skip_payload_fetch`.
            if let SqlPlan::VectorSearch {
                ref collection,
                ref mut skip_payload_fetch,
                ref mut filters,
                ref mut payload_filters,
                ..
            } = plan
            {
                let info = catalog
                    .get_collection(DatabaseId::DEFAULT, collection)
                    .ok()
                    .flatten();
                let is_vector_primary = info
                    .as_ref()
                    .map(|c| c.primary == nodedb_types::PrimaryEngine::Vector)
                    .unwrap_or(false);
                if is_vector_primary {
                    if let Some(ref proj) = pre_order_by_projection
                        && pre_order_by_collection.as_deref() == Some(collection.as_str())
                    {
                        *skip_payload_fetch = is_pure_vector_projection(proj);
                    }
                    if let Some(vp) = info.as_ref().and_then(|c| c.vector_primary.as_ref()) {
                        let mut peeled: Vec<SqlPayloadAtom> = Vec::new();
                        let is_indexed = |name: &str| {
                            vp.payload_indexes
                                .iter()
                                .any(|(p, _)| p.eq_ignore_ascii_case(name))
                        };
                        filters.retain(|f| match &f.expr {
                            FilterExpr::Comparison {
                                field,
                                op: CompareOp::Eq,
                                value,
                            } if is_indexed(field) => {
                                peeled.push(SqlPayloadAtom::Eq(field.clone(), value.clone()));
                                false
                            }
                            FilterExpr::InList { field, values } if is_indexed(field) => {
                                peeled.push(SqlPayloadAtom::In(field.clone(), values.clone()));
                                false
                            }
                            FilterExpr::Between { field, low, high } if is_indexed(field) => {
                                peeled.push(SqlPayloadAtom::Range {
                                    field: field.clone(),
                                    low: Some(low.clone()),
                                    low_inclusive: true,
                                    high: Some(high.clone()),
                                    high_inclusive: true,
                                });
                                false
                            }
                            FilterExpr::Comparison { field, op, value }
                                if matches!(
                                    op,
                                    CompareOp::Lt | CompareOp::Le | CompareOp::Gt | CompareOp::Ge
                                ) && is_indexed(field) =>
                            {
                                let inclusive = matches!(op, CompareOp::Le | CompareOp::Ge);
                                let upper = matches!(op, CompareOp::Lt | CompareOp::Le);
                                peeled.push(SqlPayloadAtom::Range {
                                    field: field.clone(),
                                    low: if upper { None } else { Some(value.clone()) },
                                    low_inclusive: !upper && inclusive,
                                    high: if upper { Some(value.clone()) } else { None },
                                    high_inclusive: upper && inclusive,
                                });
                                false
                            }
                            FilterExpr::Expr(SqlExpr::BinaryOp {
                                left,
                                op: BinaryOp::Eq,
                                right,
                            }) => match (&**left, &**right) {
                                (SqlExpr::Column { name, .. }, SqlExpr::Literal(v))
                                    if is_indexed(name) =>
                                {
                                    peeled.push(SqlPayloadAtom::Eq(name.clone(), v.clone()));
                                    false
                                }
                                (SqlExpr::Literal(v), SqlExpr::Column { name, .. })
                                    if is_indexed(name) =>
                                {
                                    peeled.push(SqlPayloadAtom::Eq(name.clone(), v.clone()));
                                    false
                                }
                                _ => true,
                            },
                            FilterExpr::Expr(SqlExpr::InList {
                                expr,
                                list,
                                negated: false,
                            }) => match &**expr {
                                SqlExpr::Column { name, .. } if is_indexed(name) => {
                                    let mut lits = Vec::with_capacity(list.len());
                                    let all_lit = list.iter().all(|e| {
                                        if let SqlExpr::Literal(v) = e {
                                            lits.push(v.clone());
                                            true
                                        } else {
                                            false
                                        }
                                    });
                                    if all_lit {
                                        peeled.push(SqlPayloadAtom::In(name.clone(), lits));
                                        false
                                    } else {
                                        true
                                    }
                                }
                                _ => true,
                            },
                            FilterExpr::Expr(SqlExpr::Between {
                                expr,
                                low,
                                high,
                                negated: false,
                            }) => match (&**expr, &**low, &**high) {
                                (
                                    SqlExpr::Column { name, .. },
                                    SqlExpr::Literal(lo),
                                    SqlExpr::Literal(hi),
                                ) if is_indexed(name) => {
                                    peeled.push(SqlPayloadAtom::Range {
                                        field: name.clone(),
                                        low: Some(lo.clone()),
                                        low_inclusive: true,
                                        high: Some(hi.clone()),
                                        high_inclusive: true,
                                    });
                                    false
                                }
                                _ => true,
                            },
                            FilterExpr::Expr(SqlExpr::BinaryOp { left, op, right })
                                if matches!(
                                    op,
                                    BinaryOp::Lt | BinaryOp::Le | BinaryOp::Gt | BinaryOp::Ge
                                ) =>
                            {
                                match (&**left, &**right) {
                                    (SqlExpr::Column { name, .. }, SqlExpr::Literal(v))
                                        if is_indexed(name) =>
                                    {
                                        let inclusive = matches!(op, BinaryOp::Le | BinaryOp::Ge);
                                        let upper = matches!(op, BinaryOp::Lt | BinaryOp::Le);
                                        peeled.push(SqlPayloadAtom::Range {
                                            field: name.clone(),
                                            low: if upper { None } else { Some(v.clone()) },
                                            low_inclusive: !upper && inclusive,
                                            high: if upper { Some(v.clone()) } else { None },
                                            high_inclusive: upper && inclusive,
                                        });
                                        false
                                    }
                                    _ => true,
                                }
                            }
                            _ => true,
                        });
                        *payload_filters = peeled;
                    }
                }
            }
            apply_limit(plan, &tail)
        }
        SetExpr::SetOperation {
            op,
            left,
            right,
            set_quantifier,
        } => crate::planner::union::plan_set_operation(
            op,
            left,
            right,
            set_quantifier,
            catalog,
            functions,
            temporal,
        ),
        _ => Err(SqlError::Unsupported {
            detail: format!("query body type: {}", query.body),
        }),
    }
}

/// Catalog wrapper that resolves CTE names as schemaless document collections.
pub(crate) struct CteCatalog<'a> {
    pub(crate) inner: &'a dyn SqlCatalog,
    pub(crate) cte_names: Vec<String>,
}

impl SqlCatalog for CteCatalog<'_> {
    fn get_collection(
        &self,
        database_id: DatabaseId,
        name: &str,
    ) -> std::result::Result<Option<CollectionInfo>, SqlCatalogError> {
        // Check CTE names first.
        if self.cte_names.iter().any(|n| n == name) {
            return Ok(Some(CollectionInfo {
                name: name.into(),
                engine: EngineType::DocumentSchemaless,
                columns: Vec::new(),
                primary_key: Some("id".into()),
                has_auto_tier: false,
                indexes: Vec::new(),
                bitemporal: false,
                primary: nodedb_types::PrimaryEngine::Document,
                vector_primary: None,
                partition_strategy: nodedb_types::PartitionStrategy::CollectionHomed,
            }));
        }
        self.inner.get_collection(database_id, name)
    }
}
