// SPDX-License-Identifier: BUSL-1.1

//! Read-path RLS injection into physical plans.

use crate::bridge::envelope::PhysicalPlan;
use crate::control::security::auth_context::AuthContext;
use crate::control::security::rls::RlsPolicyStore;
use nodedb_physical::physical_plan::{
    ColumnarOp, DocumentOp, ExchangeOp, GraphOp, KvOp, QueryOp, SpatialOp, TextOp, TimeseriesOp,
    VectorOp,
};
use nodedb_physical::physical_task::PhysicalTask;

use super::filters::{get_rls, merge_filters};

/// Inject RLS predicates into physical tasks after plan conversion.
///
/// This is the read-path RLS enforcement entry point. For each task:
/// 1. Extracts the collection name from the physical plan.
/// 2. Fetches RLS read policies for `(tenant_id, collection)`.
/// 3. Substitutes `$auth.*` references using the `AuthContext`.
/// 4. Injects the resulting concrete filters into the plan:
///    - **Scans**: merged into the existing `filters` field (AND-combined).
///    - **Point gets**: stored in `rls_filters` for post-fetch evaluation.
///    - **Search ops**: stored in `rls_filters` for post-candidate filtering.
///
/// **Caller**: Session query execution, after DataFusion logical planning.
/// **Superuser bypass**: Handled inside `combined_read_predicate_with_auth`.
///
/// Returns `Err` if a required `$auth` field is missing (fail-closed).
pub fn inject_rls(
    tasks: &mut [PhysicalTask],
    rls_store: &RlsPolicyStore,
    auth: &AuthContext,
) -> crate::Result<()> {
    for task in tasks.iter_mut() {
        let tenant_id = task.tenant_id.as_u64();
        inject_rls_for_plan(tenant_id, &mut task.plan, rls_store, auth)?;
    }
    Ok(())
}

/// Inject RLS into a single physical plan (public for native protocol dispatch).
pub fn inject_rls_for_single_plan(
    tenant_id: u64,
    plan: &mut PhysicalPlan,
    rls_store: &RlsPolicyStore,
    auth: &AuthContext,
) -> crate::Result<()> {
    inject_rls_for_plan(tenant_id, plan, rls_store, auth)
}

/// Core dispatch: inject RLS into a single physical plan.
fn inject_rls_for_plan(
    tenant_id: u64,
    plan: &mut PhysicalPlan,
    rls_store: &RlsPolicyStore,
    auth: &AuthContext,
) -> crate::Result<()> {
    match plan {
        // ── Plans with scan-style `filters` field (merge RLS into existing filters) ──
        PhysicalPlan::Document(DocumentOp::Scan {
            collection,
            filters,
            ..
        })
        | PhysicalPlan::Kv(KvOp::Scan {
            collection,
            filters,
            ..
        }) => {
            let rls = get_rls(rls_store, tenant_id, collection, auth)?;
            if !rls.is_empty() {
                merge_filters(filters, &rls)?;
            }
        }

        // Aggregate: a catalog aggregate (`input: Some`) sources rows from the
        // embedded sub-plan, so RLS must be injected into that input rather
        // than the aggregate's own (empty) filters. A legacy aggregate
        // (`input: None`) merges RLS into its `filters` as before.
        PhysicalPlan::Query(QueryOp::Aggregate {
            collection,
            input,
            filters,
            ..
        }) => {
            if let Some(child) = input {
                inject_rls_for_plan(tenant_id, child, rls_store, auth)?;
            } else {
                let rls = get_rls(rls_store, tenant_id, collection, auth)?;
                if !rls.is_empty() {
                    merge_filters(filters, &rls)?;
                }
            }
        }

        // ── Plans with `rls_filters` field (set directly) ──
        PhysicalPlan::Document(DocumentOp::PointGet {
            collection,
            rls_filters,
            ..
        })
        | PhysicalPlan::Kv(KvOp::Get {
            collection,
            rls_filters,
            ..
        })
        | PhysicalPlan::Vector(VectorOp::Search {
            collection,
            rls_filters,
            ..
        })
        | PhysicalPlan::Vector(VectorOp::MultiSearch {
            collection,
            rls_filters,
            ..
        })
        | PhysicalPlan::Text(TextOp::Search {
            collection,
            rls_filters,
            ..
        })
        | PhysicalPlan::Text(TextOp::HybridSearch {
            collection,
            rls_filters,
            ..
        })
        | PhysicalPlan::Text(TextOp::HybridSearchTriple {
            collection,
            rls_filters,
            ..
        })
        | PhysicalPlan::Columnar(ColumnarOp::Scan {
            collection,
            rls_filters,
            ..
        })
        | PhysicalPlan::Timeseries(TimeseriesOp::Scan {
            collection,
            rls_filters,
            ..
        })
        | PhysicalPlan::Spatial(SpatialOp::Scan {
            collection,
            rls_filters,
            ..
        }) => {
            let rls = get_rls(rls_store, tenant_id, collection, auth)?;
            if !rls.is_empty() {
                *rls_filters = rls;
            }
        }

        // ── Plans that filter post-fetch (no storage pushdown slot) ──
        //
        // These have no filter the storage layer can push down, so the handler
        // evaluates the policy on the rows it fetched. A row the policy
        // excludes reads back as absent — indistinguishable from a missing key,
        // so a caller cannot probe for rows it may not read.
        PhysicalPlan::Document(DocumentOp::RangeScan {
            collection,
            rls_filters,
            ..
        })
        | PhysicalPlan::Kv(KvOp::BatchGet {
            collection,
            rls_filters,
            ..
        })
        | PhysicalPlan::Kv(KvOp::FieldGet {
            collection,
            rls_filters,
            ..
        }) => {
            let rls = get_rls(rls_store, tenant_id, collection, auth)?;
            if !rls.is_empty() {
                *rls_filters = rls;
            }
        }

        // ── Plans that still deny if RLS policies exist (unsupported) ──
        //
        // `IndexLookup` returns index entries rather than rows, so there is no
        // row body to evaluate a policy against.
        PhysicalPlan::Document(DocumentOp::IndexLookup { collection, .. }) => {
            let rls = get_rls(rls_store, tenant_id, collection, auth)?;
            if !rls.is_empty() {
                return Err(crate::Error::PlanError {
                    detail: format!(
                        "RLS policies on '{collection}' not supported with this operation type"
                    ),
                });
            }
        }

        // ── Graph traversal: deny while a policy exists on the collection ──
        //
        // A traversal returns node ids and edge labels, not row bodies, so
        // there is nothing here for a row filter to evaluate — the rows are
        // fetched later through `DocumentOp::PointGet`, which applies the
        // policy then. What a traversal *does* disclose is topology: which
        // nodes exist and how they connect. A read policy says some of those
        // rows are not the caller's to see, and their edges are equally not,
        // so the traversal refuses rather than leaking the shape of data whose
        // contents are protected.
        //
        // A traversal with no collection (`None`) is a tree-index walk scoped
        // by edge label; it has no collection whose policy could be consulted,
        // and the DDL that builds such an index is authorized separately.
        PhysicalPlan::Graph(
            GraphOp::Hop { collection, .. }
            | GraphOp::Neighbors { collection, .. }
            | GraphOp::NeighborsMulti { collection, .. }
            | GraphOp::Path { collection, .. }
            | GraphOp::Subgraph { collection, .. },
        ) => {
            if let Some(collection) = collection.as_deref() {
                let rls = get_rls(rls_store, tenant_id, collection, auth)?;
                if !rls.is_empty() {
                    return Err(crate::Error::PlanError {
                        detail: format!(
                            "RLS policies on '{collection}' are not supported with graph \
                             traversal: a traversal returns graph topology, which the row \
                             filter cannot evaluate"
                        ),
                    });
                }
            }
        }

        // ── Exchange: coordinator wrapper — recurse into the child plan ──
        //
        // The converter wraps any sharded real-collection scan in an Exchange
        // before RLS injection runs. Without this arm the catch-all silently
        // swallows the Exchange and the inner scan never receives its RLS filter.
        PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp { child, .. })) => {
            inject_rls_for_plan(tenant_id, child, rls_store, auth)?;
        }

        // ── PostProcess: recurse into the materialized child ──
        //
        // The post-processor wraps a subquery body (a sharded scan under
        // `Exchange{Gather}`); without this arm the catch-all swallows it and
        // the inner scan never receives its RLS filter — an RLS bypass.
        PhysicalPlan::Query(QueryOp::PostProcess { input, .. }) => {
            inject_rls_for_plan(tenant_id, input, rls_store, auth)?;
        }

        // ── LateralTopK / LateralLoop: recurse into outer_plan; also inject
        //    RLS into the inner_filters that the executor applies per outer row ──
        //
        // The outer_plan is a fully-formed PhysicalPlan (possibly Exchange-wrapped)
        // that produces the driving rows — it must receive RLS.  The inner_collection
        // is scanned directly by the Data Plane per-outer-row using inner_filters;
        // those filters must also have RLS merged in.
        PhysicalPlan::Query(QueryOp::LateralTopK {
            outer_plan,
            inner_collection,
            inner_filters,
            ..
        }) => {
            inject_rls_for_plan(tenant_id, outer_plan, rls_store, auth)?;
            let rls = get_rls(rls_store, tenant_id, inner_collection, auth)?;
            if !rls.is_empty() {
                merge_filters(inner_filters, &rls)?;
            }
        }

        PhysicalPlan::Query(QueryOp::LateralLoop {
            outer_plan,
            inner_collection,
            inner_filters,
            ..
        }) => {
            inject_rls_for_plan(tenant_id, outer_plan, rls_store, auth)?;
            let rls = get_rls(rls_store, tenant_id, inner_collection, auth)?;
            if !rls.is_empty() {
                merge_filters(inner_filters, &rls)?;
            }
        }

        // ── HashJoin: RLS per side, wherever that side's rows come from ──
        //
        // left_input / right_input hold a resolved sub-plan (e.g. an
        // Exchange-wrapped scan or a ProviderScan) supplied by the coordinator.
        // When Some, the child is the actual source of rows and receives RLS by
        // recursion. When None, the executor scans left_collection /
        // right_collection locally, and the filters go into the join's own
        // per-side slots — which the handler applies to the scanned rows before
        // building or probing, so an excluded row neither matches a partner nor
        // produces a null-extended outer row.
        PhysicalPlan::Query(QueryOp::HashJoin {
            left_collection,
            right_collection,
            left_input,
            right_input,
            left_rls_filters,
            right_rls_filters,
            ..
        }) => {
            match left_input {
                Some(child) => inject_rls_for_plan(tenant_id, child, rls_store, auth)?,
                None => {
                    let rls = get_rls(rls_store, tenant_id, left_collection, auth)?;
                    if !rls.is_empty() {
                        *left_rls_filters = rls;
                    }
                }
            }
            match right_input {
                Some(child) => inject_rls_for_plan(tenant_id, child, rls_store, auth)?,
                None => {
                    let rls = get_rls(rls_store, tenant_id, right_collection, auth)?;
                    if !rls.is_empty() {
                        *right_rls_filters = rls;
                    }
                }
            }
        }

        // ── Nested-loop / sort-merge joins: both sides always scan locally ──
        //
        // Neither variant takes a resolved child input, so both collections are
        // read directly by the handler and both slots are always populated.
        PhysicalPlan::Query(QueryOp::NestedLoopJoin {
            left_collection,
            right_collection,
            left_rls_filters,
            right_rls_filters,
            ..
        })
        | PhysicalPlan::Query(QueryOp::SortMergeJoin {
            left_collection,
            right_collection,
            left_rls_filters,
            right_rls_filters,
            ..
        }) => {
            let left = get_rls(rls_store, tenant_id, left_collection, auth)?;
            if !left.is_empty() {
                *left_rls_filters = left;
            }
            let right = get_rls(rls_store, tenant_id, right_collection, auth)?;
            if !right.is_empty() {
                *right_rls_filters = right;
            }
        }

        // Write operations, DDL, meta — no read RLS needed.
        _ => {}
    }

    Ok(())
}
