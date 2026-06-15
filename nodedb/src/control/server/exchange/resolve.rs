// SPDX-License-Identifier: BUSL-1.1

//! Plan resolution: materialize catalog providers and resolve Exchange nodes.
//!
//! `resolve_and_materialize` is the single shared entry point called by both
//! the pgwire dispatch path and the native dispatch path before handing a plan
//! to the gateway or SPSC bridge.  It performs two passes in order:
//!
//! 1. **Catalog materialization**: walk the plan tree; for every
//!    `QueryOp::ProviderScan { provider: Some(name), rows: [] }`, call
//!    `catalog::catalog_rows` (async, identity-scoped) and replace `rows`
//!    with the encoded result.  This happens per-request, post-cache, so
//!    identity-scoped catalog rows never enter the plan cache.
//!
//! 2. **Exchange resolution** (recursive):
//!    - `Gather{as_aggregate}` at the plan root → fan child to all cores,
//!      merge, and return `Resolved::Gathered`.
//!    - `Broadcast` inside a `HashJoin.left_input` / `right_input` →
//!      gather child to coordinator, encode as a merged msgpack array, and
//!      embed as `ProviderScan{provider: None, rows}`.  The modified join is
//!      self-contained and returned as `Resolved::Plan`.
//!    - `Shuffle{..}` → typed error (reserved seam, not implemented).
//!    - No Exchange / no empty ProviderScan → `Resolved::Plan` unchanged.

use nodedb_physical::physical_plan::{ExchangeMode, ExchangeOp, PhysicalPlan, QueryOp};

use crate::bridge::envelope::Response;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::server::pgwire::catalog;
use crate::control::state::SharedState;
use crate::data::executor::response_codec::{encode_binary_rows, flatten_to_relational_rows};
use crate::types::{DatabaseId, TenantId, TraceId};

use super::gather::{
    GatherOutcome, finalize_aggregate, gather_all_cores, gather_all_vshards, outcome_to_response,
};

/// Result of `resolve_and_materialize`.
pub enum Resolved {
    /// The plan was a root-level `Gather` — the coordinator has already
    /// executed it and the response is ready to return to the client.
    Gathered(Response),
    /// The plan (possibly mutated by catalog materialization or Broadcast
    /// embedding) is self-contained and should be dispatched normally.
    Plan(PhysicalPlan),
}

/// Materialize catalog providers and resolve Exchange nodes in `plan`.
///
/// See module-level documentation for the two-pass behaviour.
pub async fn resolve_and_materialize(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    tenant_id: TenantId,
    plan: PhysicalPlan,
    trace_id: TraceId,
) -> crate::Result<Resolved> {
    // Pass 1: fill empty ProviderScan rows (identity-scoped, per-request).
    let plan = materialize_providers(state, identity, plan).await?;

    // Pass 2: resolve Exchange nodes.
    resolve_exchange(state, database_id, tenant_id, plan, trace_id).await
}

/// Resolve only `Exchange` nodes (pass 2), without catalog provider
/// materialization. Used by the shared `dispatch_to_data_plane` funnel so that
/// internal query consumers (COPY, cursors, materialized-view refresh,
/// constraint subqueries) — which build `Exchange{Gather}`-wrapped read plans
/// over user tables but never carry catalog providers — still fan out and merge
/// correctly. Identity-free: catalog materialization happens earlier on the
/// pgwire/native paths that own the request identity. A no-op for plans with no
/// `Exchange` node.
pub async fn resolve_exchange_in_plan(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
    plan: PhysicalPlan,
    trace_id: TraceId,
) -> crate::Result<Resolved> {
    resolve_exchange(state, database_id, tenant_id, plan, trace_id).await
}

// ── pass 1 ───────────────────────────────────────────────────────────────────

/// Walk `plan` and replace every `ProviderScan{provider: Some(name), rows: []}`
/// with `ProviderScan{provider: None, rows: <encoded>}`.
///
/// The walk is structural: it recurses into `HashJoin` inputs and
/// `LateralTopK`/`LateralLoop` outer plans, plus `Exchange` children so that
/// catalog providers nested inside Exchange children are also filled before the
/// Exchange itself is resolved.
async fn materialize_providers(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    plan: PhysicalPlan,
) -> crate::Result<PhysicalPlan> {
    match plan {
        PhysicalPlan::Query(QueryOp::ProviderScan {
            provider: Some(name),
            rows: _,
            filters,
            projection,
            sort_keys,
            limit,
            offset,
            distinct,
        }) => {
            let rows = catalog::catalog_rows(&name, state, identity).await?;
            let encoded = encode_binary_rows(&rows);
            Ok(PhysicalPlan::Query(QueryOp::ProviderScan {
                provider: None,
                rows: encoded,
                filters,
                projection,
                sort_keys,
                limit,
                offset,
                distinct,
            }))
        }

        PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp { child, mode })) => {
            let child = Box::pin(materialize_providers(state, identity, *child)).await?;
            Ok(PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
                child: Box::new(child),
                mode,
            })))
        }

        // Aggregate over a sub-plan (catalog): recurse so the nested
        // `ProviderScan{provider: Some(name)}` gets its identity-scoped rows
        // filled per-request before the aggregate runs.
        PhysicalPlan::Query(QueryOp::Aggregate {
            collection,
            input: Some(input),
            group_by,
            aggregates,
            filters,
            having,
            limit,
            sub_group_by,
            sub_aggregates,
            grouping_sets,
            sort_keys,
        }) => {
            let input = Box::pin(materialize_providers(state, identity, *input)).await?;
            Ok(PhysicalPlan::Query(QueryOp::Aggregate {
                collection,
                input: Some(Box::new(input)),
                group_by,
                aggregates,
                filters,
                having,
                limit,
                sub_group_by,
                sub_aggregates,
                grouping_sets,
                sort_keys,
            }))
        }

        PhysicalPlan::Query(QueryOp::HashJoin {
            left_collection,
            right_collection,
            left_alias,
            right_alias,
            on,
            join_type,
            limit,
            post_group_by,
            post_aggregates,
            projection,
            post_filters,
            left_input,
            right_input,
            left_bitmap,
            right_bitmap,
        }) => {
            let left_input = match left_input {
                Some(p) => Some(Box::new(
                    Box::pin(materialize_providers(state, identity, *p)).await?,
                )),
                None => None,
            };
            let right_input = match right_input {
                Some(p) => Some(Box::new(
                    Box::pin(materialize_providers(state, identity, *p)).await?,
                )),
                None => None,
            };
            let left_bitmap = match left_bitmap {
                Some(p) => Some(Box::new(
                    Box::pin(materialize_providers(state, identity, *p)).await?,
                )),
                None => None,
            };
            let right_bitmap = match right_bitmap {
                Some(p) => Some(Box::new(
                    Box::pin(materialize_providers(state, identity, *p)).await?,
                )),
                None => None,
            };
            Ok(PhysicalPlan::Query(QueryOp::HashJoin {
                left_collection,
                right_collection,
                left_alias,
                right_alias,
                on,
                join_type,
                limit,
                post_group_by,
                post_aggregates,
                projection,
                post_filters,
                left_input,
                right_input,
                left_bitmap,
                right_bitmap,
            }))
        }

        PhysicalPlan::Query(QueryOp::LateralTopK {
            outer_plan,
            outer_alias,
            inner_collection,
            inner_filters,
            inner_order_by,
            inner_limit,
            correlation_keys,
            lateral_alias,
            projection,
            left_join,
        }) => {
            let outer_plan = Box::pin(materialize_providers(state, identity, *outer_plan)).await?;
            Ok(PhysicalPlan::Query(QueryOp::LateralTopK {
                outer_plan: Box::new(outer_plan),
                outer_alias,
                inner_collection,
                inner_filters,
                inner_order_by,
                inner_limit,
                correlation_keys,
                lateral_alias,
                projection,
                left_join,
            }))
        }

        PhysicalPlan::Query(QueryOp::LateralLoop {
            outer_plan,
            outer_alias,
            inner_collection,
            inner_filters,
            correlation_predicates,
            lateral_alias,
            projection,
            left_join,
            outer_row_cap,
        }) => {
            let outer_plan = Box::pin(materialize_providers(state, identity, *outer_plan)).await?;
            Ok(PhysicalPlan::Query(QueryOp::LateralLoop {
                outer_plan: Box::new(outer_plan),
                outer_alias,
                inner_collection,
                inner_filters,
                correlation_predicates,
                lateral_alias,
                projection,
                left_join,
                outer_row_cap,
            }))
        }

        // All other variants: no catalog providers can be nested here —
        // pass through unchanged.
        other => Ok(other),
    }
}

// ── pass 2 ───────────────────────────────────────────────────────────────────

/// Resolve any `Exchange` nodes in `plan`.
///
/// - Root-level `Gather` → gather all cores, return `Resolved::Gathered`.
/// - `Broadcast` nested inside a `HashJoin` input → gather the child, embed
///   the `merged_array` as `ProviderScan{None, rows}`, return `Resolved::Plan`.
/// - `Shuffle` → typed error.
/// - Anything else → `Resolved::Plan` unchanged.
async fn resolve_exchange(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
    plan: PhysicalPlan,
    trace_id: TraceId,
) -> crate::Result<Resolved> {
    match plan {
        // Root-level Gather: fan child to all cores and merge. First resolve any
        // Exchange{Broadcast} nodes nested inside the child (e.g. a HashJoin's
        // build side) so the plan fanned to cores is self-contained — no
        // Exchange node may reach a Data-Plane core.
        PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
            child,
            mode: ExchangeMode::Gather { as_aggregate },
        })) => {
            let child = match Box::pin(resolve_exchange(
                state,
                database_id,
                tenant_id,
                *child,
                trace_id,
            ))
            .await?
            {
                Resolved::Plan(p) => p,
                Resolved::Gathered(resp) => return Ok(Resolved::Gathered(resp)),
            };
            let outcome: GatherOutcome =
                gather_all_vshards(state, tenant_id, database_id, child, trace_id).await?;
            let payload = if as_aggregate {
                finalize_aggregate(&outcome.merged_array)
            } else {
                outcome.merged_array
            };
            Ok(Resolved::Gathered(outcome_to_response(
                payload,
                outcome.watermark_lsn,
            )))
        }

        // Root-level Broadcast: unusual but treat as Gather without merge.
        PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
            child,
            mode: ExchangeMode::Broadcast,
        })) => {
            let outcome =
                gather_all_vshards(state, tenant_id, database_id, *child, trace_id).await?;
            Ok(Resolved::Gathered(outcome_to_response(
                outcome.merged_array,
                outcome.watermark_lsn,
            )))
        }

        // Shuffle: reserved seam, not implemented.
        PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
            mode: ExchangeMode::Shuffle { .. },
            ..
        })) => Err(crate::Error::Internal {
            detail: "distributed shuffle is not yet implemented".into(),
        }),

        // HashJoin: resolve Broadcast children embedded in left_input / right_input.
        PhysicalPlan::Query(QueryOp::HashJoin {
            left_collection,
            right_collection,
            left_alias,
            right_alias,
            on,
            join_type,
            limit,
            post_group_by,
            post_aggregates,
            projection,
            post_filters,
            left_input,
            right_input,
            left_bitmap,
            right_bitmap,
        }) => {
            let left_input =
                resolve_join_input(state, database_id, tenant_id, left_input, trace_id).await?;
            let right_input =
                resolve_join_input(state, database_id, tenant_id, right_input, trace_id).await?;

            Ok(Resolved::Plan(PhysicalPlan::Query(QueryOp::HashJoin {
                left_collection,
                right_collection,
                left_alias,
                right_alias,
                on,
                join_type,
                limit,
                post_group_by,
                post_aggregates,
                projection,
                post_filters,
                left_input,
                right_input,
                left_bitmap,
                right_bitmap,
            })))
        }

        // All other plan variants: pass through unchanged.
        other => Ok(Resolved::Plan(other)),
    }
}

/// Resolve a `HashJoin` input slot.
///
/// When the slot contains an `Exchange{Broadcast}` child, gathers the child to
/// the coordinator and replaces the slot with a `ProviderScan{None, merged_array}`.
/// When the slot is already a `ProviderScan{None, ..}` or `None`, it is
/// returned unchanged.
async fn resolve_join_input(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
    input: Option<Box<PhysicalPlan>>,
    trace_id: TraceId,
) -> crate::Result<Option<Box<PhysicalPlan>>> {
    let Some(boxed) = input else {
        return Ok(None);
    };

    match *boxed {
        PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
            child,
            mode: ExchangeMode::Broadcast,
        })) => {
            // Gather the broadcast child to the coordinator, then embed the
            // merged msgpack array as an inline ProviderScan.
            //
            // We use `merged_array` (not `raw`) because `merged_array` is a
            // single well-formed msgpack array.  The Data-Plane executor
            // materialises the ProviderScan via `response_with_payload(rows)`,
            // producing a Response whose payload is exactly `merged_array`.
            // `decode_response_to_docs` in `hash_handlers.rs` then reads that
            // Response as a msgpack array — so the two shapes match.
            let outcome = gather_all_cores(state, tenant_id, database_id, *child, trace_id).await?;
            let provider_scan = PhysicalPlan::Query(QueryOp::ProviderScan {
                provider: None,
                rows: flatten_to_relational_rows(&outcome.merged_array),
                filters: Vec::new(),
                projection: Vec::new(),
                sort_keys: Vec::new(),
                limit: None,
                offset: 0,
                distinct: false,
            });
            Ok(Some(Box::new(provider_scan)))
        }

        // Exchange{Shuffle} inside a join input: error.
        PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
            mode: ExchangeMode::Shuffle { .. },
            ..
        })) => Err(crate::Error::Internal {
            detail: "distributed shuffle in join input is not yet implemented".into(),
        }),

        // Exchange{Gather} inside a join input: unusual but execute and embed.
        PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
            child,
            mode: ExchangeMode::Gather { as_aggregate },
        })) => {
            let outcome = gather_all_cores(state, tenant_id, database_id, *child, trace_id).await?;
            let merged = if as_aggregate {
                finalize_aggregate(&outcome.merged_array)
            } else {
                outcome.merged_array
            };
            let provider_scan = PhysicalPlan::Query(QueryOp::ProviderScan {
                provider: None,
                rows: flatten_to_relational_rows(&merged),
                filters: Vec::new(),
                projection: Vec::new(),
                sort_keys: Vec::new(),
                limit: None,
                offset: 0,
                distinct: false,
            });
            Ok(Some(Box::new(provider_scan)))
        }

        // Already resolved (ProviderScan{None, ..} or any other plan):
        // pass through.
        other => Ok(Some(Box::new(other))),
    }
}
