// SPDX-License-Identifier: BUSL-1.1

//! HashJoin input-slot resolution: embed Broadcast/Gather children as inline
//! `ProviderScan`s and gather cross-node build sides on the coordinator.

use nodedb_physical::physical_plan::{ExchangeMode, ExchangeOp, PhysicalPlan, QueryOp};

use crate::control::state::SharedState;
use crate::data::executor::response_codec::flatten_to_relational_rows;
use crate::types::{DatabaseId, TenantId, TraceId, TxnId};

use crate::control::server::exchange::gather::{
    finalize_aggregate, gather_all_cores, gather_all_vshards,
};

/// Resolve a `HashJoin` input slot.
///
/// When the slot contains an `Exchange{Broadcast}` child, gathers the child to
/// the coordinator and replaces the slot with a `ProviderScan{None, merged_array}`.
/// When the slot is already a `ProviderScan{None, ..}` or `None`, it is
/// returned unchanged.
pub(super) async fn resolve_join_input(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
    input: Option<Box<PhysicalPlan>>,
    trace_id: TraceId,
    txn_id: Option<TxnId>,
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
            let outcome =
                gather_all_cores(state, tenant_id, database_id, *child, trace_id, txn_id).await?;
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

        // Exchange{Shuffle} inside a join input is never a shape the emit
        // produces: a shuffle wraps a WHOLE hash join (the root arm), so it
        // cannot appear as one join's input. Reject with a clear message rather
        // than speculatively implementing an unreachable nesting.
        PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
            mode: ExchangeMode::Shuffle { .. },
            ..
        })) => Err(crate::Error::Internal {
            detail: "ExchangeMode::Shuffle is only valid wrapping a complete hash join, \
                     not as a join input"
                .into(),
        }),

        // Exchange{ShuffleAggregate} inside a join input is never a shape the
        // emit produces: a shuffle-aggregate wraps a WHOLE root aggregate, so it
        // cannot appear as one join's input. Reject with a clear message rather
        // than speculatively implementing an unreachable nesting (mirrors the
        // Shuffle rejection above).
        PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
            mode: ExchangeMode::ShuffleAggregate { .. },
            ..
        })) => Err(crate::Error::Internal {
            detail: "ExchangeMode::ShuffleAggregate is only valid wrapping a complete root \
                     aggregate, not as a join input"
                .into(),
        }),

        // Exchange{Gather} inside a join input: unusual but execute and embed.
        PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
            child,
            mode: ExchangeMode::Gather { as_aggregate },
        })) => {
            let outcome =
                gather_all_cores(state, tenant_id, database_id, *child, trace_id, txn_id).await?;
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

/// Gather a HashJoin build collection across all vShards and inline it as a
/// `ProviderScan` (cluster mode only).
///
/// Looks up `collection`'s engine in the catalog, builds a minimal unfiltered
/// full-collection scan for that engine, gathers it across all vShards via the
/// gateway, and embeds the merged rows as a `ProviderScan{provider: None, rows}`
/// — mirroring the embedding shape used by `resolve_join_input`.
///
/// Returns `Ok(None)` (the name-scan fallback) when the catalog has no record
/// for `collection`. This is a graceful degradation, never an error: a missing
/// catalog entry on the coordinator falls back to the existing by-name scan on
/// the executing node.
pub(super) async fn gather_join_build_side(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
    collection: &str,
    trace_id: TraceId,
    txn_id: Option<TxnId>,
) -> crate::Result<Option<Box<PhysicalPlan>>> {
    // Build a minimal, unfiltered, unprojected full-collection scan for the
    // engine via the shared builder. `Ok(None)` (no catalog / unknown
    // collection) keeps the existing graceful name-scan fallback — never an
    // error. The build side of a hash join must be COMPLETE (every build row is
    // needed for correct match output); the shared builder uses an unbounded
    // scan, which is allocation-safe (see `full_scan`).
    //
    // (Memory for a very large build relation is the inherent cost of a hash
    // join; spill-to-disk is a future optimization, not a reason to truncate. The
    // probe side's local name-scan and the converter's 10k default for unbounded
    // SELECTs remain separately capped — that engine-wide unbounded-scan limit is
    // its own effort and is the remaining truncation source, TRACKED.)
    let Some(scan_plan) =
        crate::control::server::exchange::full_scan::full_scan_plan_for_collection(
            state,
            database_id,
            tenant_id,
            collection,
        )?
    else {
        // No catalog on this node, or unknown collection: fall back to name-scan.
        return Ok(None);
    };

    // `Box::pin` breaks the async-fn recursion cycle: `gather_all_vshards`
    // dispatches through the gateway, which re-enters `resolve_exchange_in_plan`
    // → `resolve_exchange` → here. The cycle terminates at runtime (the scan
    // plan is Exchange-free), but the future must be heap-indirected so its size
    // is finite.
    let outcome = Box::pin(gather_all_vshards(
        state,
        tenant_id,
        database_id,
        scan_plan,
        trace_id,
        txn_id,
    ))
    .await?;

    Ok(Some(Box::new(PhysicalPlan::Query(QueryOp::ProviderScan {
        provider: None,
        rows: flatten_to_relational_rows(&outcome.merged_array),
        filters: Vec::new(),
        projection: Vec::new(),
        sort_keys: Vec::new(),
        limit: None,
        offset: 0,
        distinct: false,
    }))))
}
