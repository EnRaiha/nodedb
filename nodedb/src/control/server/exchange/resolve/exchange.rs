// SPDX-License-Identifier: BUSL-1.1

//! Pass 2 of plan resolution: Exchange node resolution.
//!
//! - `Gather{as_aggregate}` at the plan root → fan child to all vShards,
//!   merge, and return `Resolved::Gathered`.
//! - `Broadcast` inside a `HashJoin.left_input` / `right_input` →
//!   gather child to coordinator, encode as a merged msgpack array, and
//!   embed as `ProviderScan{provider: None, rows}`.  The modified join is
//!   self-contained and returned as `Resolved::Plan`.
//! - Root `Shuffle{keys, num_parts}` wrapping a `HashJoin` → orchestrate a
//!   cross-node grace hash join (`super::shuffle`) and return the merged rows
//!   as `Resolved::Gathered`. `Shuffle` as a join INPUT is a typed error (it
//!   only ever wraps a complete join).
//! - No Exchange / no empty ProviderScan → `Resolved::Plan` unchanged.

use nodedb_physical::physical_plan::{ExchangeMode, ExchangeOp, PhysicalPlan, QueryOp};

use crate::bridge::envelope::Response;
use crate::control::security::identity::AuthenticatedIdentity;
use crate::control::state::SharedState;
use crate::types::{DatabaseId, Lsn, TenantId, TraceId, TxnId, VShardId};

use crate::control::server::exchange::gather::{
    GatherOutcome, finalize_aggregate, gather_all_cores_stream, gather_all_vshards,
    outcome_to_response,
};
use crate::control::server::result_stream::ResultStream;

use crate::control::server::response_translate::vector::resolve_surrogate_pk;

use super::capture::DistributedReadCapture;
use super::join_input::{gather_join_build_side, resolve_join_input};
use super::materialize::materialize_providers;
use crate::control::server::exchange::full_scan::full_scan_plan_for_collection;

/// Result of `resolve_and_materialize`.
pub enum Resolved {
    /// The plan was a root-level `Gather` — the coordinator has already
    /// executed it and the response is ready to return to the client. The
    /// second field carries the per-shard watermark LSNs the gather observed
    /// (one `(vshard, watermark_lsn)` per responding core), so an in-transaction
    /// read can record one read-set entry per participating shard rather than a
    /// single collapsed max. Empty for cross-node gathers (per-shard watermarks
    /// are not yet threaded through the gateway) and for shuffle joins.
    ///
    /// The third field carries per-collection read captures for a distributed
    /// read materialized on the coordinator — both the GATHER path (each base
    /// collection under a root `Exchange{Gather}`, including both sides of a
    /// gathered `HashJoin`) and the SHUFFLE JOIN path (probe/left and
    /// build/right). The record seam records one read-set entry per capture, so
    /// EVERY participating collection's vshard is validated at commit rather than
    /// just the plan's collapsed left collection. Empty when there is no
    /// in-transaction base-collection capture (autocommit reads, and shuffle
    /// AGGREGATE which carries its single read version on the response scalar).
    Gathered(Response, Vec<(VShardId, Lsn)>, Vec<DistributedReadCapture>),
    /// The plan (possibly mutated by catalog materialization or Broadcast
    /// embedding) is self-contained and should be dispatched normally.
    Plan(Box<PhysicalPlan>),
    /// The plan was a single-node, unordered, non-aggregate scan eligible for
    /// streaming. The coordinator has eagerly dispatched it to all cores; the
    /// carried [`ResultStream`] yields row batches as they arrive. The pgwire
    /// path surfaces this lazily to the client; all other consumers
    /// `materialize` it back into a `Response`/bytes (behaviour-preserving).
    Stream(ResultStream),
}

/// Materialize catalog providers and resolve Exchange nodes in `plan`.
///
/// See module-level documentation for the two-pass behaviour.
///
/// `txn_id` is the originating session transaction id (if the dispatching
/// task ran inside a transaction block); it is threaded down to every
/// per-core `Request` built by the gather primitives so in-transaction scans
/// can merge the transaction's staging overlay (read-your-own-writes).
/// Autocommit / non-transactional callers pass `None`.
pub async fn resolve_and_materialize(
    state: &SharedState,
    identity: &AuthenticatedIdentity,
    database_id: DatabaseId,
    tenant_id: TenantId,
    plan: PhysicalPlan,
    trace_id: TraceId,
    txn_id: Option<TxnId>,
) -> crate::Result<Resolved> {
    // Pass 1: fill empty ProviderScan rows (identity-scoped, per-request).
    let plan = materialize_providers(state, identity, plan).await?;

    // Pass 2: resolve Exchange nodes. The captures accumulator is filled at every
    // base-collection gather point beneath the plan root and consumed (taken)
    // once at the root arm that returns `Resolved::Gathered`.
    let mut captures = Vec::new();
    resolve_exchange(
        state,
        database_id,
        tenant_id,
        plan,
        trace_id,
        txn_id,
        &mut captures,
    )
    .await
}

/// Resolve only `Exchange` nodes (pass 2), without catalog provider
/// materialization. Used by the shared `dispatch_to_data_plane` funnel so that
/// internal query consumers (COPY, cursors, materialized-view refresh,
/// constraint subqueries) — which build `Exchange{Gather}`-wrapped read plans
/// over user tables but never carry catalog providers — still fan out and merge
/// correctly. Identity-free: catalog materialization happens earlier on the
/// pgwire/native paths that own the request identity. A no-op for plans with no
/// `Exchange` node.
///
/// See `resolve_and_materialize` for `txn_id` semantics.
pub async fn resolve_exchange_in_plan(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
    plan: PhysicalPlan,
    trace_id: TraceId,
    txn_id: Option<TxnId>,
) -> crate::Result<Resolved> {
    let mut captures = Vec::new();
    resolve_exchange(
        state,
        database_id,
        tenant_id,
        plan,
        trace_id,
        txn_id,
        &mut captures,
    )
    .await
}

// ── pass 2 ───────────────────────────────────────────────────────────────────

/// Resolve any `Exchange` nodes in `plan`.
///
/// - Root-level `Gather` → gather all vShards, return `Resolved::Gathered`.
/// - `Broadcast` nested inside a `HashJoin` input → gather the child, embed
///   the `merged_array` as `ProviderScan{None, rows}`, return `Resolved::Plan`.
/// - Root-level `Shuffle` wrapping a `HashJoin` → orchestrate a cross-node
///   grace hash join, return `Resolved::Gathered`. `Shuffle` as a join input is
///   a typed error.
/// - Anything else → `Resolved::Plan` unchanged.
///
/// `captures` accumulates one [`DistributedReadCapture`] per base collection an
/// in-transaction distributed read observes: build/right sides push at their
/// gather points in [`gather_join_build_side`] / [`resolve_join_input`], the
/// probe/single side pushes in the root Gather arm here. Only the outermost root
/// arm returning `Resolved::Gathered` `mem::take`s the accumulator, so every
/// base collection is captured exactly once and taken exactly once at the true
/// root; a nested `Exchange` that itself resolves to `Gathered` returns its
/// already-taken captures up unchanged.
async fn resolve_exchange(
    state: &SharedState,
    database_id: DatabaseId,
    tenant_id: TenantId,
    plan: PhysicalPlan,
    trace_id: TraceId,
    txn_id: Option<TxnId>,
    captures: &mut Vec<DistributedReadCapture>,
) -> crate::Result<Resolved> {
    match plan {
        // Root-level Gather: fan child to all vShards and merge. First resolve any
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
                txn_id,
                captures,
            ))
            .await?
            {
                Resolved::Plan(p) => *p,
                Resolved::Gathered(resp, wms, caps) => {
                    return Ok(Resolved::Gathered(resp, wms, caps));
                }
                // A nested Exchange that itself resolved to a stream cannot be
                // re-wrapped by an outer Gather without materializing first;
                // surface it as the stream (the outer Gather is redundant —
                // nested root-level Gathers do not occur in practice, but if one
                // did, the inner stream is already the correct result).
                Resolved::Stream(s) => return Ok(Resolved::Stream(s)),
            };

            // Streaming fast path: a non-aggregate, unordered scan can stream
            // straight to the client without coordinator-side materialization.
            //
            // - Single-node (`gateway.is_none()`): fan to all local cores via
            //   `gather_all_cores_stream`.
            // - Cluster (`gateway.is_some()`): `gateway.execute_stream` routes
            //   the scan to its owning vShard — local cores when this node owns
            //   it, or the remote owner over QUIC (L4 streaming transport) —
            //   and merges the per-route streams with the same `select_all`.
            //
            // Aggregate gathers keep the materialize-then-merge behaviour.
            //
            // An in-transaction read (`txn_id.is_some()`) also keeps the
            // materialize path: streaming collapses per-core watermarks into one
            // value, but a transaction must record each participating shard's own
            // read version for optimistic-concurrency validation, so it takes the
            // `gather_all_vshards` branch below whose `GatherOutcome` preserves
            // `shard_watermarks`.
            if !as_aggregate && txn_id.is_none() && child.is_streamable_unordered_scan() {
                let stream = if let Some(gw) = state.gateway.get() {
                    let ctx = crate::control::gateway::core::QueryContext {
                        tenant_id,
                        trace_id,
                        database_id,
                        txn_id: None,
                    };
                    // NOTE: cluster mode does not yet thread `txn_id` through
                    // `gateway.execute_stream` — cross-node in-transaction
                    // read-your-own-writes is a tracked gap; single-node
                    // (`gather_all_cores_stream` below) is fixed.
                    gw.execute_stream_internal(&ctx, child).await?
                } else {
                    gather_all_cores_stream(state, tenant_id, database_id, child, trace_id, txn_id)?
                };
                return Ok(Resolved::Stream(stream));
            }

            // Determine the single base collection this gather observes for the
            // transaction read-set BEFORE the child plan is moved into the
            // gather. For a gathered `HashJoin` it is the probe (left) collection
            // scanned locally on the routed vShard; the build (right) collection
            // is captured separately at its own gather point in `join_input`. For
            // any other single-collection gather it is the child's own
            // collection. Only in-transaction reads need captures (the read-set
            // is only recorded inside a transaction block), so autocommit skips
            // the catalog lookup entirely.
            let probe_collection: Option<String> = if txn_id.is_some() {
                match &child {
                    PhysicalPlan::Query(QueryOp::HashJoin {
                        left_collection, ..
                    }) => Some(left_collection.clone()),
                    other => other.collection().map(str::to_owned),
                }
            } else {
                None
            };

            let outcome: GatherOutcome =
                gather_all_vshards(state, tenant_id, database_id, child, trace_id, txn_id).await?;

            // Record the probe/single-collection read at its OWN observed
            // read-version (the gathered collection's `coll_write_lsn`), scoped to
            // a bare single-collection scan so the commit-time OCC validator
            // re-homes and revalidates exactly that collection's vshard. A
            // `HashJoin` plan would otherwise collapse to the left collection
            // alone via `extract_collection` and miss the build side (captured
            // separately in `join_input`).
            if let Some(coll) = probe_collection
                && let Some(scan_plan) =
                    full_scan_plan_for_collection(state, database_id, tenant_id, &coll)?
            {
                captures.push(DistributedReadCapture {
                    scan_plan,
                    read_version_lsn: outcome.read_version_lsn,
                });
            }

            let payload = if as_aggregate {
                finalize_aggregate(&outcome.merged_array)
            } else {
                outcome.merged_array
            };
            Ok(Resolved::Gathered(
                outcome_to_response(payload, outcome.watermark_lsn, outcome.read_version_lsn),
                outcome.shard_watermarks,
                std::mem::take(captures),
            ))
        }

        // Root-level Broadcast: unusual but treat as Gather without merge.
        PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
            child,
            mode: ExchangeMode::Broadcast,
        })) => {
            let outcome =
                gather_all_vshards(state, tenant_id, database_id, *child, trace_id, txn_id).await?;
            Ok(Resolved::Gathered(
                outcome_to_response(
                    outcome.merged_array,
                    outcome.watermark_lsn,
                    outcome.read_version_lsn,
                ),
                outcome.shard_watermarks,
                std::mem::take(captures),
            ))
        }

        // Root-level Shuffle: orchestrate a real cross-node grace hash join.
        // The child must be a `QueryOp::HashJoin` (shuffle wraps a complete hash
        // join); `super::shuffle` validates that, fans producers + consumers,
        // and returns the merged join rows as `Resolved::Gathered`.
        PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
            child,
            mode: ExchangeMode::Shuffle { keys, num_parts },
        })) => {
            super::shuffle::resolve_shuffle_join(
                state,
                database_id,
                tenant_id,
                *child,
                keys,
                num_parts,
                trace_id,
            )
            .await
        }

        // Root-level ShuffleAggregate: orchestrate a real cross-node distributed
        // GROUP BY shuffle. The child must be a `QueryOp::Aggregate` (shuffle
        // wraps a complete aggregate); `super::shuffle_aggregate` validates that,
        // fans the partial-state producers + per-part consumers, and returns the
        // merged finalized rows as `Resolved::Gathered`.
        PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
            child,
            mode: ExchangeMode::ShuffleAggregate { keys, num_parts },
        })) => {
            super::shuffle_aggregate::resolve_shuffle_aggregate(
                state,
                database_id,
                tenant_id,
                *child,
                keys,
                num_parts,
                trace_id,
            )
            .await
        }

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
            computed_projection,
            join_filters,
            post_filters,
            left_input,
            right_input,
            left_bitmap,
            right_bitmap,
            left_rls_filters,
            right_rls_filters,
        }) => {
            let left_input = resolve_join_input(
                state,
                database_id,
                tenant_id,
                left_input,
                trace_id,
                txn_id,
                captures,
            )
            .await?;
            let mut right_input = resolve_join_input(
                state,
                database_id,
                tenant_id,
                right_input,
                trace_id,
                txn_id,
                captures,
            )
            .await?;

            // Cross-node build-side gather (cluster only).
            //
            // The HashJoin task routes to the LEFT (probe) collection's owning
            // vShard, where the LEFT side is scanned locally. The RIGHT (build)
            // collection is otherwise scanned BY NAME from that same node — but
            // a single-vShard-homed build collection may live on a DIFFERENT
            // node, so the by-name scan returns nothing and the join drops rows.
            //
            // When running in cluster mode (`gateway.is_some()`), and the build
            // side has not already been materialized by `resolve_join_input`
            // (i.e. `right_input` is still `None`), and `right_collection` names
            // a real user collection (catalog sides carry an empty name and are
            // already embedded as a ProviderScan), gather the build collection
            // across all vShards on the coordinator and inline it as a
            // `ProviderScan`. The HashJoin shipped to the probe node is then
            // self-contained. Only the RIGHT/build side is gathered; the
            // LEFT/probe side stays local to the routed vShard.
            if state.gateway.get().is_some()
                && right_input.is_none()
                && !right_collection.is_empty()
            {
                right_input = gather_join_build_side(
                    state,
                    database_id,
                    tenant_id,
                    &right_collection,
                    trace_id,
                    txn_id,
                    captures,
                )
                .await?;
            }

            Ok(Resolved::Plan(Box::new(PhysicalPlan::Query(
                QueryOp::HashJoin {
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
                    computed_projection,
                    join_filters,
                    post_filters,
                    left_input,
                    right_input,
                    left_bitmap,
                    right_bitmap,
                    left_rls_filters,
                    right_rls_filters,
                },
            ))))
        }

        // PostProcess: materialize the child's rows on the coordinator, then
        // lower to a `ProviderScan` that applies filter → offset → sort →
        // distinct → project → limit on a single core (its existing tail). This
        // keeps "run exactly once over the full union" correct: the child is
        // gathered here, so the relational tail never runs per-shard.
        PhysicalPlan::Query(QueryOp::PostProcess {
            input,
            filters,
            projection,
            sort_keys,
            limit,
            offset,
            distinct,
        }) => {
            // The converter wraps a sharded body in `Exchange{Gather}`; unwrap
            // it so the child is the real body plan (a plain body has no
            // wrapper and routes to its owning vShard directly).
            let (child, as_aggregate) = match *input {
                PhysicalPlan::Query(QueryOp::Exchange(ExchangeOp {
                    child,
                    mode: ExchangeMode::Gather { as_aggregate },
                })) => (*child, as_aggregate),
                other => (other, false),
            };

            // Resolve any Exchange nested inside the child first (e.g. a
            // `HashJoin` build-side `Broadcast`) so the plan gathered below is
            // self-contained — no Exchange may reach a Data-Plane core.
            let child = match Box::pin(resolve_exchange(
                state,
                database_id,
                tenant_id,
                child,
                trace_id,
                txn_id,
                captures,
            ))
            .await?
            {
                Resolved::Plan(p) => *p,
                // The unwrapped body is not itself a root Gather / stream;
                // surface these defensively without dropping post-processing.
                Resolved::Gathered(resp, wms, caps) => {
                    return Ok(Resolved::Gathered(resp, wms, caps));
                }
                Resolved::Stream(s) => return Ok(Resolved::Stream(s)),
            };

            // Classify the body's row shape so the gathered payload is
            // flattened correctly:
            //  - `Vector`  → vector/sparse/multivec hits (`{id, distance,
            //    doc_id, body}`): merge the document `body` to top-level and
            //    resolve the surrogate to the user PK.
            //  - `Hybrid`  → RRF fusion hits (`{doc_id: hex, <score alias>}`,
            //    no body): resolve `doc_id` to the user PK as `id`.
            //  - `None`    → flat storage rows (document / text `{id, data}`,
            //    columnar, spatial) or computed rows: the ordinary storage
            //    flatten already exposes every column.
            // `collection` and `hit_kind` are captured before the gather
            // consumes `child`.
            let hit_kind = classify_hit_shape(&child);
            // Extract the collection from the hit op directly: `collection()`
            // has no arm for sparse / multi-vector search, so it would yield
            // `None` and the PK resolver would be handed an empty collection.
            let hit_collection = hit_collection_name(&child);

            // Record the child's single base collection in the in-transaction
            // read-set at its own observed read-version (mirrors the root
            // Gather arm). Autocommit reads skip the catalog lookup.
            let probe_collection: Option<String> = if txn_id.is_some() {
                hit_collection.clone()
            } else {
                None
            };

            let outcome: GatherOutcome =
                gather_all_vshards(state, tenant_id, database_id, child, trace_id, txn_id).await?;

            if let Some(coll) = probe_collection
                && let Some(scan_plan) =
                    full_scan_plan_for_collection(state, database_id, tenant_id, &coll)?
            {
                captures.push(DistributedReadCapture {
                    scan_plan,
                    read_version_lsn: outcome.read_version_lsn,
                });
            }

            let merged = if as_aggregate {
                finalize_aggregate(&outcome.merged_array)
            } else {
                outcome.merged_array
            };

            // Flatten to the bare relational row shape the `ProviderScan` tail
            // consumes, resolving surrogate→PK for hit-shaped bodies via the
            // catalog so `SELECT id` returns the user PK, not the surrogate.
            use crate::data::executor::response_codec::{
                flatten_hybrid_hits_to_relational_rows, flatten_to_relational_rows,
                flatten_vector_hits_to_relational_rows,
            };
            let coll = hit_collection.unwrap_or_default();
            let rows = match hit_kind {
                HitShape::Vector => flatten_vector_hits_to_relational_rows(&merged, |surrogate| {
                    resolve_surrogate_pk(
                        state,
                        database_id,
                        tenant_id,
                        &coll,
                        nodedb_types::Surrogate::new(surrogate),
                    )
                }),
                HitShape::Hybrid => {
                    flatten_hybrid_hits_to_relational_rows(&merged, |hex| {
                        // `__local_<id>` is the headless-vector-leg sentinel; it
                        // is not a real surrogate and must not be parsed as hex.
                        if hex.starts_with("__local_") {
                            return None;
                        }
                        let surrogate = u32::from_str_radix(hex, 16).ok()?;
                        resolve_surrogate_pk(
                            state,
                            database_id,
                            tenant_id,
                            &coll,
                            nodedb_types::Surrogate::new(surrogate),
                        )
                    })
                }
                HitShape::None => flatten_to_relational_rows(&merged),
            };
            Ok(Resolved::Plan(Box::new(PhysicalPlan::Query(
                QueryOp::ProviderScan {
                    provider: None,
                    rows,
                    filters,
                    projection,
                    sort_keys,
                    limit,
                    offset,
                    distinct,
                },
            ))))
        }

        // All other plan variants: pass through unchanged.
        other => Ok(Resolved::Plan(Box::new(other))),
    }
}

/// The row shape a `PostProcess` body produces, driving how its gathered
/// payload is flattened into bare relational rows.
enum HitShape {
    /// Vector / sparse / multi-vector hits: `{id: <surrogate>, distance,
    /// doc_id?, body?}`. Merge the document `body` to top-level and resolve
    /// the surrogate to the user PK.
    Vector,
    /// Hybrid (RRF) fusion hits: `{doc_id: <surrogate hex>, <score alias>,
    /// ...}` with no body. Resolve `doc_id` to the user PK as `id`.
    Hybrid,
    /// Flat storage rows (`{id, data}` document / text, columnar, spatial) or
    /// computed rows — already fully columned after the storage flatten.
    None,
}

/// Classify a resolved `PostProcess` child by the row shape its engine emits.
fn classify_hit_shape(plan: &PhysicalPlan) -> HitShape {
    use nodedb_physical::physical_plan::{TextOp, VectorOp};
    match plan {
        PhysicalPlan::Vector(
            VectorOp::Search { .. }
            | VectorOp::MultiSearch { .. }
            | VectorOp::SparseSearch { .. }
            | VectorOp::MultiVectorScoreSearch { .. },
        ) => HitShape::Vector,
        PhysicalPlan::Text(TextOp::HybridSearch { .. } | TextOp::HybridSearchTriple { .. }) => {
            HitShape::Hybrid
        }
        _ => HitShape::None,
    }
}

/// Collection a `PostProcess` child reads, for the surrogate→PK resolver.
///
/// The search ops that emit surrogate-keyed hits carry their collection in a
/// field `PhysicalPlan::collection` does not surface (sparse / multi-vector),
/// so match them explicitly; every other body defers to `collection()`.
fn hit_collection_name(plan: &PhysicalPlan) -> Option<String> {
    use nodedb_physical::physical_plan::{TextOp, VectorOp};
    match plan {
        PhysicalPlan::Vector(
            VectorOp::Search { collection, .. }
            | VectorOp::MultiSearch { collection, .. }
            | VectorOp::SparseSearch { collection, .. }
            | VectorOp::MultiVectorScoreSearch { collection, .. },
        )
        | PhysicalPlan::Text(
            TextOp::HybridSearch { collection, .. } | TextOp::HybridSearchTriple { collection, .. },
        ) => Some(collection.clone()),
        other => other.collection().map(str::to_owned),
    }
}
